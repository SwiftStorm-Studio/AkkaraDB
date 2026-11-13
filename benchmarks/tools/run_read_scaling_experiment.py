"""Run serial, pinned Windows read comparisons and one separate diagnostic profile.

Usage: python run_read_scaling_experiment.py BUILD_BIN NEW_OUTPUT_DIRECTORY
"""
import csv
import ctypes
import hashlib
import json
import statistics
import struct
import subprocess
from datetime import datetime, timezone

import math
import sys
import time
from ctypes import wintypes
from pathlib import Path


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as source:
        return list(csv.DictReader(source))


def topology():
    query = ctypes.WinDLL("kernel32", use_last_error=True).GetLogicalProcessorInformationEx
    query.argtypes = [wintypes.DWORD, ctypes.c_void_p, ctypes.POINTER(wintypes.DWORD)]
    size = wintypes.DWORD()
    query(0, None, ctypes.byref(size))
    buffer = ctypes.create_string_buffer(size.value)
    if not query(0, buffer, ctypes.byref(size)):
        raise ctypes.WinError(ctypes.get_last_error())
    cores, offset = [], 0
    while offset < size.value:
        _, length = struct.unpack_from("II", buffer.raw, offset)
        flags, efficiency = struct.unpack_from("BB", buffer.raw, offset + 8)
        groups = struct.unpack_from("H", buffer.raw, offset + 30)[0]
        if groups != 1:
            raise RuntimeError("experiment runner currently supports one processor group")
        mask, group = struct.unpack_from("QH", buffer.raw, offset + 32)
        if group != 0:
            raise RuntimeError("experiment runner currently supports processor group zero")
        cores.append({"smt": bool(flags & 1), "efficiency_class": efficiency,
                      "cpus": [i for i in range(64) if mask & (1 << i)]})
        offset += length
    return sorted(cores, key=lambda core: (-core["efficiency_class"], core["cpus"][0]))


def validate(run):
    config = {row["key"]: row["value"] for row in read_csv(run / "config.csv")}
    assert config["verification"] == "passed"
    summary = read_csv(run / "summary.csv")
    samples = {}
    with (run / "samples.csv").open(newline="", encoding="utf-8") as source:
        for sample in csv.DictReader(source):
            key = (sample["round"], sample["threads"])
            samples.setdefault(key, []).append(float(sample["latency_ns"]))
    observed_calls = 0
    for row in summary:
        count = int(row["threads"])
        expected = int(config["iterations_total_per_bulk_pass"]) + int(config["latency_samples_total_per_pass"])
        expected += int(config["warmup_per_worker_per_pass"]) * count * (2 if int(row["latency_samples"]) else 1)
        assert int(row["verified_read_calls"]) == expected
        assert int(row["checksum"]) == expected * 218  # Experiment fixes 128-byte values, first=0, last=90.
        observed_calls += expected
        assert math.isclose(float(row["aggregate_ops_per_sec"]), int(row["bulk_operations"]) * 1e9 / float(row["bulk_total_ns"]))
        key = (row["round"], row["threads"])
        values = sorted(samples.get(key, []))
        assert len(values) == int(row["latency_samples"])
        if values:
            assert math.isclose(float(row["latency_mean_ns"]), statistics.fmean(values))
            for field, p in (("p50_ns", .5), ("p90_ns", .9), ("p99_ns", .99), ("p999_ns", .999)):
                assert float(row[field]) == values[math.ceil(p * len(values)) - 1]
    assert observed_calls == int(config["verified_measured_and_warmup_read_calls"])
    return summary, sum(map(len, samples.values()))


bin_dir = Path(sys.argv[1]).resolve()
root = Path(sys.argv[2]).resolve()
root.mkdir(parents=True, exist_ok=False)
exe = bin_dir / "akkaradb_akkengine_read_scaling_benchmark.exe"
sampler = bin_dir / "akkaradb_windows_cpu_sampler.exe"
cores = topology()
# First one hardware thread per physical core, then SMT siblings.
cpus = [c["cpus"][0] for c in cores] + [cpu for c in cores for cpu in c["cpus"][1:]]
thread_counts = [t for t in (1, 2, 4, 8, 12, 16) if t <= len(cpus)]
commands = []
metadata = {"started_utc": datetime.now(timezone.utc).isoformat(), "cores": cores, "worker_cpu_order": cpus,
            "binary_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                              for p in (exe, sampler, *bin_dir.glob("akkaradb-native-*.dll"))},
            "commands": commands, "cases": []}
(root / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
aggregates = []
for backend, shards in (("bptree", 1), ("bptree", 4), ("bptree", 16), ("art", 1), ("art", 16), ("skiplist", 1)):
    name = f"{backend}-shards-{shards}"
    destination = root / name
    args = [str(exe), "--keys", "1000000", "--iterations", "1000000", "--latency-samples", "100000",
            "--threads", ",".join(map(str, thread_counts)), "--cpus", ",".join(map(str, cpus)),
            "--rounds", "3", "--backend", backend, "--shards", str(shards), "--csv-dir", str(destination)]
    commands.append(args)
    print(f"Running {name}...", flush=True)
    with (root / f"{name}.stdout.log").open("w", encoding="utf-8") as stdout, \
            (root / f"{name}.stderr.log").open("w", encoding="utf-8") as stderr:
        subprocess.run(args, stdout=stdout, stderr=stderr, check=True, timeout=240,
                       creationflags=subprocess.CREATE_NO_WINDOW)
    runs = list(destination.glob("run-*"))
    assert len(runs) == 1
    rows, sample_count = validate(runs[0])
    assert len(rows) == 3 * len(thread_counts)
    metadata["cases"].append({"case": name, "run": str(runs[0]), "validated_samples": sample_count})
    for threads in thread_counts:
        selected = [r for r in rows if int(r["threads"]) == threads]
        rates = [float(r["aggregate_ops_per_sec"]) for r in selected]
        item = {"backend": backend, "shards": shards, "threads": threads,
                "median_ops_per_sec": statistics.median(rates), "min_ops_per_sec": min(rates), "max_ops_per_sec": max(rates),
                "median_p50_ns": statistics.median(float(r["p50_ns"]) for r in selected),
                "median_p90_ns": statistics.median(float(r["p90_ns"]) for r in selected),
                "median_p99_ns": statistics.median(float(r["p99_ns"]) for r in selected),
                "median_p999_ns": statistics.median(float(r["p999_ns"]) for r in selected)}
        aggregates.append(item)
        print(f"  threads={threads}: {item['median_ops_per_sec']:,.0f} ops/s", flush=True)
    (root / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

print("Running separate single-thread diagnostic profile...", flush=True)
control = root / "profile-control"
args = [str(exe), "--keys", "1000000", "--iterations", "10000000", "--latency-samples", "0",
        "--threads", "1", "--rounds", "1", "--cpus", str(cpus[0]), "--backend", "bptree", "--shards", "1",
        "--csv-dir", str(root / "profile-benchmark"), "--profile-control", str(control)]
commands.append(args)
with (root / "profile-benchmark.stdout.log").open("w", encoding="utf-8") as stdout, \
        (root / "profile-benchmark.stderr.log").open("w", encoding="utf-8") as stderr:
    process = subprocess.Popen(args, stdout=stdout, stderr=stderr, creationflags=subprocess.CREATE_NO_WINDOW)
    try:
        ready = Path(str(control) + ".ready")
        deadline = time.monotonic() + 55
        while not ready.exists() or not ready.read_text().strip():
            if process.poll() is not None:
                raise RuntimeError("profile benchmark exited before sampler handshake")
            if time.monotonic() > deadline:
                raise TimeoutError("profile benchmark not ready")
            time.sleep(.05)
        pid, tid = ready.read_text().split()
        sampler_args = [str(sampler), pid, tid, str(control), str(root / "profile"), str(bin_dir)]
        commands.append(sampler_args)
        completed = subprocess.run(sampler_args, capture_output=True, text=True, timeout=240,
                                   creationflags=subprocess.CREATE_NO_WINDOW)
        (root / "sampler.stdout.log").write_text(completed.stdout, encoding="utf-8")
        (root / "sampler.stderr.log").write_text(completed.stderr, encoding="utf-8")
        completed.check_returncode()
        assert process.wait(timeout=60) == 0
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=10)
validate(next((root / "profile-benchmark").glob("run-*")))
metadata["finished_utc"] = datetime.now(timezone.utc).isoformat()
(root / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
with (root / "aggregate.csv").open("w", newline="", encoding="utf-8") as target:
    writer = csv.DictWriter(target, fieldnames=list(aggregates[0]))
    writer.writeheader()
    writer.writerows(aggregates)
print(f"Experiment complete: {root}", flush=True)

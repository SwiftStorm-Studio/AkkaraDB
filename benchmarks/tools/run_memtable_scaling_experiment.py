"""Compare key count, key length and scan length one axis at a time.

Uses the read-only, single-shard MemTable API; no SST, WAL or transactions.
Each dataset is seeded and fully verified once, then reused for three rounds.
"""
import argparse
import csv
import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def rows(path):
    with Path(path).open(encoding="utf-8", newline="") as source:
        return list(csv.DictReader(source))


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate(directory):
    directory = Path(directory)
    c = {r["key"]: r["value"] for r in rows(directory / "config.csv")}
    assert c["contract"] == "memtable-scaling-v2" and c["verification"] == "passed"
    assert c["pre_scan_normalization"] == "full-ordered-oracle-in-65536-row-chunks"
    keys, rounds, point, scans, warm = [int(c[n]) for n in ("keys", "rounds", "iterations", "scan_iterations", "warmup")]
    summary = rows(directory / "summary.csv")
    selected_rows = sorted({int(r["scan_rows"]) for r in summary if r["operation"] == "scan"})
    assert selected_rows and max(selected_rows) <= keys
    assert len(summary) == rounds * (2 + len(selected_rows))
    expected = {(r, op, size) for r in range(rounds) for op, size in
                [("get", 0), ("get.miss", 0), *[("scan", n) for n in selected_rows]]}
    assert {(int(r["round"]), r["operation"], int(r["scan_rows"])) for r in summary} == expected
    checksum = int(c["value_bytes"]) + 1 + (0 if int(c["value_bytes"]) == 8 else 90)
    for r in summary:
        count, size = int(r["iterations"]), int(r["scan_rows"])
        assert r["backend"] == c["backend"]
        for n in ("keys", "key_bytes", "value_bytes"):
            assert r[n] == c[n]
        assert count == (scans if r["operation"] == "scan" else point)
        total, mean, rate, row_rate = [float(r[n]) for n in ("total_ns", "mean_ns", "ops_per_sec", "rows_per_sec")]
        assert all(math.isfinite(v) and v >= 0 for v in (total, mean, rate, row_rate)) and total > 0
        assert math.isclose(mean, total / count) and math.isclose(rate, count * 1e9 / total)
        assert math.isclose(row_rate, size * rate)
        expected_checksum = count * (1 if r["operation"] == "get.miss" else checksum * (size or 1))
        assert int(r["checksum"]) == expected_checksum
    assert int(c["verified_distinct_keys"]) == keys
    assert int(c["verified_get_calls"]) == keys + rounds * 2 * (point + warm)
    assert int(c["verified_scan_calls"]) == (keys + 65535) // 65536 + rounds * len(selected_rows) * (scans + warm + 2)
    assert int(c["verified_scan_rows"]) == keys + rounds * sum(selected_rows) * (scans + warm + 2)
    assert int(c["memtable_bytes"]) > 0 and float(c["seed_ns"]) > 0
    assert math.isclose(float(c["seed_ops_per_sec"]), keys * 1e9 / float(c["seed_ns"]))
    assert c["sst"] == c["wal"] == "0" and c["shards"] == "1"
    return c, summary


def write_rows(path, data):
    if not data:
        return
    with Path(path).open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=list(data[0]))
        writer.writeheader()
        writer.writerows(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bin_dir", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--backends", nargs="+", default=["skiplist", "bptree", "art", "wormhole"],
                        choices=["skiplist", "bptree", "art", "wormhole"])
    parser.add_argument("--key-counts", nargs="+", type=int, default=[100000, 1000000, 10000000])
    parser.add_argument("--key-lengths", nargs="+", type=int, default=[16, 32, 64, 128])
    parser.add_argument("--scan-rows", nargs="+", type=int, default=[1, 8, 32, 128, 1024])
    parser.add_argument("--base-keys", type=int, default=100000)
    parser.add_argument("--base-key-bytes", type=int, default=16)
    parser.add_argument("--base-scan-rows", type=int, default=32)
    parser.add_argument("--iterations", type=int, default=100000)
    parser.add_argument("--scan-iterations", type=int, default=10000)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=100)
    parser.add_argument("--value-bytes", type=int, default=128)
    parser.add_argument("--cpu", type=int, default=0 if os.name == "nt" else None)
    args = parser.parse_args()
    if min(args.base_keys, args.base_scan_rows, args.iterations, args.scan_iterations, args.rounds, *args.key_counts, *args.scan_rows) <= 0:
        parser.error("counts must be positive")
    if min(args.base_key_bytes, *args.key_lengths) < 16 or args.value_bytes < 8 or args.warmup < 0:
        parser.error("invalid key/value/warmup size")
    for values in (args.backends, args.key_counts, args.key_lengths, args.scan_rows):
        if len(set(values)) != len(values):
            parser.error("duplicate axis/backend value")
    if max(args.scan_rows + [args.base_scan_rows]) > min(args.key_counts + [args.base_keys]):
        parser.error("scan range exceeds the smallest dataset")
    bin_dir, output = args.bin_dir.resolve(), args.output.resolve()
    exe = bin_dir / ("akkaradb_memtable_scaling_benchmark.exe" if os.name == "nt" else "akkaradb_memtable_scaling_benchmark")
    if not exe.exists():
        parser.error("build akkaradb_memtable_scaling_benchmark first")
    output.mkdir(parents=True, exist_ok=False)
    source_root = Path(__file__).resolve().parents[2]
    sources = [Path(__file__).resolve(), source_root / "benchmarks/throughput/memtable_scaling_benchmark.cpp",
               *sorted((source_root / "akkara/akkengine/src/engine/memtable").glob("*.cpp")),
               *sorted((source_root / "akkara/akkengine/include/akk/engine/memtable").rglob("*.hpp"))]
    binaries = [exe, *bin_dir.glob("akkaradb-native-*.dll"), *bin_dir.glob("libakkaradb*.so*")]
    metadata = {"verification": "running", "started_utc": datetime.now(timezone.utc).isoformat(),
                "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                "system": platform.platform(), "logical_cpus": os.cpu_count(),
                "source_sha256": {str(p.relative_to(source_root)): sha(p) for p in sources},
                "binaries_sha256": {p.name: sha(p) for p in binaries}, "commands": [], "runs": []}

    def save():
        (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    # Reuse one dataset for the five scan lengths. Other axes vary one parameter
    # around the same base point, rather than creating a factorial experiment.
    datasets = [(args.base_keys, args.base_key_bytes)]
    datasets += [(n, args.base_key_bytes) for n in args.key_counts if n != args.base_keys]
    datasets += [(args.base_keys, n) for n in args.key_lengths if n != args.base_key_bytes]
    all_rows = []
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    save()
    try:
        for dataset_number, (keys, length) in enumerate(datasets):
            row_counts = sorted(set(args.scan_rows + [args.base_scan_rows])) if (keys, length) == datasets[0] else [args.base_scan_rows]
            backends = args.backends[dataset_number % len(args.backends):] + args.backends[:dataset_number % len(args.backends)]
            for backend in backends:
                label = f"{backend}-n{keys}-k{length}"
                destination = output / label
                cmd = [str(exe), "--backend", backend, "--keys", str(keys), "--key-bytes", str(length),
                       "--value-bytes", str(args.value_bytes), "--scan-rows", ",".join(map(str, row_counts)),
                       "--iterations", str(args.iterations), "--scan-iterations", str(args.scan_iterations),
                       "--warmup", str(args.warmup), "--rounds", str(args.rounds), "--output", str(destination)]
                if args.cpu is not None:
                    cmd += ["--cpu", str(args.cpu)]
                metadata["commands"].append(cmd)
                save()
                print("Measuring " + label, flush=True)
                with (output / (label + ".stdout.log")).open("w", encoding="utf-8") as stdout, \
                     (output / (label + ".stderr.log")).open("w", encoding="utf-8") as stderr:
                    process = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, creationflags=flags)
                    try:
                        while True:
                            try:
                                code = process.wait(timeout=30)
                                break
                            except subprocess.TimeoutExpired:
                                text = (output / (label + ".stdout.log")).read_text(encoding="utf-8").splitlines()
                                print(label + ": " + (text[-1] if text else "starting"), flush=True)
                        if code != 0:
                            raise RuntimeError(f"{label}: benchmark failed ({code}); inspect stderr log")
                    finally:
                        if process.poll() is None:
                            process.terminate()
                            process.wait(timeout=15)
                config, result = validate(destination)
                assert int(config["keys"]) == keys and int(config["key_bytes"]) == length and config["backend"] == backend
                all_rows += result
                metadata["runs"].append({"label": label, "directory": str(destination), "config": config})
                write_rows(output / "rounds.csv", all_rows)
                save()
        groups = {}
        for row in all_rows:
            key = tuple(row[n] for n in ("backend", "keys", "key_bytes", "value_bytes", "operation", "scan_rows"))
            groups.setdefault(key, []).append(row)
        aggregate = []
        for key, group in groups.items():
            assert len(group) == args.rounds
            record = dict(zip(("backend", "keys", "key_bytes", "value_bytes", "operation", "scan_rows"), key))
            record["rounds"] = len(group)
            for field in ("ops_per_sec", "rows_per_sec", "mean_ns"):
                values = [float(r[field]) for r in group]
                record["median_" + field] = statistics.median(values)
                record["min_" + field] = min(values)
                record["max_" + field] = max(values)
            aggregate.append(record)
        write_rows(output / "aggregate.csv", aggregate)
        for path, expected in metadata["source_sha256"].items():
            assert sha(source_root / path) == expected, path
        for path, expected in metadata["binaries_sha256"].items():
            assert sha(bin_dir / path) == expected, path
        metadata["fingerprints_unchanged"] = True
        metadata["verification"] = "passed"
    except Exception as error:
        metadata["verification"] = "failed"
        metadata["error"] = str(error)
        raise
    finally:
        metadata["finished_utc"] = datetime.now(timezone.utc).isoformat()
        save()
    print("Complete: " + str(output), flush=True)


if __name__ == "__main__":
    main()

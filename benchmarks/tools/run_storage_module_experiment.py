"""Measure direct MemTable/SST operations and collect separate Windows function profiles.

python benchmarks/tools/run_storage_module_experiment.py BUILD_BIN NEW_RESULT_DIRECTORY
Uses only Python's standard library. Existing result directories are never overwritten.
"""
import argparse
import csv
import hashlib
import json
import os
import platform
import statistics
import subprocess
from datetime import datetime, timezone

import math
import time
from pathlib import Path

from summarize_storage_profiles import summarize_profiles

OPERATIONS = ("get", "get.miss", "scan", "put.insert", "put.update", "remove")


def operations(sst):
    return OPERATIONS + (("get.sst", "get.miss.sst", "scan.sst") if sst else ())


def rows(path):
    with path.open(newline="", encoding="utf-8") as source:
        return list(csv.DictReader(source))


def write_rows(path, data):
    if not data:
        return
    with path.open("w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(target, fieldnames=list(data[0]))
        writer.writeheader()
        writer.writerows(data)


def validate(parent, iterations, keys, profiled=False):
    runs = list(parent.glob("run-*"))
    if len(runs) != 1:
        raise RuntimeError(f"expected one run under {parent}")
    run = runs[0]
    config = {r["setting"]: r["value"] for r in rows(run / "config.csv")}
    if config.get("verification") != "passed" or config.get("mode") != "storage_modules":
        raise RuntimeError(f"incomplete/incorrect benchmark: {run}")
    if int(config["keys"]) != keys or config["profiled"] != str(int(profiled)):
        raise RuntimeError("incorrect workload metadata")
    summary = rows(run / "summary.csv")
    if profiled:
        if summary or rows(run / "samples.csv"):
            raise RuntimeError("profiled run was mixed into normal latency measurements")
        return run, config, summary
    if config.get("storage_contract_version") != "2":
        raise RuntimeError("storage contract v2 is required; do not mix legacy results")
    expected_operations = operations(config["sst"] == "1")
    by_operation = {op: [] for op in expected_operations}
    with (run / "samples.csv").open(newline="", encoding="utf-8") as source:
        for sample in csv.DictReader(source):
            values = by_operation[sample["operation"]]
            if int(sample["sample_index"]) != len(values):
                raise RuntimeError("raw samples are out of execution order")
            value = float(sample["latency_ns"])
            if not math.isfinite(value) or value < 0:
                raise RuntimeError("invalid raw latency")
            values.append(value)
    if {r["operation"] for r in summary} != set(expected_operations) or len(summary) != len(expected_operations):
        raise RuntimeError("missing or duplicate operation rows")
    for row in summary:
        values = sorted(by_operation[row["operation"]])
        if len(values) != iterations or int(row["iterations"]) != iterations:
            raise RuntimeError("incorrect sample count")
        if not math.isclose(float(row["mean_ns"]), float(row["bulk_total_ns"]) / iterations):
            raise RuntimeError("incorrect bulk mean")
        if not math.isclose(float(row["ops_per_sec"]), 1e9 / float(row["mean_ns"])):
            raise RuntimeError("incorrect throughput")
        if not math.isclose(float(row["latency_mean_ns"]), statistics.fmean(values)):
            raise RuntimeError("incorrect individual latency mean")
        for column, permille in (("p50_ns", 500), ("p90_ns", 900), ("p99_ns", 990), ("p999_ns", 999)):
            if float(row[column]) != values[(len(values) * permille + 999) // 1000 - 1]:
                raise RuntimeError("incorrect percentile")
    batches = rows(run / "batches.csv")
    if len(batches) != int(config["storage_flush_samples"]):
        raise RuntimeError("incorrect end-to-end batch count")
    for row in batches:
        if int(row["records"]) != keys or not math.isclose(float(row["total_ns"]), float(row["apply_ns"]) + float(row["drain_ns"])):
            raise RuntimeError("end-to-end duration excludes part of ingestion")
        if row["completion"] != ("durable_sst" if config["sst"] == "1" else "memory_applied"):
            raise RuntimeError("incorrect batch completion boundary")
    if config["sst"] == "1":
        batches = rows(run / "flushes.csv")
        if len(batches) != int(config["storage_flush_samples"]) or any(
                int(r["records"]) != keys or float(r["elapsed_ns"]) <= 0 for r in batches
        ):
            raise RuntimeError("incorrect flush samples")
    return run, config, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bin_dir", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--keys", type=int, default=100000)
    parser.add_argument("--iterations", type=int, default=100000)
    parser.add_argument("--scan-rows", type=int, default=32)
    parser.add_argument("--value-bytes", type=int, default=128)
    parser.add_argument("--warmup", type=int, default=100)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--flush-samples", type=int, default=5)
    parser.add_argument("--profile-iterations", type=int)
    parser.add_argument("--profile-flush-keys", type=int, default=500000)
    parser.add_argument("--backends", nargs="+", choices=("skiplist", "bptree", "art"), default=["skiplist", "bptree", "art"])
    parser.add_argument("--cpu", type=int, default=0 if os.name == "nt" else None)
    parser.add_argument("--no-profiles", action="store_true")
    args = parser.parse_args()
    if args.profile_iterations is None:
        args.profile_iterations = args.iterations
    if min(args.keys, args.iterations, args.scan_rows, args.repeats, args.flush_samples,
           args.profile_iterations, args.profile_flush_keys) <= 0 or args.warmup < 0 or args.value_bytes < 8 or args.scan_rows > args.keys:
        parser.error("invalid workload sizes")
    if len(set(args.backends)) != len(args.backends):
        parser.error("backend names must be unique")
    if args.cpu is not None and (os.name != "nt" or not 0 <= args.cpu < 64):
        parser.error("--cpu requires Windows and a logical CPU in 0..63")
    if not args.no_profiles and os.name != "nt":
        parser.error("bundled function profiles require Windows; use --no-profiles for latency only")
    bin_dir, output = args.bin_dir.resolve(), args.output.resolve()
    suffix = ".exe" if os.name == "nt" else ""
    exe = bin_dir / ("akkaradb_akkengine_latency_benchmark" + suffix)
    sampler = bin_dir / ("akkaradb_windows_cpu_sampler" + suffix)
    if not exe.is_file() or (not args.no_profiles and not sampler.is_file()):
        parser.error("build the latency benchmark and Windows sampler first")
    output.mkdir(parents=True, exist_ok=False)
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    metadata = {"started_utc": datetime.now(timezone.utc).isoformat(), "system": platform.platform(),
                "machine": platform.machine(), "processor": platform.processor(), "logical_cpus": os.cpu_count(), "arguments": vars(args).copy(),
                "binaries_sha256": {}, "commands": [], "runs": [], "verification": "running"}
    metadata["arguments"]["bin_dir"] = str(bin_dir)
    metadata["arguments"]["output"] = str(output)
    source_root = Path(__file__).resolve().parents[2]
    sources = [Path(__file__).resolve(), Path(__file__).resolve().with_name("summarize_storage_profiles.py"),
               source_root / "benchmarks/throughput/akkengine_latency_benchmark.cpp",
               source_root / "benchmarks/throughput/detail/storage_module_benchmark.inc",
               *sorted((source_root / "akkara/akkengine/src/engine/memtable").glob("*.cpp")),
               *sorted((source_root / "akkara/akkengine/src/engine/sstable").glob("*.cpp"))]
    metadata["source_sha256"] = {str(p.relative_to(source_root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
    for file in (exe, sampler, *bin_dir.glob("akkaradb-native-*.dll"), *bin_dir.glob("*.pdb")):
        if file.is_file():
            metadata["binaries_sha256"][file.name] = hashlib.sha256(file.read_bytes()).hexdigest()

    def save():
        (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    def command(backend, sst, destination, iterations, keys):
        cmd = [str(exe), "--storage-only", "t", "--backend", backend, "--sst", "t" if sst else "f",
               "--iterations", str(iterations), "--keys", str(keys), "--value-bytes", str(args.value_bytes),
               "--scan-rows", str(min(args.scan_rows, keys)), "--warmup", str(args.warmup),
               "--flush-samples", str(args.flush_samples), "--csv-dir", str(destination)]
        if args.cpu is not None:
            cmd += ["--cpu", str(args.cpu)]
        return cmd

    save()
    all_rows, aggregates = [], []
    try:
        cases = [(backend, sst) for backend in args.backends for sst in (False, True)]
        for repeat in range(args.repeats):
            for index in range(len(cases)):
                backend, sst = cases[(index + repeat) % len(cases)]
                label = f"r{repeat + 1}-{backend}-{'sst' if sst else 'memory'}"
                destination = output / label
                cmd = command(backend, sst, destination, args.iterations, args.keys)
                metadata["commands"].append(cmd);
                save()
                print(f"Measuring {label}...", flush=True)
                with (output / (label + ".stdout.log")).open("w", encoding="utf-8") as stdout, \
                        (output / (label + ".stderr.log")).open("w", encoding="utf-8") as stderr:
                    subprocess.run(cmd, stdout=stdout, stderr=stderr, check=True, timeout=900, creationflags=flags)
                run, config, summary = validate(destination, args.iterations, args.keys)
                for row in summary:
                    all_rows.append({"repeat": repeat + 1, "mode": "sst" if sst else "memory", **row})
                timings = [float(r["total_ns"]) for r in rows(run / "batches.csv")]
                all_rows.append({"repeat": repeat + 1, "mode": "sst" if sst else "memory", **{k: "" for k in summary[0]},
                                 "operation": "ingest.batch", "unit": "durable_sst_batch" if sst else "memory_apply_batch",
                                 "backend": backend, "sst": int(sst), "iterations": len(timings), "keys": args.keys,
                                 "mean_ns": statistics.fmean(timings), "ops_per_sec": 1e9 / statistics.fmean(timings),
                                 "p50_ns": sorted(timings)[math.ceil(len(timings) * .5) - 1],
                                 "p99_ns": sorted(timings)[math.ceil(len(timings) * .99) - 1]})
                if sst:
                    timings = [float(r["elapsed_ns"]) for r in rows(run / "flushes.csv")]
                    # Flush batches are individually timed with untimed seeding between calls.
                    all_rows.append({"repeat": repeat + 1, "mode": "sst", **{k: "" for k in summary[0]},
                                     "operation": "flush", "unit": "batch", "backend": backend, "sst": "1",
                                     "iterations": len(timings), "keys": args.keys,
                                     "mean_ns": statistics.fmean(timings), "ops_per_sec": 1e9 / statistics.fmean(timings),
                                     "p50_ns": sorted(timings)[math.ceil(len(timings) * .5) - 1],
                                     "p99_ns": sorted(timings)[math.ceil(len(timings) * .99) - 1]})
                metadata["runs"].append({"label": label, "directory": str(run), "config": config});
                save()
        comparisons = []
        for repeat in range(args.repeats):
            for backend in args.backends:
                memory = next(r["config"] for r in metadata["runs"] if r["label"] == f"r{repeat + 1}-{backend}-memory")
                disk = next(r["config"] for r in metadata["runs"] if r["label"] == f"r{repeat + 1}-{backend}-sst")
                for operation in OPERATIONS:
                    for pass_name in ("bulk", "latency"):
                        for state in ("before", "after"):
                            prefix = f"{operation}_{pass_name}_{state}"
                            for metric in ("memory_bytes", "memory_puts", "memory_removes", "memory_flushes", "memory_immutables"):
                                a, b = memory[f"{prefix}_{metric}"], disk[f"{prefix}_{metric}"]
                                if a != b or disk[f"{prefix}_sst_files"] != "0":
                                    raise RuntimeError(f"unmatched active state: {backend} {prefix} {metric}")
                                comparisons.append({"repeat": repeat + 1, "backend": backend, "operation": operation,
                                                    "pass": pass_name, "state": state, "metric": metric,
                                                    "memory_only": a, "with_sst": b, "matched": True})
        write_rows(output / "comparison-checks.csv", comparisons)
        metadata["controlled_state_comparison"] = "passed"
        save()
        for backend, sst in cases:
            mode = "sst" if sst else "memory"
            for operation in (*operations(sst), "ingest.batch", *(("flush",) if sst else ())):
                selected = [r for r in all_rows if r["backend"] == backend and r["mode"] == mode and r["operation"] == operation]
                aggregates.append({"backend": backend, "mode": mode, "operation": operation,
                                   "unit": selected[0]["unit"], "repeats": len(selected),
                                   "median_mean_ns": statistics.median(float(r["mean_ns"]) for r in selected),
                                   "min_mean_ns": min(float(r["mean_ns"]) for r in selected),
                                   "max_mean_ns": max(float(r["mean_ns"]) for r in selected),
                                   "median_ops_per_sec": statistics.median(float(r["ops_per_sec"]) for r in selected),
                                   "median_p50_ns": statistics.median(float(r["p50_ns"]) for r in selected),
                                   "median_p99_ns": statistics.median(float(r["p99_ns"]) for r in selected)})
        write_rows(output / "runs.csv", all_rows)
        write_rows(output / "aggregate.csv", aggregates)
        if not args.no_profiles:
            for backend, sst in cases:
                for operation in (*operations(sst), *(("flush",) if sst else ())):
                    label = f"profile-{backend}-{'sst' if sst else 'memory'}-{operation}"
                    destination, control = output / label, output / (label + "-control")
                    profile_keys = args.profile_flush_keys if operation == "flush" else args.keys
                    cmd = command(backend, sst, destination, args.profile_iterations, profile_keys)
                    cmd += ["--profile-operation", operation, "--profile-control", str(control)]
                    metadata["commands"].append(cmd);
                    save()
                    print(f"Profiling {label}...", flush=True)
                    with (output / (label + ".stdout.log")).open("w", encoding="utf-8") as stdout, \
                            (output / (label + ".stderr.log")).open("w", encoding="utf-8") as stderr:
                        process = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, creationflags=flags)
                        try:
                            ready = Path(str(control) + ".ready")
                            deadline = time.monotonic() + 180
                            while not ready.exists() or not ready.read_text().strip():
                                if process.poll() is not None:
                                    raise RuntimeError(f"profile exited before handshake: {label}")
                                if time.monotonic() > deadline:
                                    raise TimeoutError(f"profile did not become ready: {label}")
                                time.sleep(.02)
                            pid, tid = ready.read_text().split()
                            sampler_cmd = [str(sampler), pid, tid, str(control), str(destination / "functions"), str(bin_dir), "--active-window"]
                            metadata["commands"].append(sampler_cmd);
                            save()
                            result = subprocess.run(sampler_cmd, capture_output=True, text=True, timeout=330, creationflags=flags)
                            (output / (label + ".sampler.log")).write_text(result.stdout + result.stderr, encoding="utf-8")
                            result.check_returncode()
                            if process.wait(timeout=180) != 0:
                                raise RuntimeError(f"profile benchmark failed: {label}")
                        finally:
                            if process.poll() is None:
                                process.terminate();
                                process.wait(timeout=15)
                    run, config, _ = validate(destination, args.profile_iterations, profile_keys, True)
                    functions = rows(destination / "functions" / "functions.csv")
                    total = sum(int(r["exclusive_samples"]) for r in functions)
                    metadata["runs"].append({"label": label, "directory": str(run), "config": config, "samples": total});
                    save()
        metadata["verification"] = "passed"
        save()
        if not args.no_profiles:
            summarize_profiles(output)
    except Exception as error:
        metadata["verification"] = "failed";
        metadata["error"] = str(error)
        raise
    finally:
        metadata["finished_utc"] = datetime.now(timezone.utc).isoformat();
        save()
    print(f"Completed: {output}", flush=True)


if __name__ == "__main__":
    main()

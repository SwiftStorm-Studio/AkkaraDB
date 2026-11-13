"""Compare captured baseline/inline/prefix engine builds with one identical benchmark executable.

Usage: python run_bptree_key_experiment.py VARIANT_DIRECTORY NEW_RESULT_DIRECTORY
Each variant directory must contain its engine DLL and the same read-scaling benchmark.
"""
import csv
import hashlib
import json
import statistics
import subprocess
from datetime import datetime, timezone

import math
import sys
from pathlib import Path


def rows(path):
    with path.open(newline="", encoding="utf-8") as source:
        return list(csv.DictReader(source))


variants = Path(sys.argv[1]).resolve()
output = Path(sys.argv[2]).resolve()
output.mkdir(parents=True, exist_ok=False)
names = ("baseline", "inline", "prefix")
exe_name = "akkaradb_akkengine_read_scaling_benchmark.exe"
metadata = {"started_utc": datetime.now(timezone.utc).isoformat(), "binaries": {}, "commands": [], "runs": []}
for name in names:
    metadata["binaries"][name] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                  for p in (variants / name).iterdir() if p.suffix in (".exe", ".dll", ".cpp", ".hpp")}
assert len({metadata["binaries"][name][exe_name] for name in names}) == 1
all_rows = []
validated_samples = 0
for block in range(3):
    for pattern in (("prefix", "mixed") if block % 2 == 0 else ("mixed", "prefix")):
        for offset in range(3):
            variant = names[(block + offset) % 3]
            label = f"block-{block + 1}-{pattern}-{variant}"
            destination = output / label
            command = [str(variants / variant / exe_name), "--backend", "bptree", "--keys", "1000000",
                       "--iterations", "1000000", "--threads", "1,4", "--cpus", "0,2,4,6", "--rounds", "3",
                       "--latency-samples", "100000", "--key-pattern", pattern, "--csv-dir", str(destination)]
            metadata["commands"].append(command)
            print(f"Running {label}...", flush=True)
            with (output / (label + ".stdout.log")).open("w", encoding="utf-8") as stdout, \
                    (output / (label + ".stderr.log")).open("w", encoding="utf-8") as stderr:
                subprocess.run(command, stdout=stdout, stderr=stderr, check=True, timeout=180,
                               creationflags=subprocess.CREATE_NO_WINDOW)
            run = next(destination.glob("run-*"))
            config = {r["key"]: r["value"] for r in rows(run / "config.csv")}
            assert config["verification"] == "passed" and config["verified_distinct_keys"] == "1000000"
            summary = rows(run / "summary.csv")
            assert len(summary) == 6
            samples = {}
            for r in rows(run / "samples.csv"):
                samples.setdefault((r["round"], r["threads"]), []).append(float(r["latency_ns"]))
            read_calls = 0
            for row in summary:
                expected = 1100000 + 200 * int(row["threads"])
                assert int(row["verified_read_calls"]) == expected
                assert int(row["checksum"]) == expected * 218
                assert math.isclose(float(row["aggregate_ops_per_sec"]), 1e15 / float(row["bulk_total_ns"]))
                values = sorted(samples[row["round"], row["threads"]])
                assert len(values) == 100000
                assert math.isclose(float(row["latency_mean_ns"]), statistics.fmean(values))
                for field, permille in (("p50_ns", 500), ("p90_ns", 900), ("p99_ns", 990), ("p999_ns", 999)):
                    assert float(row[field]) == values[(len(values) * permille + 999) // 1000 - 1]
                validated_samples += len(values)
                read_calls += expected
                all_rows.append({"block": block + 1, "pattern": pattern, "variant": variant, **row,
                                 "seed_insert_ns_per_op": config["seed_insert_ns_per_op"],
                                 "engine_memtable_bytes": config["engine_memtable_bytes"]})
            assert read_calls == int(config["verified_measured_and_warmup_read_calls"])
            metadata["runs"].append({"label": label, "run": str(run), "config": config})
            for threads in (1, 4):
                rate = statistics.median(float(r["aggregate_ops_per_sec"]) for r in summary if int(r["threads"]) == threads)
                print(f"  {threads} thread(s): {rate:,.0f} ops/s", flush=True)
            (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

aggregate = []
for pattern in ("prefix", "mixed"):
    for name in names:
        for threads in (1, 4):
            selected = [r for r in all_rows if r["pattern"] == pattern and r["variant"] == name and int(r["threads"]) == threads]
            rates = [float(r["aggregate_ops_per_sec"]) for r in selected]
            aggregate.append({"pattern": pattern, "variant": name, "threads": threads, "bulk_passes": len(rates),
                              "median_ops_per_sec": statistics.median(rates), "min_ops_per_sec": min(rates), "max_ops_per_sec": max(rates),
                              **{"median_" + field: statistics.median(float(r[field]) for r in selected)
                                 for field in ("p50_ns", "p90_ns", "p99_ns", "p999_ns", "seed_insert_ns_per_op", "engine_memtable_bytes")}})
for filename, data in (("summary.csv", all_rows), ("aggregate.csv", aggregate)):
    with (output / filename).open("w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(target, fieldnames=list(data[0]))
        writer.writeheader()
        writer.writerows(data)
metadata["finished_utc"] = datetime.now(timezone.utc).isoformat()
metadata["validated_samples"] = validated_samples
metadata["verification"] = "passed"
(output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
print(f"Completed: {validated_samples:,} validated samples; {output}", flush=True)

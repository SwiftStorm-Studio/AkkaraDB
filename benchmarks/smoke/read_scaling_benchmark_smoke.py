"""Check public CLI, uneven work partitions, read accounting and exported percentiles."""
import csv
import subprocess
import tempfile

import math
import sys
from pathlib import Path


def rows(path):
    with path.open(newline="", encoding="utf-8") as source:
        return list(csv.DictReader(source))


exe = str(Path(sys.argv[1]).resolve())
with tempfile.TemporaryDirectory(prefix="read-scaling-smoke-", dir=sys.argv[2]) as temporary:
    parent = Path(temporary)
    for backend in ("bptree", "art", "skiplist"):
        for shards, size in ((1, 128), (4, 8)):
            destination = parent / f"{backend}-{shards}"
            subprocess.run([exe, "--keys", "17", "--iterations", "37", "--latency-samples", "29",
                            "--warmup", "3", "--threads", "1,2,4", "--rounds", "2",
                            "--backend", backend, "--shards", str(shards), "--value-bytes", str(size),
                            "--key-pattern", "prefix" if shards == 1 else "mixed",
                            "--csv-dir", str(destination)], check=True, capture_output=True, text=True, timeout=30)
            runs = list(destination.glob("run-*"))
            assert len(runs) == 1
            run = runs[0]
            config = {row["key"]: row["value"] for row in rows(run / "config.csv")}
            assert config["verification"] == "passed" and config["verified_distinct_keys"] == "17"
            assert config["key_pattern"] == ("prefix" if shards == 1 else "mixed")
            assert float(config["seed_insert_total_ns"]) > 0 and int(config["engine_memtable_bytes"]) > 0
            summary = rows(run / "summary.csv")
            samples = rows(run / "samples.csv")
            assert len(summary) == 6 and len(samples) == 6 * 29
            assert len({(r["round"], r["threads"]) for r in summary}) == 6
            reads = 0
            for row in summary:
                threads = int(row["threads"])
                expected = 37 + 29 + 6 * threads
                assert int(row["verified_read_calls"]) == expected
                assert int(row["checksum"]) == expected * (size + (90 if size > 8 else 0))
                assert int(row["bulk_operations"]) == 37 and int(row["latency_samples"]) == 29
                reads += expected
                assert math.isclose(float(row["aggregate_ops_per_sec"]), 37e9 / float(row["bulk_total_ns"]))
                assert math.isclose(float(row["wall_ns_per_op"]), float(row["bulk_total_ns"]) / 37)
                selected = [s for s in samples if (s["round"], s["threads"]) == (row["round"], row["threads"])]
                values = sorted(float(s["latency_ns"]) for s in selected)
                assert len(values) == 29 and min(values) >= 0
                assert math.isclose(float(row["latency_mean_ns"]), sum(values) / len(values))
                for field, percentile in (("p50_ns", 500), ("p90_ns", 900), ("p99_ns", 990), ("p999_ns", 999)):
                    assert float(row[field]) == values[math.ceil(len(values) * percentile / 1000) - 1]
                for tid in range(threads):
                    indices = [int(s["sample_index"]) for s in selected if int(s["worker"]) == tid]
                    assert indices == list(range(29 * (tid + 1) // threads - 29 * tid // threads))
            assert reads == int(config["verified_measured_and_warmup_read_calls"])
    destination = parent / "bulk-only"
    subprocess.run([exe, "--keys", "17", "--iterations", "37", "--latency-samples", "0",
                    "--threads", "1,2", "--rounds", "1", "--csv-dir", str(destination)],
                   check=True, capture_output=True, timeout=30)
    run = next(destination.glob("run-*"))
    assert not rows(run / "samples.csv")
    for row in rows(run / "summary.csv"):
        assert row["p50_ns"] == row["p999_ns"] == row["latency_mean_ns"] == ""
        assert int(row["verified_read_calls"]) == 37 + 100 * int(row["threads"])
    for arguments in (("--iterations", "0"), ("--shards", "3"), ("--backend", "invalid"),
                      ("--key-pattern", "invalid"),
                      ("--threads", "1,1"), ("--threads", "1,"), ("--cpus", "0,0"),
                      ("--iterations", "2", "--threads", "4"),
                      ("--profile-control", "x", "--threads", "2")):
        failed = subprocess.run([exe, *arguments], capture_output=True, timeout=10)
        assert failed.returncode == 2, (arguments, failed.returncode)
print("Read scaling smoke: passed (3 backends, 2 shard/value sizes, uneven partitions, CSV, invalid inputs)")

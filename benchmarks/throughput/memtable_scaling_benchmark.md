# MemTable size / key length / scan length comparison

Build `akkaradb_memtable_scaling_benchmark` in Release mode. This benchmark uses
the single-shard public `MemTable` API with `skiplist`, `bptree`, `art`, or
`wormhole`; SST, WAL, VersionLog, transactions, Blob and Cluster are excluded.
It does not change any storage implementation or stored format.

```powershell
cmake --build builds/read-scaling --target akkaradb_memtable_scaling_benchmark
python -B benchmarks/tools/run_memtable_scaling_experiment.py builds/read-scaling/bin results/NEW-SCALING-RUN
```

The default experiment varies one axis at a time around 100,000 keys, 16-byte
keys and 32-row scans:

| Axis | Values | Held constant |
|---|---|---|
| Key count | 100,000 / 1,000,000 / 10,000,000 | 16-byte keys, 32-row scans |
| Key length | 16 / 32 / 64 / 128 bytes | 100,000 keys, 32-row scans |
| Scan length | 1 / 8 / 32 / 128 / 1024 rows | 100,000 keys, 16-byte keys |

Six distinct datasets per backend suffice: the base dataset serves all five
scan lengths. Each dataset is inserted and verified once and reused for three
measurement rounds. These are three rounds in the same process, not three
independent process restarts. The wrapper rotates backend order between
datasets and operation order between rounds.

## Workload contract

- Keys are `k`, zero padding and a big-endian uint64 tail. Even IDs are present;
  odd IDs are absent. Keys are unique and input order is ascending. Increasing
  key length also increases the common prefix; this is not a random-key study.
- Values default to 128 bytes and all models retain up to four versions per key.
  This read-only study stores exactly one version per key, with sequence ordering
  enabled. There are no concurrent writers or background flushes.
- The key slab, random query indexes, absent keys and scan starts are prepared
  outside timing. Random valid indexes are uniform with replacement, with the
  same seeds for each backend. Each dataset resides wholly in memory while it
  is measured; datasets/backends run sequentially.
- Every inserted key, sequence, value and flag is verified. An additional ordered
  sweep covers all records in 65,536-row chunks before measurement. This both
  checks ordering and completes lazy leaf sorting, with bounded temporary space.
- A hit returns an owning value copy and includes destruction. A non-inlined
  helper preserves this allocation/copy across the call boundary. A miss must
  return no record. Scans consume non-owning `RecordView`s completely, including
  iterator construction, snapshot capture and cleanup.
- Each round defaults to 100,000 hit calls, 100,000 misses, and 10,000 calls per
  scan length, with 100 warmup calls per operation. Warmup scans and both edge
  ranges verify every returned record's key, sequence and full value.
- Time is an aggregate wall-clock interval per operation. Individual-call
  latency sampling is intentionally omitted here. The prior latency benchmark
  is unchanged and remains available for percentile measurements.

The scan-1 result is an approximation of range startup cost **including the
first row and cleanup**, not a pure internal `seek`. Scans/s and rows/s are both
exported; `mean_ns / scan_rows` is an amortized per-row cost including startup.
Do not subtract timings from different row counts to claim a pure seek/next time.

Example for a single dataset:

```powershell
.\builds\read-scaling\bin\akkaradb_memtable_scaling_benchmark.exe --backend wormhole --keys 10000000 --key-bytes 16 --value-bytes 128 --iterations 100000 --scan-iterations 10000 --scan-rows 32 --rounds 3 --warmup 100 --cpu 0 --output results/NEW-LARGE-RUN
```

`--output` must name a new directory. `--cpu` pins the process on Windows and is
optional when running the executable directly. The experiment wrapper defaults
to CPU 0 on Windows. Only one dataset/backend runs at once.

## Results and validation

Each process exports `config.csv` and `summary.csv`. Contract
`memtable-scaling-v2` requires ordered full-range normalization before timing.
The runner validates result checksums, exact API call/row counts, round
coverage, operation counts and exported rates, and records memory counters. It writes
`rounds.csv`, `aggregate.csv`, logs and SHA-256 provenance in `metadata.json`.
Memory counters include payload/records, not just index metadata, and are not RSS.

The smoke test exercises all backends and key lengths, row/buffer/leaf edges,
overwriting protection, invalid/overflowing arguments and actual work accounting:

```powershell
python -B benchmarks/smoke/memtable_scaling_benchmark_smoke.py builds/read-scaling/bin/akkaradb_memtable_scaling_benchmark.exe .bench-tmp
ctest --test-dir builds/read-scaling -R akkaradb_memtable_scaling_benchmark_smoke --output-on-failure
```

This benchmark's query sequence, scan driver and single-seed rounds differ from
the prior full storage suite. Compare models and trends within this experiment;
do not attribute every difference from earlier reports to a storage change.

# Shared AkkEngine read scaling

Build `akkaradb_akkengine_read_scaling_benchmark` in Release mode. This uses one
live AkkEngine shared by all calling threads. WAL, SST, vLog, Blob, Cluster and
API servers are disabled; paths stay empty. The benchmark uses the normal engine
library and does not bypass its public read API.

```powershell
.\akkaradb_akkengine_read_scaling_benchmark.exe --backend bptree --keys 1000000 --iterations 1000000 --threads 1,2,4,8,12,16 --shards 1 --rounds 3 --latency-samples 100000 --csv-dir results/read-scaling
```

With the default `--key-pattern prefix`, keys, values and random permutation match the latency benchmark's read comparison:
16-byte ordered keys, 128-byte values, seed `0xA44A5EED`, one version per key.
Seeding is single-threaded, outside measurement. Every seeded value and the
distinct key count are checked. Reads use public copying `get`; result consumption
and disposal are included. No writes or transactions occur during measurement.

`--key-pattern mixed` uses two bijective 64-bit scramblers to produce distinct
16-byte keys whose bytes are distributed across the key, instead of sharing the
long prefix of the default keys. The read permutation/seed stays the same. This
also changes the insertion order relative to lexicographic key order; compare
engine variants within the same pattern. `config.csv` records the pattern,
engine MemTable's approximate bytes and a separate one-interval initial-insert
time. The latter times seeding puts with keys/values already prepared; it is
not an existing-key-update or write-durability measurement.

`--iterations` means **total calls across all workers**, not calls per worker.
Workers receive disjoint contiguous segments of the global shuffled access
stream, with uneven division supported. The stream wraps after visiting all
keys. Warmup defaults to 100 calls **per worker per pass**. Each round rotates the
thread-count order. One shard count is used for an entire invocation; compare
separate invocations with `--shards 1`, `4`, `16`, etc. Shards must be powers of two.

The bulk pass has one shared wall-clock interval from releasing already warmed
workers through the last worker finishing. Thread creation/join, output and
sample collection are outside that interval. A single caller runs directly on
the main thread. The loop has no benchmark-added shared atomic counter. Engine
read counters and checksums are checked after each case, including warmups and
the separate latency pass. Internal engine counters/locks are part of the cost.

`aggregate_ops_per_sec` is total completed calls divided by bulk wall time.
`wall_ns_per_op` is its reciprocal; with concurrent callers this **is not
individual request latency**. `worker_cpu_ns` sums Windows thread user+kernel CPU
time; `cpu_cores_used` divides it by bulk wall time (coarse for short passes).
On other platforms these two CPU fields are zero/unavailable.

`--latency-samples` sets the **total** calls in a separate concurrent pass
(default 100000, zero disables it). Raw individual-call timings include timer
overhead, with no subtraction. Percentiles use nearest rank over all workers'
samples. Samples exclude warmup and preserve order within each worker, not global
completion order. When disabled, latency summary fields are empty.

Each invocation writes a unique `run-*` folder containing `config.csv`,
`summary.csv` and `samples.csv`. CSV and aggregation occur between cases, outside
timing; subsequent cases warm up again. Only a complete, verified run gets
`verification,passed`. Exceptions leave `verification,failed` when the config
file remains writable. Abrupt process termination can leave no final marker.

Windows `--cpus 0,2,4,6,...` pins each worker to the corresponding logical CPU.
The main/seeding thread uses the first CPU. Inspect the machine's physical core
and efficiency-class mapping first; logical CPU numbers do not imply equal cores.
Without `--cpus`, the OS schedules workers freely.

## Separate diagnostic stack sampling (Windows x64)

Build `akkaradb_windows_cpu_sampler` and compile both the engine and benchmark
with optimized debug information, e.g. Release `/O2 /Ob2 /DNDEBUG /Z7` and linker
`/DEBUG:FULL /INCREMENTAL:NO`. This needs only the platform DbgHelp library.

Launch the benchmark with `--threads 1 --rounds 1 --latency-samples 0
--profile-control ABSOLUTE_PREFIX`, using a new prefix in an existing directory.
After seeding, verification and warmup, it writes `PREFIX.ready` with PID and TID,
then waits up to 60 seconds. Start the sampler as:

```text
akkaradb_windows_cpu_sampler.exe PID TID ABSOLUTE_PREFIX OUTPUT_DIR SYMBOL_DIR
```

The sampler loads modules/symbols, writes `PREFIX.go`, samples only that thread
by suspending it and unwinding its stack, then stops when `PREFIX.done` appears
or the process exits. The benchmark signals done immediately after the read
loop. Only use this on the benchmark process. Keep normal throughput runs free
of the sampler; this diagnostic interrupts execution and changes its speed.

`functions.csv` contains exclusive top-frame and inclusive stack sample counts;
`stacks.csv` retains addresses and resolved symbols. `sampling.txt` records sample
count, failures and time paused. These are **wall-clock stack observations**, not
hardware CPU utilization or cache-miss counters; off-CPU waits can be sampled.
Optimized/inlined functions may be attributed to their enclosing symbol.

## Reproducing and checking the experiment

`benchmarks/tools/run_read_scaling_experiment.py BUILD_BIN NEW_OUTPUT_DIRECTORY`
runs the B+Tree shard sweep (1/4/16), ART (1/16), and SkipList (1), followed by a
separate B+Tree single-thread profile. It records commands, binary hashes, CPU
topology, raw CSV/logs and three-round aggregate statistics. The runner chooses
one hardware thread per physical core first, with higher efficiency classes
first, then SMT siblings. This is an explicit placement policy, not an assertion
that mixed-core thread counts scale linearly. The runner requires Windows x64
and uses only Python's standard library.

The focused CLI/CSV check is:

```text
python benchmarks/smoke/read_scaling_benchmark_smoke.py BUILD_BIN/akkaradb_akkengine_read_scaling_benchmark.exe EXISTING_TEMP_PARENT
```

It checks all backends, two shard counts/value sizes, uneven work partitions,
actual read counts/checksums, raw-sample percentiles, bulk arithmetic, disabled
latency and invalid CLI inputs. It does not assert machine-specific speed.

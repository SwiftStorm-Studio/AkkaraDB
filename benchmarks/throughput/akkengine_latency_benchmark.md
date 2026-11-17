# AkkEngine latency and throughput benchmark

Build the `akkaradb_akkengine_latency_benchmark` CMake target in **Release** mode.
The executable is written to the build's `bin` directory. It uses the existing
engine library and adds no dependencies or public API changes.

With no arguments it asks for `t`/`f` switches and a positive `int` iteration count:

```text
MemTable only [t/f]: f
SST [t/f]: t
vLog [t/f]: t
MemTable backend [skiplist/bptree/art, Enter=skiplist]: bptree
Iterations per operation per pass [int > 0]: 10000
```

`MemTable only: t` skips the SST/vLog questions. MemTable is always required by
AkkEngine. For scripted runs, pass flags (either `--name value` or `--name=value`):

```powershell
# MemTable only
.\akkaradb_akkengine_latency_benchmark.exe --sst f --vlog f --iterations 10000
# MemTable + SST
.\akkaradb_akkengine_latency_benchmark.exe --sst t --vlog f --iterations 10000
# MemTable + VersionLog
.\akkaradb_akkengine_latency_benchmark.exe --sst f --vlog t --iterations 10000
# MemTable + SST + VersionLog
.\akkaradb_akkengine_latency_benchmark.exe --sst t --vlog t --iterations 10000
```

Run `--help` for all settings. With flags, omitted settings use defaults and there
are no prompts. `--iterations` is the number of operations **in each of two
measurement passes**, for each output row. An additional `--warmup` calls are run
before each pass (default 100, zero allowed). For example, 10000 iterations runs
20000 measured calls plus 200 warmup calls per row. Values must fit a positive
signed `int`; invalid/overflowing input returns exit code 2.

The distinct key count defaults to the iteration count. Entering `1000000` in
interactive mode therefore seeds **one million distinct keys**, and each point
operation visits every key once per measured pass. Use `--keys` to set the data
size independently. For example:

```powershell
.\akkaradb_akkengine_latency_benchmark.exe --sst f --vlog f --iterations 1000000
# Previous small working-set workload:
.\akkaradb_akkengine_latency_benchmark.exe --iterations 1000000 --keys 4096
```

The benchmark prints and verifies the seeded distinct key count. Values have the
configured fixed payload size; keys identify distinct records. `put` and write
transactions update those existing records rather than inserting new keys.

## MemTable backend and CSV output

`--backend skiplist|bptree|art` selects the actual MemTable implementation used
for writes, snapshots, rotation, and in-memory compaction. The default is
`skiplist`, preserving the previous engine default. Interactive mode also asks
for the backend; press Enter to accept `skiplist`.

Every run saves CSV files under a new, unique folder inside `benchmark-results`
in the working directory. `--csv-dir PATH` selects a different parent. The full
run directory is printed before seeding. Existing result folders are preserved.

```powershell
.\akkaradb_akkengine_latency_benchmark.exe --backend bptree --iterations 1000000 --csv-dir results
.\akkaradb_akkengine_latency_benchmark.exe --backend art --iterations 1000000 --csv-dir results
```

| File | Contents |
|---|---|
| `config.csv` | Backend, storage switches, isolation, data size, iteration/warmup counts, shuffle seed, timer calibration, compiler/assertion settings, and verification status |
| `summary.csv` | One row per operation: bulk duration, mean ns, ops/s, individual-call mean, population standard deviation, min/max, P50/P90/P99/P99.9, workload settings, getAt revision, and checksum |
| `samples.csv` | Each individual-call sample: operation, zero-based sample index, and raw latency in ns, in execution order |

CSV uses UTF-8, comma separators, CRLF records, escaped quoted strings, and a
locale-independent decimal point. Numeric output retains double precision;
the console table is rounded for readability. `history_depth` is the actual
seeded depth (1 with vLog disabled). The summary's `mean_ns` and `ops_per_sec`
come from the bulk pass; `latency_*` statistics and percentiles come from the
individual-call pass. Raw samples exclude warmup and have no per-call timer
correction. Missing getAt rows mean vLog was disabled, rather than zero cost.

CSV export, statistics, and file flushing occur **outside both timed passes**.
Each operation's raw samples are exported before sorting for percentiles, so
indices preserve execution order. Only one operation's sample buffer is kept
in memory, regardless of the number of benchmark rows. A million iterations
produces 8 million raw sample records without vLog, or 11 million with vLog.
Between-operation file I/O can affect system/cache state; each following
operation still performs its configured warmup and preparation.

The final `verification,passed` setting (quoted in the file) is written only
after all checks, engine shutdown, and result-file closure succeed. Failed runs
exit nonzero and can leave partial results. Catchable failures record
`verification=failed`, `failed_stage`, and `error` in `config.csv`; partial
summary/sample rows are retained. Abrupt termination may leave no final marker.

## Operations

### Direct MemTable and MemTable + SST modules (contract v2)

`--storage-only t` uses MemTable/SSTManager directly, excluding AkkEngine, WAL,
VersionLog, Blob and transactions. Version 2 corrects the first storage-module
benchmark's unmatched active state and missing end-to-end completion comparison.
Do not pool v1 and v2 results.

Common rows keep **identical seeded active MemTables** in both configurations.
Automatic flush is disabled during these API timers. Adding an SST manager does
not move the seed to disk for these rows. Each pass has a fresh store; warmup writes
use separate keys, so they do not change the records measured by insert/update/remove.
Warmup is followed directly by timing, without discarding/reseeding the warmed store.

| Row            | Timed operation / residency                              |
|----------------|----------------------------------------------------------|
| `get`          | Active-memory hit with owning value copy and disposal    |
| `get.miss`     | Active-memory miss                                       |
| `scan`         | Fully consumed bounded scan over active memory           |
| `put.insert`   | New-key insertion into the equally seeded active index   |
| `put.update`   | Append a version to an existing active key in both modes |
| `remove`       | Tombstone an existing active key in both modes           |
| `get.sst`      | SST-only warm hit; active MemTable empty (SST mode only) |
| `get.miss.sst` | SST-only warm miss (SST mode only)                       |
| `scan.sst`     | SST-only warm bounded scan (SST mode only)               |

The `*.sst` rows deliberately use a different residency and are labeled separately;
they do not measure the overhead of enabling SST on an identical memory lookup.
Key/value data and shuffled access order are otherwise shared. Misses use odd
indexes among seeded even indexes (except the final odd index, just above the range).
Insertions use precomputed distinct keys above the seeded range. Values consist of
an 8-byte version and repeated `0x5a` and are highly compressible.

All configurations use one shard, sequence ordering and four retained versions per
key. SST uses Zstd level 1, 32 KiB blocks, 64 MiB decoded cache, compaction disabled,
a durable standalone catalog and default Blob-reference metadata tracking.
Full seed verification warms memory/decoded/OS caches outside timing; this is not
a cold-disk test. CSV output and post-pass write verification/drains are untimed.
The common write rows therefore measure **memory application**, not durable writes.

```powershell
.\akkaradb_akkengine_latency_benchmark.exe --storage-only t --sst f --backend bptree --keys 100000 --iterations 100000 --scan-rows 32 --csv-dir results
.\akkaradb_akkengine_latency_benchmark.exe --storage-only t --sst t --backend bptree --keys 100000 --iterations 100000 --scan-rows 32 --flush-samples 3 --csv-dir results
```

`config.csv` includes `storage_contract_version=2` and each operation/pass's memory
bytes, put/remove counters, flush counters, immutable count and SST file count,
before and after timing. The runner compares both modes' active-state records and
rejects mismatches instead of accepting arithmetic-correct CSV as a fair experiment.
The evidence is saved as `comparison-checks.csv`.

### End-to-end completion and isolated flush

Both modes also produce `batches.csv`:

```text
sample_index,records,apply_ns,drain_ns,total_ns,completion
```

Each sample starts **empty**, applies the same shuffled input, then calls
`forceFlush()`. `total_ns = apply_ns + drain_ns` is measured across the entire
pipeline. SST mode enables normal 4 MiB automatic flush for this pipeline and waits
for durable SST/catalog publication; its completion is `durable_sst`. Memory mode
ends at `memory_applied` and provides no persistence. Both completion meanings are
explicit; only SST is a durable pipeline. Construction, validation and cleanup are
outside these timers. There are `--flush-samples` independent batches per run.

SST mode additionally saves `flushes.csv` for a different question: how long it takes
to persist an already populated, manually held table. Input seeding is excluded;
rotation, queueing, streaming writing, catalog sync and completion waiting are included.
Its fields are `sample_index,records,elapsed_ns,ns_per_record`.

### Repeated measurements and sampled function shares

```powershell
python -B benchmarks/tools/run_storage_module_experiment.py builds/read-scaling/bin results/storage-NEW --keys 100000 --iterations 100000 --scan-rows 32 --repeats 3
```

Build `akkaradb_akkengine_latency_benchmark` and `akkaradb_windows_cpu_sampler` first.
Use optimized Release with symbols (`/O2 /Ob2 /DNDEBUG /Z7`, `/DEBUG:FULL /OPT:REF /OPT:ICF`
for this Windows profiling build). `--no-profiles` measures operation/batch timings
without Windows profiling; `--backends bptree` narrows the experiment. The output
directory must be new. On Windows the runner defaults to `--cpu 0`: the entire
benchmark process (caller and flush workers) is restricted to one logical CPU.
Use `--cpu N` to select another available CPU. This resource boundary is recorded.

| File                        | Contents                                                                       |
|-----------------------------|--------------------------------------------------------------------------------|
| `aggregate.csv`             | Operation/batch means across repeats, min/max, throughput, P50/P99             |
| `runs.csv`                  | All normal summary rows, with batch completion units                           |
| `comparison-checks.csv`     | Matched active-state evidence for common operation rows                        |
| Each normal run             | Config, summary, per-call `samples.csv`, `batches.csv`, optional `flushes.csv` |
| `function-breakdown.csv`    | Per-operation symbols, exclusive/inclusive sample counts/shares                |
| Each profile's `functions/` | Raw stacks, symbolized function counts, sampling metadata                      |
| `metadata.json`             | Commands, machine/workload data, binary/PDB/source hashes, verification        |

Profiling is a separate diagnostic execution. `--profile-operation` selects one row (`flush` selects the persistence callback thread); `--profile-control PATH`
supplies
a fresh handshake prefix. A high-resolution waitable timer and start/finish events
bound external stack sampling to the operation region. Common API profiles do not
auto-flush. Flush-worker profiles exclude caller rotation/queue/wait time.

Profile iteration count defaults to the normal count. An explicit
`--profile-iterations` override is recorded and flagged as a different workload;
`--profile-flush-keys` controls the one diagnostic flush batch. More profile samples
do not make a mismatched growing index the same experiment.

Function CSV schema v2 intentionally **does not convert samples to nanoseconds**.
The former exporter multiplied samples from a different run/workload by a normal mean;
those values were not measured function durations and have been removed. Sampling
provides bottleneck shares, not per-function clocks or invocation counts. Inclusive
shares overlap; inlined/folded code may be attributed to a physical enclosing symbol.
`sparse_profile` marks fewer than 200 total stacks. `normal_workload_matches` records
whether sizes/contract match; it does not turn diagnostic timing into normal latency.

Regenerate sample shares from retained profiles without rerunning workloads:

```powershell
python -B benchmarks/tools/summarize_storage_profiles.py results/storage-NEW
```

`function-breakdown.metadata.json` records exporter/schema/method details. Legacy
profiles remain usable as diagnostic shares and are marked contract v1, but their
prior ns estimates and cross-residency overhead comparisons are superseded.

### Read-path comparison

`--read-comparison t` runs a dedicated memory-only comparison instead of the
normal get/put/scan/transaction suite. Storage switches must all be off. Example:

```powershell
.\akkaradb_akkengine_latency_benchmark.exe --backend bptree --read-comparison t --iterations 1000000 --keys 1000000 --csv-dir results/read-comparison
```

It seeds three independent stores (the selected `IMemTable` backend, the MemTable
coordinator, and AkkEngine) with identical keys, values, sequences, and one
version per key. All three use COMMIT_ORDER-compatible full version retention.
Every returned value is verified before timing. The access order, snapshot cut,
value size and warmup count are shared. No writes or transactions occur during
measurement. The default is three rounds (`--comparison-rounds INT`), rotating
the operation order by two positions each round to expose order/run variation.

| Row prefix | Measured read |
|---|---|
| `backend.view` | Direct `IMemTable::get`, borrowed record |
| `backend.copy` | Same lookup plus a newly allocated value copy |
| `memtable.view` | `MemTable::get`, borrowed record |
| `memtable.pinned` | `MemTable::getPinnedWithSnapshot`, record with lifetime ownership |
| `memtable.into` | `MemTable::getInto`, preallocated reusable output buffer |
| `engine.get` | Public `AkkEngine::get`, newly allocated value copy |
| `engine.into` | Public `AkkEngine::getInto`, preallocated reusable output buffer |

CSV/console operation names end in `.r1`, `.r2`, etc. Each row retains the normal
bulk and individual-call measurement passes. CSV metadata identifies the mode,
round count and approximate bytes of all three stores. Compare copies with
copies and views with views; lifetime and consistency guarantees differ between
APIs. Differences are not a CPU profile: layout/cache state, compiler boundaries,
and round variation can affect them. This mode uses live, mutable tables with
one version, matching the normal suite's initial point-read stage, not its later
transaction-sealed or write-heavy state.

Comparison mode also checks the engine's actual read counter against
`4 * (iterations + warmup) * comparison_rounds` and saves
`verified_engine_read_calls`. This independently checks both engine read APIs'
bulk/individual passes and warmups against the measured call counts.

| Row | One measured operation |
|---|---|
| `get` | Read an existing key with the copying `get` API |
| `put` | Update one existing key |
| `scan` | Open, fully consume, and destroy a bounded scan cursor |
| `getAt.oldest` | Read the first seeded version of a key |
| `getAt.middle` | Read the middle seeded version |
| `getAt.latest` | Read the latest seeded version |
| `tx.empty` | Begin and commit an empty transaction |
| `tx.get` | Begin, get one existing key, commit |
| `tx.scan` | Begin, fully consume a bounded scan, commit |
| `tx.put` | Begin, stage one update, commit |
| `tx.get_put` | Begin, get and update the same key, commit |

All transaction rows include session destruction. `--isolation` selects
`serializable` (default) or `snapshot`. In persistent configurations, write
transaction costs include AkkEngine's durable decision journal, even with WAL
disabled. These are single-threaded, uncontended transactions, with one key per
point transaction; distributed transactions and conflicts are not measured.

VersionLog rows are skipped when disabled. `--history-depth` (default 8, minimum
3) controls how many versions are seeded per key. Each getAt target is an actual
revision captured from history after a complete seeding round; every seeded key's
historical values are verified before measurement. Historical reads run before
measured writes, so the configured history depth is preserved for those rows.

The full suite reuses one engine across operations and both passes. Writes add
versions, and later transaction rows therefore include the state/history produced
by preceding rows; they are not measurements on freshly reset stores. In large
memory-only runs this can reach the engine's transaction snapshot limits and stop
the suite. Such a run is partial, not a zero-cost or completed transaction result.
The dedicated read comparison avoids writes/transactions during measurement and
keeps one version per key. Changing `--keys` independently of `--iterations`
also changes how often each key is revisited; record both when comparing runs.

## Measurement and interpretation

- `Mean_ns` is total bulk-loop time divided by iterations. Only one timer pair is
  used for this entire pass; no per-call timing or sample writes occur in it.
- `Ops/s` is iterations divided by that same bulk-loop time in seconds. For
  scans this counts fully consumed scans per second, and for transactions it
  counts complete transactions per second, rather than rows or inner operations.
- `P50_ns`, `P90_ns`, `P99_ns`, and `P999_ns` are nearest-rank percentiles from a
  **separate pass timing individual operations**. P999 means P99.9. These are
  individual-call latencies, not percentiles of batch averages.
- Percentiles are raw elapsed ns. Empty timer-pair median, mean, P90, and smallest
  observed nonzero interval are printed as references; they are not subtracted,
  since doing so cannot remove all timing
  perturbation and can distort short-operation tails. Zero measurement overhead
  cannot be guaranteed. Very short operations approach the timer's resolution.
- The latency sample buffer is allocated and touched before timing. Key creation,
  deterministic shuffling, seeding, logging, percentile sorting, and drains/forced
  flushes occur outside timing. Compiler fences bracket timing; no per-operation
  atomic counter or logging is added. Both passes include the same minimal
  result consumption/checks and destruction of returned buffers/pins before
  their end timestamps. API-internal allocation and I/O are part of the cost.
  `measurement_contract_version=2` records this boundary. Older results (without
  that setting) timed individual calls before consuming/destroying returned
  results, while their bulk pass already included that work. Old mean/ops/s
  remain bulk measurements, but old and new percentile values have different
  boundaries and should not be pooled.
- At least 10000 samples are recommended for P99.9. Smaller counts are accepted
  with a tail-resolution note. No minimum duration or throughput is asserted by
  the smoke tests.

The defaults are one distinct key per iteration, 16-byte ordered binary keys,
128-byte values, and `min(128, keys)` rows per scan. Point access uses a shuffled
key order with a fixed seed, without replacement until all keys have been visited.
If iterations exceed an explicit `--keys`, access wraps around that permutation.
Scan starts have their own shuffled permutation of valid starts, wrapping only
after every valid start has been visited. End keys and permutations are prepared
outside measurement. `scan_shuffle_seed` records that independent order.
All reads hit existing keys and follow seeding/warmup; a large working set need
not fit in CPU caches. This is not a controlled cold-disk benchmark. `--keys`,
`--value-bytes` (minimum 8), and `--scan-rows` tune this workload. Larger data sets
also increase snapshot resource use and can reach engine transaction limits.

SST mode flushes before each measured pass, so reads exercise SST-backed data
rather than just a newly populated MemTable. Writes use normal engine admission,
with a 4 MiB MemTable flush threshold; sufficiently large runs can include flush
and compaction work. Forced flush and queue-drain costs between passes are not
included in results. VersionLog defaults to async appends; `--vlog-sync t`
measures synchronous appends. Async write results measure API acknowledgement,
not durable completion. Optional `--wal t` enables synced WAL writes. WAL and
Blob are otherwise disabled to isolate MemTable/SST/VersionLog costs; Cluster
and API servers are disabled.

Each invocation creates its own unique directory under the OS temporary
directory. Persistent files are written there, so that volume affects the result.
The benchmark removes only that newly created directory on exit. Memory-only
mode leaves engine paths empty and creates no transaction journal. Verification
checks seeded values, historical reads, scan cardinality and committed writes;
any failure exits nonzero. Existing database directories are never opened.

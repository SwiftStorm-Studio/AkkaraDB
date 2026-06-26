# AkkaraDB Benchmark Notes

This file is an index of the benchmark and smoke-test targets that currently exist and build in `akkaradb-native`.

`akkaradb_benchmark` is now the SPECv5 typed API comparison benchmark. It compares raw inline, raw heap, typed trivial aggregate, typed BinPack, and non-unique secondary-index paths.

Multi-machine benchmark results are recorded in [`benchmarks/MULTI_MACHINE_BENCHMARK_RESULTS.md`](benchmarks/MULTI_MACHINE_BENCHMARK_RESULTS.md).

## Current Targets

| Target                                   | Purpose                                                                                                                        |
|------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `akkaradb_memtable_smoke_test`           | Smoke test for MemTable backends, snapshots, iterators, and flush callbacks.                                                   |
| `akkaradb_memtable_throughput_benchmark` | Measures sharded MemTable put / get / scan throughput and sampled latency.                                                     |
| `akkaradb_wal_smoke_test`                | Checks WAL append, recovery, tombstones, async force sync, corruption handling, and rotation.                                  |
| `akkaradb_wal_throughput_benchmark`      | Measures sharded WAL append, close drain, and recovery throughput. See `readme/ja/WAL_SETUP.md` for the detailed tuning notes. |
| `akkaradb_sstable_smoke_test`            | Checks SST writer/reader round-trip, MemTable flush through SSTManager, recovery, compaction, and tombstone behavior.          |
| `akkaradb_sstable_throughput_benchmark`  | Measures SST writer throughput, point-read throughput, full-scan throughput, sampled latency, and generated file bytes.        |
| `akkaradb_sstable_bloom_negative_lookup_benchmark` | Measures in-range negative SST lookups that exercise the Bloom filter rejection path.                              |
| `akkaradb_benchmark`                    | Compares raw `AkkEngine` access with SPECv5 `PackedTable` typed CRUD and secondary-index lookup overhead.          |
| `akkaradb_typed_api_smoke_test`         | Checks typed CRUD, `getInto`, BinPack round-trip, table-scoped scan/count, and non-unique index cleanup.          |
| `akkaradb_manifest_smoke_test`           | Checks Manifest SST lifecycle records, compaction commit, and CRC replay behavior.                                             |
| `akkaradb_versionlog_smoke_test`         | Checks VersionLog append, history, get-at, and rollback behavior.                                                              |

## Build Examples

Build individual Release targets:

```powershell
cmake --build cmake-build-release --target akkaradb_memtable_throughput_benchmark --config Release
cmake --build cmake-build-release --target akkaradb_wal_throughput_benchmark --config Release
cmake --build cmake-build-release --target akkaradb_sstable_throughput_benchmark --config Release
cmake --build cmake-build-release --target akkaradb_sstable_bloom_negative_lookup_benchmark --config Release
cmake --build cmake-build-release --target akkaradb_benchmark --config Release
```

Build the smoke tests:

```powershell
cmake --build cmake-build-release --target akkaradb_memtable_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_wal_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_sstable_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_typed_api_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_manifest_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_versionlog_smoke_test --config Release
```

If a normal Windows PowerShell session cannot find MSVC standard headers, run through the Visual Studio developer environment:

```powershell
cmd /c "call ""C:\Program Files\Microsoft Visual Studio\18\Community\Common7\Tools\VsDevCmd.bat"" -arch=x64 -host_arch=x64 >nul && cmake --build cmake-build-release --target akkaradb_wal_smoke_test --config Release"
```

## Run Examples

```powershell
cmake-build-release\bin\akkaradb_memtable_smoke_test.exe
cmake-build-release\bin\akkaradb_wal_smoke_test.exe
cmake-build-release\bin\akkaradb_sstable_smoke_test.exe
cmake-build-release\bin\akkaradb_typed_api_smoke_test.exe
cmake-build-release\bin\akkaradb_manifest_smoke_test.exe
cmake-build-release\bin\akkaradb_versionlog_smoke_test.exe
```

Typed API comparison:

```powershell
cmake-build-release\bin\akkaradb_benchmark.exe
```

MemTable throughput:

```powershell
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 200000 --writers=16 --backend=art
```

The MemTable throughput benchmark writes three CSV files next to the executable by default:

- `memtable_throughput_results.csv`: MemTable benchmark results only
- `memtable_throughput_results_environment.csv`: machine profile and DiskSpd best-of-5 summary
- `memtable_throughput_results_diskspd_raw.csv`: per-run DiskSpd raw results

WAL throughput:

```powershell
cmake-build-release\bin\akkaradb_wal_throughput_benchmark.exe 200000 --writers=16 --sync=async --group-n=2048 --group-micros=500 --group-bytes=4MiB --max-pending-bytes=64MiB
```

SSTable throughput:

```powershell
cmake-build-release\bin\akkaradb_sstable_throughput_benchmark.exe 200000 --readers=16 --codec=zstd --block-size=32KiB --cache-bytes=64MiB
```

Use `--same-ops` when comparing directly with MemTable/WAL runs and you want every size case to use the requested operation count instead of the SST benchmark's payload-size cap:

```powershell
cmake-build-release\bin\akkaradb_sstable_throughput_benchmark.exe 200000 --writers=16 --codec=zstd --block-size=32KiB --cache-bytes=64MiB --same-ops
```

SSTable Bloom negative lookup:

```powershell
cmake-build-release\bin\akkaradb_sstable_bloom_negative_lookup_benchmark.exe 1000000 5000000 --writers=16 --bits-per-key=10 --codec=zstd --cache-bytes=64MiB
```

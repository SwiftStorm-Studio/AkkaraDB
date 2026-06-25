# AkkaraDB ベンチマークメモ

このファイルは、`akkaradb-native` に現在存在し、ビルド可能な benchmark / smoke-test target の一覧です。

`akkaradb_benchmark` は現時点では SPECv5 typed API comparison benchmark です。raw inline、raw heap、typed trivial aggregate、typed BinPack、non-unique secondary-index path を比較します。

複数マシンで取得した benchmark result は [benchmarks/MULTI_MACHINE_BENCHMARK_RESULTS.md](benchmarks/MULTI_MACHINE_BENCHMARK_RESULTS.md) にまとめています。

## 現在の target

| Target | 目的 |
|---|---|
| `akkaradb_memtable_smoke_test` | MemTable backend、snapshot、iterator、flush callback の smoke test |
| `akkaradb_memtable_throughput_benchmark` | sharded MemTable の put / get / scan throughput と sampled latency を測定 |
| `akkaradb_wal_smoke_test` | WAL append、recovery、tombstone、async force sync、corruption handling、rotation を確認 |
| `akkaradb_wal_throughput_benchmark` | sharded WAL の append、close drain、recovery throughput を測定 |
| `akkaradb_sstable_smoke_test` | SST writer/reader round-trip、MemTable flush through SSTManager、recovery、compaction、tombstone behavior を確認 |
| `akkaradb_sstable_throughput_benchmark` | SST writer throughput、point-read throughput、full-scan throughput、sampled latency、生成ファイルサイズを測定 |
| `akkaradb_sstable_bloom_negative_lookup_benchmark` | Bloom filter の reject path を使う in-range negative SST lookup を測定 |
| `akkaradb_benchmark` | raw `AkkEngine` access と SPECv5 `PackedTable` typed CRUD / secondary-index lookup の overhead を比較 |
| `akkaradb_typed_api_smoke_test` | typed CRUD、`getInto`、BinPack round-trip、table-scoped scan/count、non-unique index cleanup を確認 |
| `akkaradb_manifest_smoke_test` | Manifest SST lifecycle record、compaction commit、CRC replay behavior を確認 |
| `akkaradb_versionlog_smoke_test` | VersionLog append、history、get-at、rollback behavior を確認 |

## ビルド例

Release target を個別にビルドする例:

```powershell
cmake --build cmake-build-release --target akkaradb_memtable_throughput_benchmark --config Release
cmake --build cmake-build-release --target akkaradb_wal_throughput_benchmark --config Release
cmake --build cmake-build-release --target akkaradb_sstable_throughput_benchmark --config Release
cmake --build cmake-build-release --target akkaradb_sstable_bloom_negative_lookup_benchmark --config Release
cmake --build cmake-build-release --target akkaradb_benchmark --config Release
```

smoke test をビルドする例:

```powershell
cmake --build cmake-build-release --target akkaradb_memtable_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_wal_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_sstable_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_typed_api_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_manifest_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_versionlog_smoke_test --config Release
```

通常の Windows PowerShell から MSVC standard header を見つけられない場合は、Visual Studio の developer environment 経由で実行します。

```powershell
cmd /c "call ""C:\Program Files\Microsoft Visual Studio\18\Community\Common7\Tools\VsDevCmd.bat"" -arch=x64 -host_arch=x64 >nul && cmake --build cmake-build-release --target akkaradb_wal_smoke_test --config Release"
```

## 実行例

```powershell
cmake-build-release\bin\akkaradb_memtable_smoke_test.exe
cmake-build-release\bin\akkaradb_wal_smoke_test.exe
cmake-build-release\bin\akkaradb_sstable_smoke_test.exe
cmake-build-release\bin\akkaradb_typed_api_smoke_test.exe
cmake-build-release\bin\akkaradb_manifest_smoke_test.exe
cmake-build-release\bin\akkaradb_versionlog_smoke_test.exe
```

typed API comparison:

```powershell
cmake-build-release\bin\akkaradb_benchmark.exe
```

MemTable throughput:

```powershell
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 200000 --writers=16 --backend=art
```

WAL throughput:

```powershell
cmake-build-release\bin\akkaradb_wal_throughput_benchmark.exe 200000 --writers=16 --sync=async --group-n=2048 --group-micros=500 --group-bytes=4MiB --max-pending-bytes=64MiB
```

SSTable throughput:

```powershell
cmake-build-release\bin\akkaradb_sstable_throughput_benchmark.exe 200000 --readers=16 --codec=zstd --block-size=32KiB --cache-bytes=64MiB
```

MemTable / WAL と同じ operation count で比べたい場合は `--same-ops` を付けてください。

```powershell
cmake-build-release\bin\akkaradb_sstable_throughput_benchmark.exe 200000 --writers=16 --codec=zstd --block-size=32KiB --cache-bytes=64MiB --same-ops
```

SSTable Bloom negative lookup:

```powershell
cmake-build-release\bin\akkaradb_sstable_bloom_negative_lookup_benchmark.exe 1000000 5000000 --writers=16 --bits-per-key=10 --codec=zstd --cache-bytes=64MiB
```

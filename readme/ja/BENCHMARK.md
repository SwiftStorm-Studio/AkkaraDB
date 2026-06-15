# AkkaraDB ベンチマークメモ

このファイルは、現時点で `akkaradb-native` に存在し、ビルドできるベンチマーク / smoke test の索引です。

## 現在使うターゲット

| ターゲット                                    | 目的                                                                                             |
|------------------------------------------|------------------------------------------------------------------------------------------------|
| `akkaradb_memtable_smoke_test`           | MemTable backend の基本動作、snapshot、iterator、flush callback などの smoke test。                        |
| `akkaradb_memtable_throughput_benchmark` | Sharded MemTable の put / get / scan throughput と latency sample を測る。                           |
| `akkaradb_wal_smoke_test`                | WAL append / recovery / tombstone / async force sync / corruption handling / rotation を確認する。   |
| `akkaradb_wal_throughput_benchmark`      | Sharded WAL の append / close drain / recovery throughput を測る。詳細は `readme/ja/WAL_SETUP.md` を参照。 |
| `akkaradb_manifest_smoke_test`           | Manifest の SST lifecycle record、compaction commit、CRC replay などを確認する。                          |
| `akkaradb_versionlog_smoke_test`         | VersionLog の append / history / getAt / rollback 系の基本動作を確認する。                                 |

## ビルド例

Release で個別ターゲットをビルドします。

```powershell
cmake --build cmake-build-release --target akkaradb_memtable_throughput_benchmark --config Release
cmake --build cmake-build-release --target akkaradb_wal_throughput_benchmark --config Release
```

全 smoke test を一通り見る場合は、次のように個別にビルドします。

```powershell
cmake --build cmake-build-release --target akkaradb_memtable_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_wal_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_manifest_smoke_test --config Release
cmake --build cmake-build-release --target akkaradb_versionlog_smoke_test --config Release
```

Windows の通常 PowerShell で MSVC の標準ヘッダーが見つからない場合は、Visual Studio の developer environment を通してください。

```powershell
cmd /c "call ""C:\Program Files\Microsoft Visual Studio\18\Community\Common7\Tools\VsDevCmd.bat"" -arch=x64 -host_arch=x64 >nul && cmake --build cmake-build-release --target akkaradb_wal_smoke_test --config Release"
```

## 実行例

```powershell
cmake-build-release\bin\akkaradb_memtable_smoke_test.exe
cmake-build-release\bin\akkaradb_wal_smoke_test.exe
cmake-build-release\bin\akkaradb_manifest_smoke_test.exe
cmake-build-release\bin\akkaradb_versionlog_smoke_test.exe
```

MemTable throughput:

```powershell
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 200000 --writers=16 --backend=art
```

WAL throughput:

```powershell
cmake-build-release\bin\akkaradb_wal_throughput_benchmark.exe 200000 --writers=16 --sync=async --group-n=2048 --group-micros=500 --group-bytes=4MiB --max-pending-bytes=64MiB
```
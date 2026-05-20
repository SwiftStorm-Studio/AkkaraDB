# 複数環境ベンチマーク結果

このファイルは、複数の開発環境で測定した AkkaraDB native のベンチマーク結果をまとめるための記録です。
CPU、RAM、SSD、OS、コンパイラ、電源設定の影響を受けるため、異なるマシン間の順位付けではなく、環境差を含めた参考値として扱います。
回帰確認では、同一マシン・同一 commit・同一ビルド設定・同一コマンドでの before/after 比較を基本にします。

## 追加ルール

新しい環境を追加するときは、下の `<details>` ブロックをコピーして、Environment、Commands、Results を同じ形式で追記します。
Storage は CrystalDiskMark や fio など、測定ツールと条件を一緒に残します。公称値を使う場合は `Vendor spec` と明記します。

## Benchmark Environments

<details>
<summary>Machine A - MemTable</summary>

### Environment

| Item                      | Value                                            |
|---------------------------|--------------------------------------------------|
| CPU                       | TBD                                              |
| RAM                       | TBD                                              |
| Storage                   | NVMe SSD, 2 TB class                             |
| Storage Benchmark Tool    | CrystalDiskMark 8.0.6 x64                        |
| Storage Benchmark Profile | 4 runs, 64 MiB, C: 66% used, MB/s and IOPS modes |
| Storage SEQ1M Q8T1        | Read 4706.07 MB/s / Write 4451.08 MB/s           |
| Storage SEQ1M Q8T1 IOPS   | Read 4488.06 / Write 4244.88                     |
| Storage SEQ1M Q1T1        | Read 3914.71 MB/s / Write 4323.56 MB/s           |
| Storage SEQ1M Q1T1 IOPS   | Read 3733.36 / Write 4123.27                     |
| Storage RND4K Q32T1       | Read 557.91 MB/s / Write 425.80 MB/s             |
| Storage RND4K Q32T1 IOPS  | Read 136208.01 / Write 103955.81                 |
| Storage RND4K Q1T1        | Read 70.93 MB/s / Write 241.13 MB/s              |
| Storage RND4K Q1T1 IOPS   | Read 17317.87 / Write 58869.63                   |
| OS                        | Windows                                          |
| Compiler                  | TBD                                              |
| Build Type                | Release                                          |
| Commit                    | `f465a8f`                                        |

### Commands

```powershell
cmake --build cmake-build-release --target akkaradb_memtable_throughput_benchmark --config Release
cmake --build cmake-build-release --target akkaradb_sstable_throughput_benchmark --config Release
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 500000 --backend=bptree --writers=16
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 500000 --backend=art --writers=16
cmake-build-release\bin\akkaradb_sstable_throughput_benchmark.exe 500000 --readers=16 --codec=zstd --block-size=32KiB --cache-bytes=64MiB
```

### MemTable B+Tree Results

Common settings:

```text
ops_per_case = 500000
backend = bptree
writer_threads = 16
resolved_shards = 64 (auto, writers*4 locality heuristic, cap 128)
threshold_bytes_per_shard = disabled
flush_after_scan = OFF
warmup_ops = 50000
prehash_mode = OFF (hash inside put)
```

| Key | Value | Shards | Writers | Put ops/s | Get ops/s | Scan ops/s |     Mem bytes |
|----:|------:|-------:|--------:|----------:|----------:|-----------:|--------------:|
|   8 |    16 |     64 |      16 | 5,191,844 | 6,019,433 |  3,779,332 |    86,177,512 |
|  16 |    64 |     64 |      16 | 4,093,975 | 4,039,503 |  2,559,858 |   114,063,256 |
|  16 |   256 |     64 |      16 | 2,941,249 | 3,812,286 |  3,886,556 |   210,035,464 |
|  32 |  1024 |     64 |      16 | 3,588,072 | 4,697,301 |  2,909,949 |   602,120,384 |
|  32 |  4096 |     64 |      16 | 2,126,750 | 4,084,210 |  2,959,307 | 2,138,050,904 |
|  64 | 16384 |     64 |      16 |   828,944 | 4,276,008 |  2,725,151 | 8,298,131,192 |

| Key | Value |              Put P50/P90/P99/P999 |           Get P50/P90/P99/P999 |        Scan P50/P90/P99/P999 |
|----:|------:|----------------------------------:|-------------------------------:|-----------------------------:|
|   8 |    16 |   1.60 / 3.20 / 28.80 / 154.60 us |  1.60 / 2.20 / 2.80 / 14.50 us | 0.01 / 0.01 / 0.02 / 0.18 us |
|  16 |    64 |   1.70 / 3.20 / 36.00 / 181.00 us |  1.90 / 2.60 / 3.30 / 43.40 us | 0.01 / 0.01 / 0.02 / 0.03 us |
|  16 |   256 |   1.80 / 3.40 / 19.60 / 264.50 us |  2.10 / 2.70 / 4.10 / 90.10 us | 0.01 / 0.01 / 0.01 / 0.02 us |
|  32 |  1024 |   2.30 / 4.20 / 31.20 / 220.20 us |   1.90 / 2.50 / 3.10 / 5.20 us | 0.01 / 0.01 / 0.03 / 0.06 us |
|  32 |  4096 |  4.20 / 10.50 / 71.60 / 194.60 us | 2.50 / 3.40 / 5.20 / 143.00 us | 0.01 / 0.01 / 0.02 / 0.03 us |
|  64 | 16384 | 9.80 / 36.70 / 113.00 / 379.60 us |  2.80 / 3.70 / 4.90 / 65.70 us | 0.01 / 0.01 / 0.04 / 0.17 us |

| Key | Value | Put ms | Get ms | Scan ms | Flush ms | Put MiB/s | Get MiB/s | Scan MiB/s | Flushes |
|----:|------:|-------:|-------:|--------:|---------:|----------:|----------:|-----------:|--------:|
|   8 |    16 |  96.30 |  83.06 |  134.08 |     0.00 |    118.83 |    137.77 |      85.35 |       0 |
|  16 |    64 | 122.13 | 123.78 |  196.37 |     0.00 |    312.35 |    308.19 |     194.26 |       0 |
|  16 |   256 | 170.00 | 131.15 |  130.17 |     0.00 |    762.96 |    988.90 |     996.38 |       0 |
|  32 |  1024 | 139.35 | 106.44 |  173.43 |     0.00 |   3613.48 |   4730.56 |    2903.34 |       0 |
|  32 |  4096 | 235.10 | 122.42 |  171.12 |     0.00 |   8372.52 |  16078.59 |   11502.84 |       0 |
|  64 | 16384 | 603.18 | 116.93 |  185.14 |     0.00 |  13002.85 |  67073.61 |   42362.70 |       0 |

Notes:

- `put_smp` / `get_smp` / `scan_smp` are all `7813` samples in this run.
- `flush_records` is `0` for every case because `threshold_bytes_per_shard` is disabled.

### MemTable ART Results

Common settings:

```text
ops_per_case = 500000
backend = art
writer_threads = 16
resolved_shards = 64 (auto, writers*4 locality heuristic, cap 128)
threshold_bytes_per_shard = disabled
flush_after_scan = OFF
warmup_ops = 50000
prehash_mode = OFF (hash inside put)
```

| Key | Value | Shards | Writers | Put ops/s |  Get ops/s | Scan ops/s |     Mem bytes |
|----:|------:|-------:|--------:|----------:|-----------:|-----------:|--------------:|
|   8 |    16 |     64 |      16 | 3,050,987 | 16,099,015 |  2,426,133 |   897,839,144 |
|  16 |    64 |     64 |      16 | 3,083,373 | 10,039,012 |  2,096,695 |   930,813,577 |
|  16 |   256 |     64 |      16 | 3,096,504 |  8,811,054 |  2,519,827 | 1,026,778,411 |
|  32 |  1024 |     64 |      16 | 2,351,597 | 11,369,011 |  2,217,326 | 1,428,708,283 |
|  32 |  4096 |     64 |      16 | 1,532,639 |  9,071,266 |  1,816,879 | 2,964,768,056 |
|  64 | 16384 |     64 |      16 |   296,127 |  9,409,799 |  1,876,801 | 9,144,865,191 |

| Key | Value |                Put P50/P90/P99/P999 |          Get P50/P90/P99/P999 |        Scan P50/P90/P99/P999 |
|----:|------:|------------------------------------:|------------------------------:|-----------------------------:|
|   8 |    16 |     2.20 / 6.90 / 58.00 / 318.50 us |  0.80 / 1.10 / 1.50 / 2.60 us | 0.00 / 0.01 / 0.02 / 0.18 us |
|  16 |    64 |     2.20 / 6.30 / 40.40 / 154.30 us | 1.00 / 1.40 / 2.00 / 17.60 us | 0.00 / 0.01 / 0.01 / 0.02 us |
|  16 |   256 |     2.40 / 6.10 / 63.80 / 251.60 us | 1.20 / 1.60 / 2.10 / 17.10 us | 0.00 / 0.01 / 0.01 / 0.02 us |
|  32 |  1024 |     3.00 / 9.10 / 74.90 / 238.00 us | 1.00 / 1.40 / 1.90 / 20.70 us | 0.01 / 0.01 / 0.01 / 0.02 us |
|  32 |  4096 |   4.40 / 15.30 / 107.90 / 368.80 us |  1.00 / 1.40 / 2.00 / 4.00 us | 0.01 / 0.01 / 0.02 / 0.17 us |
|  64 | 16384 | 13.50 / 175.20 / 369.00 / 624.30 us |  1.20 / 1.70 / 2.20 / 3.00 us | 0.00 / 0.01 / 0.01 / 0.03 us |

| Key | Value |  Put ms | Get ms | Scan ms | Flush ms | Put MiB/s | Get MiB/s | Scan MiB/s | Flushes |
|----:|------:|--------:|-------:|--------:|---------:|----------:|----------:|-----------:|--------:|
|   8 |    16 |  163.88 |  31.06 |  207.88 |     0.00 |     69.83 |    368.48 |      55.05 |       0 |
|  16 |    64 |  162.16 |  49.81 |  239.83 |     0.00 |    235.24 |    765.92 |     159.06 |       0 |
|  16 |   256 |  161.47 |  56.75 |  199.45 |     0.00 |    803.23 |   2285.58 |     650.27 |       0 |
|  32 |  1024 |  212.62 |  43.98 |  226.83 |     0.00 |   2368.25 |  11449.50 |    2219.88 |       0 |
|  32 |  4096 |  326.23 |  55.12 |  277.50 |     0.00 |   6033.65 |  35711.46 |    7093.38 |       0 |
|  64 | 16384 | 1688.46 |  53.14 |  268.28 |     0.00 |   4645.06 | 147602.43 |   29234.33 |       0 |

Notes:

- `put_smp` / `get_smp` / `scan_smp` are all `7813` samples in this run.
- `flush_records` is `0` for every case because `threshold_bytes_per_shard` is disabled.

### SSTable Results

Common settings:

```text
ops_per_case = 500000
codec = zstd
block_size = 32768
block_cache_bytes = 64.00 MiB
max_case_payload_bytes = 512.00 MiB
reader_threads = 16
warmup_ops = 50000
```

| Key | Value | Ops | Readers | Write ops/s | Get ops/s | Scan ops/s | SST bytes |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 16 | 500,000 | 16 | 3,141,082 | 2,266,773 | 11,003,521 | 11,878,856 |
| 16 | 64 | 500,000 | 16 | 2,477,237 | 2,003,691 | 6,062,906 | 15,246,632 |
| 16 | 256 | 500,000 | 16 | 1,976,265 | 1,725,708 | 3,839,167 | 17,199,432 |
| 32 | 1024 | 500,000 | 16 | 594,141 | 671,074 | 2,012,104 | 37,808,512 |
| 32 | 4096 | 130,055 | 16 | 239,685 | 246,631 | 517,099 | 58,003,088 |
| 64 | 16384 | 32,640 | 16 | 26,453 | 52,959 | 53,360 | 333,609,296 |

| Key | Value | Get P50/P90/P99/P999 | Scan P50/P90/P99/P999 |
|---:|---:|---:|---:|
| 8 | 16 | 1.00 / 6.60 / 133.40 / 447.90 us | 0.08 / 0.09 / 0.18 / 0.31 us |
| 16 | 64 | 1.30 / 7.80 / 130.10 / 485.50 us | 0.08 / 0.16 / 1.14 / 1.79 us |
| 16 | 256 | 1.20 / 9.40 / 177.10 / 526.30 us | 0.10 / 0.59 / 0.99 / 2.01 us |
| 32 | 1024 | 1.00 / 19.70 / 526.60 / 1153.40 us | 0.45 / 0.68 / 1.09 / 3.28 us |
| 32 | 4096 | 1.40 / 246.50 / 767.50 / 1552.20 us | 1.79 / 2.56 / 3.83 / 6.39 us |
| 64 | 16384 | 265.10 / 592.60 / 1251.20 / 1710.40 us | 17.80 / 21.83 / 32.46 / 35.15 us |

| Key | Value | Write ms | Open ms | Get ms | Scan ms | Write file MiB/s | Get file MiB/s | Scan file MiB/s |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 16 | 159.18 | 6.77 | 220.58 | 45.44 | 71.17 | 51.36 | 249.31 |
| 16 | 64 | 201.84 | 8.48 | 249.54 | 82.47 | 72.04 | 58.27 | 176.31 |
| 16 | 256 | 253.00 | 9.10 | 289.74 | 130.24 | 64.83 | 56.61 | 125.95 |
| 32 | 1024 | 841.55 | 12.51 | 745.07 | 248.50 | 42.85 | 48.39 | 145.10 |
| 32 | 4096 | 542.61 | 10.26 | 527.33 | 251.51 | 101.94 | 104.90 | 219.94 |
| 64 | 16384 | 1233.87 | 12.57 | 616.33 | 611.70 | 257.85 | 516.21 | 520.12 |

| Key | Value | Write payload MiB/s | Get payload MiB/s | Scan payload MiB/s | Get samples | Scan samples |
|---:|---:|---:|---:|---:|---:|---:|
| 8 | 16 | 71.89 | 51.88 | 251.85 | 7,813 | 7,813 |
| 16 | 64 | 189.00 | 152.87 | 462.56 | 7,813 | 7,813 |
| 16 | 256 | 512.64 | 447.65 | 995.88 | 7,813 | 7,813 |
| 32 | 1024 | 598.35 | 675.82 | 2026.35 | 7,813 | 7,813 |
| 32 | 4096 | 943.58 | 970.93 | 2035.70 | 2,033 | 2,033 |
| 64 | 16384 | 414.95 | 830.71 | 837.00 | 510 | 510 |

Notes:

- `Ops` is capped by `max_case_payload_bytes` for large value cases, so `32/4096` and `64/16384` run fewer than 500,000 operations.
- `file MiB/s` is based on generated SST file bytes. `payload MiB/s` is based on key + value payload bytes.

</details>

## Copy Template

<details>
<summary>Machine X - Benchmark type</summary>

### Environment

| Item                      | Value   |
|---------------------------|---------|
| CPU                       | TBD     |
| RAM                       | TBD     |
| Storage                   | TBD     |
| Storage Benchmark Tool    | TBD     |
| Storage Benchmark Profile | TBD     |
| Storage SEQ1M Q8T1        | TBD     |
| Storage SEQ1M Q8T1 IOPS   | TBD     |
| Storage SEQ1M Q1T1        | TBD     |
| Storage SEQ1M Q1T1 IOPS   | TBD     |
| Storage RND4K Q32T1       | TBD     |
| Storage RND4K Q32T1 IOPS  | TBD     |
| Storage RND4K Q1T1        | TBD     |
| Storage RND4K Q1T1 IOPS   | TBD     |
| OS                        | TBD     |
| Compiler                  | TBD     |
| Build Type                | Release |
| Commit                    | `TBD`   |

### Commands

```powershell
TBD
```

### Results

| Benchmark | Main Settings | Throughput |    P99 | Notes |
|-----------|---------------|-----------:|-------:|-------|
| TBD       | TBD           |  TBD ops/s | TBD us |       |

</details>

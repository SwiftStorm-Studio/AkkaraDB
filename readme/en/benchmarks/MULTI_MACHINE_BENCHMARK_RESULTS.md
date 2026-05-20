# Multi-machine Benchmark Results

This file records AkkaraDB native benchmark results measured across multiple development environments.
CPU, RAM, SSD, OS, compiler, and power settings all affect the numbers, so results should be treated as reference values that include environment differences rather than as a direct ranking between machines.
For regression checks, compare before/after results on the same machine, same commit, same build settings, and same command.

## Adding New Results

When adding a new environment, copy the `<details>` block below and keep Environment, Commands, and Results in the same format.
For storage, record the measurement tool and conditions together, such as CrystalDiskMark, KDiskMark, or fio. If using vendor specifications, mark them explicitly as `Vendor spec`.

## Benchmark Environments

<details>
<summary>Machine A - MemTable / WAL / SSTable</summary>

### Environment

| Item                      | Value                                            |
|---------------------------|--------------------------------------------------|
| CPU                       | Intel Core i5-12500H                             |
| RAM                       | SO-DIMM DDR4 3200MT/s 64GB                       |
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
cmake --build cmake-build-release --target akkaradb_wal_throughput_benchmark --config Release
cmake --build cmake-build-release --target akkaradb_sstable_throughput_benchmark --config Release
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 500000 --backend=bptree --writers=16
cmake-build-release\bin\akkaradb_memtable_throughput_benchmark.exe 500000 --backend=art --writers=16
cmake-build-release\bin\akkaradb_wal_throughput_benchmark.exe 200000 --writers=16 --sync=async --group-n=2048 --group-micros=2000 --group-bytes=4MiB --max-pending-bytes=64MiB
cmake-build-release\bin\akkaradb_sstable_throughput_benchmark.exe 500000 --readers=16 --codec=zstd --block-size=32KiB --cache-bytes=64MiB
cmake-build-release\bin\akkaradb_sstable_bloom_negative_lookup_benchmark.exe 1000000 5000000 --writers=16 --bits-per-key=10 --codec=zstd --cache-bytes=64MiB
```

<details>
<summary>MemTable B+Tree Results</summary>


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

</details>

<details>
<summary>MemTable ART Results</summary>


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

</details>

<details>
<summary>WAL Results</summary>


Common settings:

```text
ops_per_case = 200000
sync_mode = async
writer_threads = 16
resolved_shards = 16 (auto, one shard per writer up to WAL limit 16)
group_n = 2048
group_micros = 2000
group_bytes = 4.00 MiB
max_pending_bytes = 64.00 MiB
warmup_ops = 50000
prehash_mode = OFF (hash inside append)
```

| Key | Value | Shards | Writers | Append ops/s | Finish ops/s | Recover ops/s |     WAL bytes |
|----:|------:|-------:|--------:|-------------:|-------------:|--------------:|--------------:|
|   8 |    16 |     16 |      16 |    8,463,638 |    1,630,755 |     2,078,421 |    11,200,768 |
|  16 |    64 |     16 |      16 |    8,264,736 |    1,574,285 |     1,310,911 |    22,400,768 |
|  16 |   256 |     16 |      16 |    2,365,344 |      893,931 |       694,187 |    60,800,768 |
|  32 |  1024 |     16 |      16 |      975,273 |      357,181 |       800,551 |   217,600,768 |
|  32 |  4096 |     16 |      16 |      280,292 |      132,717 |       296,034 |   832,000,768 |
|  64 | 16384 |     16 |      16 |       21,772 |       21,227 |        83,313 | 3,296,003,072 |

| Key | Value |                Append P50/P90/P99/P999 |
|----:|------:|----------------------------------------:|
|   8 |    16 |       0.40 / 1.30 / 57.80 / 183.00 us |
|  16 |    64 |       0.40 / 1.40 / 24.50 / 236.60 us |
|  16 |   256 |     0.50 / 2.40 / 37.60 / 1426.80 us |
|  32 |  1024 |     0.70 / 2.70 / 93.90 / 3694.70 us |
|  32 |  4096 |    1.80 / 7.00 / 212.70 / 3596.50 us |
|  64 | 16384 | 18.60 / 648.40 / 3994.90 / 10895.10 us |

| Key | Value | Append ms | Close ms | Total ms | Recover ms | Append MiB/s | Finish MiB/s | Recover MiB/s |
|----:|------:|----------:|---------:|---------:|-----------:|-------------:|-------------:|--------------:|
|   8 |    16 |     23.63 |    99.01 |   122.64 |      96.23 |       452.04 |        87.10 |        111.01 |
|  16 |    64 |     24.20 |   102.84 |   127.04 |     152.57 |       882.80 |       168.16 |        140.03 |
|  16 |   256 |     84.55 |   139.18 |   223.73 |     288.11 |       685.76 |       259.17 |        201.26 |
|  32 |  1024 |    205.07 |   354.87 |   559.94 |     249.83 |      1011.94 |       370.61 |        830.65 |
|  32 |  4096 |    713.54 |   793.43 |  1506.97 |     675.60 |      1112.00 |       526.53 |       1174.45 |
|  64 | 16384 |   9186.03 |   236.10 |  9422.13 |    2400.59 |       342.18 |       333.61 |       1309.39 |

Notes:

- `app_smp` is `3125` samples in this run.


</details>

<details>
<summary>SSTable Results</summary>


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

| Key | Value |     Ops | Readers | Write ops/s | Get ops/s | Scan ops/s |   SST bytes |
|----:|------:|--------:|--------:|------------:|----------:|-----------:|------------:|
|   8 |    16 | 500,000 |      16 |   3,141,082 | 2,266,773 | 11,003,521 |  11,878,856 |
|  16 |    64 | 500,000 |      16 |   2,477,237 | 2,003,691 |  6,062,906 |  15,246,632 |
|  16 |   256 | 500,000 |      16 |   1,976,265 | 1,725,708 |  3,839,167 |  17,199,432 |
|  32 |  1024 | 500,000 |      16 |     594,141 |   671,074 |  2,012,104 |  37,808,512 |
|  32 |  4096 | 130,055 |      16 |     239,685 |   246,631 |    517,099 |  58,003,088 |
|  64 | 16384 |  32,640 |      16 |      26,453 |    52,959 |     53,360 | 333,609,296 |

| Key | Value |                   Get P50/P90/P99/P999 |            Scan P50/P90/P99/P999 |
|----:|------:|---------------------------------------:|---------------------------------:|
|   8 |    16 |       1.00 / 6.60 / 133.40 / 447.90 us |     0.08 / 0.09 / 0.18 / 0.31 us |
|  16 |    64 |       1.30 / 7.80 / 130.10 / 485.50 us |     0.08 / 0.16 / 1.14 / 1.79 us |
|  16 |   256 |       1.20 / 9.40 / 177.10 / 526.30 us |     0.10 / 0.59 / 0.99 / 2.01 us |
|  32 |  1024 |     1.00 / 19.70 / 526.60 / 1153.40 us |     0.45 / 0.68 / 1.09 / 3.28 us |
|  32 |  4096 |    1.40 / 246.50 / 767.50 / 1552.20 us |     1.79 / 2.56 / 3.83 / 6.39 us |
|  64 | 16384 | 265.10 / 592.60 / 1251.20 / 1710.40 us | 17.80 / 21.83 / 32.46 / 35.15 us |

| Key | Value | Write ms | Open ms | Get ms | Scan ms | Write file MiB/s | Get file MiB/s | Scan file MiB/s |
|----:|------:|---------:|--------:|-------:|--------:|-----------------:|---------------:|----------------:|
|   8 |    16 |   159.18 |    6.77 | 220.58 |   45.44 |            71.17 |          51.36 |          249.31 |
|  16 |    64 |   201.84 |    8.48 | 249.54 |   82.47 |            72.04 |          58.27 |          176.31 |
|  16 |   256 |   253.00 |    9.10 | 289.74 |  130.24 |            64.83 |          56.61 |          125.95 |
|  32 |  1024 |   841.55 |   12.51 | 745.07 |  248.50 |            42.85 |          48.39 |          145.10 |
|  32 |  4096 |   542.61 |   10.26 | 527.33 |  251.51 |           101.94 |         104.90 |          219.94 |
|  64 | 16384 |  1233.87 |   12.57 | 616.33 |  611.70 |           257.85 |         516.21 |          520.12 |

| Key | Value | Write payload MiB/s | Get payload MiB/s | Scan payload MiB/s | Get samples | Scan samples |
|----:|------:|--------------------:|------------------:|-------------------:|------------:|-------------:|
|   8 |    16 |               71.89 |             51.88 |             251.85 |       7,813 |        7,813 |
|  16 |    64 |              189.00 |            152.87 |             462.56 |       7,813 |        7,813 |
|  16 |   256 |              512.64 |            447.65 |             995.88 |       7,813 |        7,813 |
|  32 |  1024 |              598.35 |            675.82 |            2026.35 |       7,813 |        7,813 |
|  32 |  4096 |              943.58 |            970.93 |            2035.70 |       2,033 |        2,033 |
|  64 | 16384 |              414.95 |            830.71 |             837.00 |         510 |          510 |

Notes:

- `Ops` is capped by `max_case_payload_bytes` for large value cases, so `32/4096` and `64/16384` run fewer than 500,000 operations.
- `file MiB/s` is based on generated SST file bytes. `payload MiB/s` is based on key + value payload bytes.

</details>

<details>
<summary>SSTable Bloom Negative Lookup Results</summary>


Common settings:

```text
keys = 1000000
negative_keys = 999999
probes = 5000000
codec = zstd
bits_per_key = 10
block_size = 32768
block_cache_bytes = 64.00 MiB
value_size = 1
reader_threads = 16
expected_absent = 5000000
```

|      Keys | Negative keys |    Probes | Readers | Negative ops/s | Total ms | Samples | Wrong present |
|----------:|--------------:|----------:|--------:|---------------:|---------:|--------:|--------------:|
| 1,000,000 |       999,999 | 5,000,000 |      16 |     34,333,632 |   145.63 |  78,125 |             0 |

|     P50 |     P90 |     P99 |    P999 |
|--------:|--------:|--------:|--------:|
| 0.20 us | 0.40 us | 0.70 us | 5.40 us |

| Write ms | Open ms |  SST bytes | Write file MiB/s |
|---------:|--------:|-----------:|-----------------:|
|   213.29 |    8.47 | 18,234,768 |            81.53 |

Notes:

- `wrong_present = 0`, so all `5,000,000` negative probes were correctly reported absent.

</details>

</details>



<details>
<summary>Machine B - MemTable / WAL / SSTable</summary>

### Environment

| Item                      | Value                                                       |
|---------------------------|-------------------------------------------------------------|
| CPU                       | Intel Core i3-10100F                                        |
| RAM                       | DIMM DDR4 2400 MT/s 32GB                                    |
| Storage                   | SATA3 SSD                                                   |
| Storage Benchmark Tool    | KDiskMark 3.1.4 / fio 3.36                                  |
| Storage Benchmark Profile | MB/s = 1,000,000 bytes/s, KB = 1000 bytes, KiB = 1024 bytes |
| Storage SEQ1M Q8T1        | Read 509.592 MB/s / Write 318.229 MB/s                      |
| Storage SEQ1M Q8T1 IOPS   | Read 497.6 / Write 310.8                                    |
| Storage SEQ1M Q1T1        | Read 427.708 MB/s / Write 301.323 MB/s                      |
| Storage SEQ1M Q1T1 IOPS   | Read 417.7 / Write 294.3                                    |
| Storage RND4K Q32T1       | Read 271.354 MB/s / Write 246.344 MB/s                      |
| Storage RND4K Q32T1 IOPS  | Read 67,838.7 / Write 61,586.0                              |
| Storage RND4K Q1T1        | Read 32.669 MB/s / Write 83.099 MB/s                        |
| Storage RND4K Q1T1 IOPS   | Read 8,167.5 / Write 20,774.7                               |
| OS                        | Ubuntu 24.04.1, Linux 6.17.0-29-generic x86_64              |
| Compiler                  | g++-15/gcc-15 15.2.0 (Ubuntu 15.2.0-14ubuntu1~24~ppa1)      |
| Build Type                | Release                                                     |
| Commit                    | `9bb1323`                                                   |

### Commands

```bash
cmake --build build --config Release
./build/akkaradb_memtable_throughput_benchmark 500000 --backend=bptree --writers=16
./build/akkaradb_memtable_throughput_benchmark 500000 --backend=art --writers=16
./build/akkaradb_wal_throughput_benchmark 200000 --writers=16 --sync=async --group-n=2048 --group-micros=2000 --group-bytes=4MiB --max-pending-bytes=64MiB
./build/akkaradb_sstable_throughput_benchmark 500000 --readers=16 --codec=zstd --block-size=32KiB --cache-bytes=64MiB
./build/akkaradb_sstable_bloom_negative_lookup_benchmark 1000000 5000000 --writers=16 --bits-per-key=10 --codec=zstd --cache-bytes=64MiB
```

<details>
<summary>MemTable B+Tree Results</summary>


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
|   8 |    16 |     64 |      16 | 2,146,262 | 4,582,648 |  5,550,846 |    86,100,312 |
|  16 |    64 |     64 |      16 | 1,917,360 | 4,348,937 |  5,558,240 |   114,123,472 |
|  16 |   256 |     64 |      16 | 2,296,091 | 4,231,919 |  3,563,769 |   210,043,184 |
|  32 |  1024 |     64 |      16 | 2,128,610 | 4,251,978 |  5,176,515 |   601,986,056 |
|  32 |  4096 |     64 |      16 | 1,151,222 | 3,464,149 |  4,998,299 | 2,138,121,928 |
|  64 | 16384 |     64 |      16 |   547,488 | 2,993,621 |  4,376,025 | 8,298,018,480 |

| Key | Value |               Put P50/P90/P99/P999 |          Get P50/P90/P99/P999 |        Scan P50/P90/P99/P999 |
|----:|------:|-----------------------------------:|------------------------------:|-----------------------------:|
|   8 |    16 |    1.21 / 3.25 / 89.45 / 464.27 us |  1.09 / 1.53 / 2.13 / 8.72 us | 0.01 / 0.01 / 0.01 / 0.02 us |
|  16 |    64 |   1.26 / 2.96 / 100.43 / 756.37 us | 1.20 / 1.59 / 2.01 / 13.37 us | 0.01 / 0.01 / 0.02 / 0.02 us |
|  16 |   256 |   1.32 / 3.76 / 94.55 / 1422.51 us |  1.31 / 1.78 / 2.43 / 9.32 us | 0.01 / 0.01 / 0.01 / 0.02 us |
|  32 |  1024 |    1.67 / 4.91 / 88.49 / 393.32 us | 1.43 / 1.96 / 3.01 / 15.19 us | 0.00 / 0.01 / 0.01 / 0.02 us |
|  32 |  4096 |  3.46 / 7.35 / 259.88 / 1264.52 us | 1.68 / 2.20 / 2.94 / 68.94 us | 0.00 / 0.01 / 0.01 / 0.02 us |
|  64 | 16384 | 8.88 / 19.00 / 433.22 / 2276.45 us | 1.91 / 2.53 / 3.50 / 44.53 us | 0.00 / 0.01 / 0.01 / 0.02 us |

| Key | Value | Put ms | Get ms | Scan ms | Flush ms | Put MiB/s | Get MiB/s | Scan MiB/s | Flushes |
|----:|------:|-------:|-------:|--------:|---------:|----------:|----------:|-----------:|--------:|
|   8 |    16 | 232.96 | 109.11 |   91.52 |     0.00 |     49.12 |    104.89 |     125.04 |       0 |
|  16 |    64 | 260.78 | 114.97 |   92.70 |     0.00 |    146.28 |    331.80 |     411.50 |       0 |
|  16 |   256 | 217.76 | 118.15 |  143.16 |     0.00 |    595.60 |   1097.76 |     905.96 |       0 |
|  32 |  1024 | 234.90 | 117.59 |   99.64 |     0.00 |   2143.68 |   4282.08 |    5053.40 |       0 |
|  32 |  4096 | 434.32 | 144.34 |  102.56 |     0.00 |   4532.09 |  13637.55 |   19192.94 |       0 |
|  64 | 16384 | 913.26 | 167.02 |  116.07 |     0.00 |   8587.92 |  46958.04 |   67569.30 |       0 |

Notes:

- `put_smp` / `get_smp` / `scan_smp` are all `7813` samples in this run.
- `flush_records` is `0` for every case because `threshold_bytes_per_shard` is disabled.

</details>

<details>
<summary>MemTable ART Results</summary>


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

| Key | Value | Shards | Writers | Put ops/s | Get ops/s | Scan ops/s |     Mem bytes |
|----:|------:|-------:|--------:|----------:|----------:|-----------:|--------------:|
|   8 |    16 |     64 |      16 | 1,848,346 | 3,636,770 |  2,824,253 |   897,854,809 |
|  16 |    64 |     64 |      16 | 1,944,106 | 7,270,405 |  2,216,150 |   930,793,878 |
|  16 |   256 |     64 |      16 | 2,188,106 | 5,603,356 |  2,855,008 | 1,026,743,657 |
|  32 |  1024 |     64 |      16 | 1,576,530 | 6,451,114 |  2,544,585 | 1,428,729,637 |
|  32 |  4096 |     64 |      16 | 1,063,198 | 8,685,070 |  2,378,497 | 2,964,825,309 |
|  64 | 16384 |     64 |      16 |   632,914 | 4,380,909 |  1,934,425 | 9,144,859,390 |

| Key | Value |              Put P50/P90/P99/P999 |           Get P50/P90/P99/P999 |        Scan P50/P90/P99/P999 |
|----:|------:|----------------------------------:|-------------------------------:|-----------------------------:|
|   8 |    16 |  1.62 / 5.95 / 113.53 / 790.29 us | 0.65 / 0.92 / 1.36 / 389.32 us | 0.01 / 0.01 / 0.01 / 0.02 us |
|  16 |    64 |  1.50 / 4.49 / 129.75 / 838.43 us |   0.72 / 1.01 / 1.37 / 3.13 us | 0.01 / 0.01 / 0.02 / 0.02 us |
|  16 |   256 |  1.52 / 5.37 / 103.72 / 308.09 us |   0.72 / 1.05 / 1.44 / 8.12 us | 0.01 / 0.01 / 0.01 / 0.02 us |
|  32 |  1024 | 1.76 / 5.25 / 164.41 / 2735.91 us |   0.76 / 1.04 / 1.38 / 8.08 us | 0.00 / 0.01 / 0.01 / 0.02 us |
|  32 |  4096 | 3.30 / 8.46 / 233.99 / 1019.88 us |   0.74 / 1.05 / 1.39 / 1.68 us | 0.00 / 0.01 / 0.01 / 0.02 us |
|  64 | 16384 | 8.98 / 19.56 / 368.85 / 878.10 us |   0.89 / 1.22 / 1.61 / 6.79 us | 0.01 / 0.01 / 0.01 / 0.02 us |

| Key | Value | Put ms | Get ms | Scan ms | Flush ms | Put MiB/s | Get MiB/s | Scan MiB/s | Flushes |
|----:|------:|-------:|-------:|--------:|---------:|----------:|----------:|-----------:|--------:|
|   8 |    16 | 270.51 | 137.48 |  178.37 |     0.00 |     42.31 |     83.24 |      64.16 |       0 |
|  16 |    64 | 257.19 |  68.77 |  225.62 |     0.00 |    148.32 |    554.69 |     169.08 |       0 |
|  16 |   256 | 228.51 |  89.23 |  175.13 |     0.00 |    567.59 |   1453.51 |     740.57 |       0 |
|  32 |  1024 | 317.15 |  77.51 |  196.50 |     0.00 |   1587.69 |   6496.79 |    2562.57 |       0 |
|  32 |  4096 | 470.28 |  57.57 |  212.28 |     0.00 |   4185.56 |  34191.10 |    9272.78 |       0 |
|  64 | 16384 | 790.00 | 114.13 |  258.48 |     0.00 |   9927.92 |  68719.09 |   30343.17 |       0 |

Notes:

- `put_smp` / `get_smp` / `scan_smp` are all `7813` samples in this run.
- `flush_records` is `0` for every case because `threshold_bytes_per_shard` is disabled.

</details>

<details>
<summary>WAL Results</summary>


Common settings:

```text
ops_per_case = 200000
sync_mode = async
writer_threads = 16
resolved_shards = 16 (auto, one shard per writer up to WAL limit 16)
group_n = 2048
group_micros = 2000
group_bytes = 4.00 MiB
max_pending_bytes = 64.00 MiB
warmup_ops = 50000
prehash_mode = OFF (hash inside append)
```

| Key | Value | Shards | Writers | Append ops/s | Finish ops/s | Recover ops/s |     WAL bytes |
|----:|------:|-------:|--------:|-------------:|-------------:|--------------:|--------------:|
|   8 |    16 |     16 |      16 |   10,116,718 |    2,580,742 |     6,680,610 |    11,200,768 |
|  16 |    64 |     16 |      16 |   11,857,453 |    1,651,105 |     3,075,210 |    22,400,768 |
|  16 |   256 |     16 |      16 |    8,800,358 |      983,130 |     2,055,080 |    60,800,768 |
|  32 |  1024 |     16 |      16 |    3,005,186 |      317,220 |     2,127,389 |   217,600,768 |
|  32 |  4096 |     16 |      16 |      813,127 |       87,237 |       680,023 |   832,000,768 |
|  64 | 16384 |     16 |      16 |       11,095 |       10,880 |       204,836 | 3,296,003,072 |

| Key | Value |           Append P50/P90/P99/P999 |
|----:|------:|----------------------------------:|
|   8 |    16 |   0.30 / 0.80 / 11.38 / 131.50 us |
|  16 |    64 |    0.35 / 0.83 / 7.68 / 128.91 us |
|  16 |   256 |   0.39 / 0.81 / 11.96 / 293.68 us |
|  32 |  1024 |   0.53 / 5.10 / 10.03 / 382.33 us |
|  32 |  4096 | 5.61 / 10.32 / 22.04 / 3171.13 us |
|  64 | 16384 | 5.46 / 10.61 / 57.47 / 3247.76 us |

| Key | Value | Append ms | Close ms | Total ms | Recover ms | Append MiB/s | Finish MiB/s | Recover MiB/s |
|----:|------:|----------:|---------:|---------:|-----------:|-------------:|-------------:|--------------:|
|   8 |    16 |     19.77 |    57.73 |    77.50 |      29.94 |       540.33 |       137.84 |        356.81 |
|  16 |    64 |     16.87 |   104.26 |   121.13 |      65.04 |      1266.56 |       176.36 |        328.48 |
|  16 |   256 |     22.73 |   180.71 |   203.43 |      97.32 |      2551.41 |       285.03 |        595.81 |
|  32 |  1024 |     66.55 |   563.93 |   630.48 |      94.01 |      3118.19 |       329.15 |       2207.38 |
|  32 |  4096 |    245.96 |  2046.65 |  2292.61 |     294.11 |      3225.91 |       346.09 |       2697.85 |
|  64 | 16384 |  18026.88 |   356.05 | 18382.93 |     976.39 |       174.37 |       170.99 |       3219.32 |

Notes:

- `app_smp` is `3125` samples in this run.

</details>

<details>
<summary>SSTable Results</summary>


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

| Key | Value |     Ops | Readers | Write ops/s | Get ops/s | Scan ops/s |   SST bytes |
|----:|------:|--------:|--------:|------------:|----------:|-----------:|------------:|
|   8 |    16 | 500,000 |      16 |   4,434,070 | 2,531,292 | 11,355,396 |  11,878,856 |
|  16 |    64 | 500,000 |      16 |   3,055,120 | 2,613,912 |  2,911,598 |  15,246,632 |
|  16 |   256 | 500,000 |      16 |   1,729,165 | 2,135,396 |  5,164,493 |  17,199,432 |
|  32 |  1024 | 500,000 |      16 |     763,040 | 1,110,222 |  2,115,538 |  37,808,512 |
|  32 |  4096 | 130,055 |      16 |     304,630 |   639,356 |    697,333 |  58,003,088 |
|  64 | 16384 |  32,640 |      16 |      15,220 |   216,508 |     73,060 | 333,609,296 |

| Key | Value |                Get P50/P90/P99/P999 |            Scan P50/P90/P99/P999 |
|----:|------:|------------------------------------:|---------------------------------:|
|   8 |    16 |     1.14 / 9.06 / 75.02 / 448.45 us |    0.04 / 0.07 / 0.09 / 11.93 us |
|  16 |    64 |     1.20 / 8.99 / 92.22 / 234.75 us |    0.07 / 0.98 / 1.90 / 16.41 us |
|  16 |   256 |     1.44 / 6.73 / 97.57 / 607.27 us |     0.04 / 0.63 / 0.91 / 1.62 us |
|  32 |  1024 |   1.22 / 29.76 / 172.15 / 644.11 us |    0.33 / 0.61 / 1.20 / 13.63 us |
|  32 |  4096 |   1.70 / 80.05 / 191.62 / 263.75 us |     1.44 / 1.65 / 2.69 / 3.30 us |
|  64 | 16384 | 59.36 / 149.25 / 254.89 / 282.66 us | 12.43 / 14.57 / 27.11 / 30.86 us |

| Key | Value | Write ms | Open ms | Get ms | Scan ms | Write file MiB/s | Get file MiB/s | Scan file MiB/s |
|----:|------:|---------:|--------:|-------:|--------:|-----------------:|---------------:|----------------:|
|   8 |    16 |   112.76 |    0.10 | 197.53 |   44.03 |           100.46 |          57.35 |          257.28 |
|  16 |    64 |   163.66 |    0.26 | 191.28 |  171.73 |            88.84 |          76.01 |           84.67 |
|  16 |   256 |   289.16 |    0.36 | 234.15 |   96.81 |            56.73 |          70.05 |          169.42 |
|  32 |  1024 |   655.27 |    0.42 | 450.36 |  236.35 |            55.03 |          80.06 |          152.56 |
|  32 |  4096 |   426.93 |    0.35 | 203.42 |  186.50 |           129.57 |         271.94 |          296.60 |
|  64 | 16384 |  2144.56 |    0.98 | 150.76 |  446.76 |           148.35 |        2110.38 |          712.14 |

| Key | Value | Write payload MiB/s | Get payload MiB/s | Scan payload MiB/s | Get samples | Scan samples |
|----:|------:|--------------------:|------------------:|-------------------:|------------:|-------------:|
|   8 |    16 |              101.49 |             57.94 |             259.90 |       7,813 |        7,813 |
|  16 |    64 |              233.09 |            199.43 |             222.14 |       7,813 |        7,813 |
|  16 |   256 |              448.54 |            553.92 |            1339.67 |       7,813 |        7,813 |
|  32 |  1024 |              768.44 |           1118.08 |            2130.52 |       7,813 |        7,813 |
|  32 |  4096 |             1199.26 |           2517.00 |            2745.24 |       2,033 |        2,033 |
|  64 | 16384 |              238.74 |           3396.15 |            1146.02 |         510 |          510 |

Notes:

- `Ops` is capped by `max_case_payload_bytes` for large value cases, so `32/4096` and `64/16384` run fewer than 500,000 operations.
- `file MiB/s` is based on generated SST file bytes. `payload MiB/s` is based on key + value payload bytes.

</details>

<details>
<summary>SSTable Bloom Negative Lookup Results</summary>


Common settings:

```text
keys = 1000000
negative_keys = 999999
probes = 5000000
codec = zstd
bits_per_key = 10
block_size = 32768
block_cache_bytes = 64.00 MiB
value_size = 1
reader_threads = 16
expected_absent = 5000000
```

|      Keys | Negative keys |    Probes | Readers | Negative ops/s | Total ms | Samples | Wrong present |
|----------:|--------------:|----------:|--------:|---------------:|---------:|--------:|--------------:|
| 1,000,000 |       999,999 | 5,000,000 |      16 |     21,540,523 |   232.12 |  78,125 |             0 |

|     P50 |     P90 |     P99 |    P999 |
|--------:|--------:|--------:|--------:|
| 0.24 us | 0.53 us | 0.86 us | 2.69 us |

| Write ms | Open ms |  SST bytes | Write file MiB/s |
|---------:|--------:|-----------:|-----------------:|
|   384.65 |   25.23 | 18,234,768 |            45.21 |

Notes:

- `wrong_present = 0`, so all `5,000,000` negative probes were correctly reported absent.

</details>

### Machine Notes

- MemTable `put_smp` / `get_smp` / `scan_smp` are all `7813` samples in this run.
- MemTable `flush_records` is `0` for every case because `threshold_bytes_per_shard` is disabled.
- KDiskMark latency details: SEQ1M Q8T1 read/write 16322.80 / 30387.55 us, SEQ1M Q1T1 2390.64 / 3486.27 us, RND4K Q32T1 470.97 / 603.84 us, RND4K Q1T1 121.55 /
  47.83 us.

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

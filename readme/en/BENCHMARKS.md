# Benchmarks

Performance measurement results for AkkaraDB v3.

## 📚 Table of Contents

- [Test Environment](#test-environment)
- [Write Performance](#write-performance)
- [Read Performance](#read-performance)
- [Mixed Workloads](#mixed-workloads)
- [Scalability](#scalability)
- [Tuning Guide](#tuning-guide)
- [Overall Assessment](#overall-assessment)

---

## Test Environment

### Hardware

| Item             | Specification                                |
|:-----------------|:---------------------------------------------|
| **CPU**          | Intel Core i5-12500H (12 cores, 2.5-4.5 GHz) |
| **Memory**       | 64 GB DDR4-3200                              |
| **Storage**      | NVMe SSD (T-FORCE Z44A5, 2TB)                |
| Sequential Read  | 4,500 MB/s                                   |
| Sequential Write | 4,500 MB/s                                   |
| Random Read 4K   | 1,000k IOPS                                  |
| Random Write 4K  | 1,000k IOPS                                  |
| **OS**           | Windows 11 (23H2/22631.5335)                 |

### Software

| Item            | Version                                                                                 |
|:----------------|:----------------------------------------------------------------------------------------|
| **JDK**         | OpenJDK 21.0.1                                                                          |
| **Kotlin**      | 2.2.21                                                                                  |
| **AkkaraDB**    | v0.2.9 (v3)                                                                             |
| **JVM Options** | `-Xmx4G -XX:+UseG1GC -XX:+AlwaysPreTouch -XX:MaxGCPauseMillis=50 -Dfile.encoding=UTF-8` |

### Benchmark Configuration

```kotlin
// Common configuration
val keyCount = 100_000  // unless otherwise noted
val valueSize = 64      // bytes
val threads = 1         // single-threaded measurement

// Database configuration (unless otherwise noted)
AkkaraDB.Options(
    k = 4,
    m = 2,
    walGroupN = 512,
    walGroupMicros = 50_000,
    walFastMode = true,
    stripeFastMode = true
)
```

---

## Write Performance

### WAL Group Commit Tuning

Measured impact of WAL group commit settings (100,000 keys, 64B values).

| # | WalGroupN | WalGroupMicros |    ops/sec | p50 (µs) | p90 (µs) |   p99 (µs) | Notes                                  |
|--:|----------:|---------------:|-----------:|---------:|---------:|-----------:|:---------------------------------------|
| ① |        64 |          1,000 |      4,004 |      5.9 |     16.1 | **15,279** | Excessive fsync (full sync bottleneck) |
| ② |       128 |          5,000 |      8,204 |      3.3 |      8.3 |       36.5 | Significantly faster                   |
| ③ |       128 |         10,000 |      8,201 |      4.4 |      8.9 |       38.8 | Time-driven batch stabilized           |
| ④ |       256 |          1,000 |     16,137 |      4.6 |      7.5 |       24.2 | Batch effect prominent                 |
| ⑤ |       256 |         10,000 |     16,170 |      2.8 |      6.4 |       22.7 | Balanced                               |
| ⑥ |       512 |         10,000 | **31,869** |      3.0 |      5.5 |   **16.8** | **Peak throughput**                    |
| ⑦ |       512 |         50,000 |     30,660 |      2.9 |      5.0 |       17.5 | Recommended setting                    |

**Graph: p99 Latency vs WalGroupN**

```
p99 (µs)
15000 |  ①
      |
 1000 |
      |
  100 |
      |    ②③
   50 |       ④⑤
      |          ⑥⑦
    0 +----+----+----+----+
      64  128  256  512
           WalGroupN
```

**Conclusions:**

- **Throughput**: Scales almost linearly with WalGroupN
- **WalGroupN=64**: Excessive fsync causes p99 > 15ms
- **Optimal setting**: `WalGroupN=512, WalGroupMicros=10,000`
    - Throughput: ≈ 32k ops/s
    - p99: ≈ 17 µs
- **Recommended setting**: `WalGroupN=512, WalGroupMicros=50,000`
    - Durability window: 50ms
    - Good throughput/latency balance

---

### Scalability by Key Count

Measured impact of increasing key count.

| Key Count | Write Time | ops/sec | p50 (µs) | p99 (µs) | Notes                     |
|----------:|-----------:|--------:|---------:|---------:|:--------------------------|
|       10k |      0.28s |  37,597 |      2.8 |     12.1 | Warm-up phase             |
|      100k |      3.15s |  32,639 |      3.0 |     17.2 | Stable operation          |
|        1M |     32.07s |  32,193 |      3.5 |     18.7 | Linear scaling maintained |

**Conclusions:**

- Perfectly linear scaling up to 1M keys
- Throughput stable at 32k ops/sec
- Maintains p99 ≤ 18µs

---

### Impact of Value Size

Measured impact of varying value size (100,000 keys).

| ValueSize | ops/sec | p50 (µs) | p99 (µs) | Throughput (MB/s) |
|----------:|--------:|---------:|---------:|------------------:|
|      16 B |  31,958 |      3.1 |     20.2 |               0.5 |
|      64 B |  38,022 |      3.4 |     24.5 |               2.3 |
|     256 B |  29,442 |      6.3 |     22.7 |               7.2 |
|      1 KB |  29,670 |      9.4 |     28.3 |              29.0 |
|      4 KB |  22,445 |     25.0 |     50.2 |              87.7 |
|     16 KB |  10,643 |     78.2 |    153.7 |             166.3 |

**Conclusions:**

- Small size (≤256B): Stable at ~30k ops/sec
- 64B achieves peak throughput (38k ops/sec)
- Large size (≥4KB): Copy cost becomes dominant
- 16KB: Achieves 166 MB/s throughput

---

## Read Performance

### MemTable vs SST

| Data Path | Benchmark | Keys | ValueSize |     ops/sec | p50 (µs) | p90 (µs) | p99 (µs) | Notes               |
|:----------|:----------|-----:|----------:|------------:|---------:|---------:|---------:|:--------------------|
| MemTable  | read      | 100k |      64 B | **538,656** |  **1.3** |  **3.6** |  **6.2** | Memory hit (no I/O) |
| SST       | read-sst  | 100k |      64 B | **108,196** |  **7.6** | **12.7** | **22.3** | Disk I/O occurs     |

**Latency Details (MemTable):**

| Percentile | Latency (µs) |
|-----------:|-------------:|
|        p50 |          1.3 |
|        p75 |          2.5 |
|        p90 |          3.6 |
|        p95 |          4.1 |
|        p99 |          6.2 |
|      p99.9 |         15.8 |
|     p99.99 |         49.9 |
|        max |        150.5 |

**Latency Details (SST):**

| Percentile | Latency (µs) |
|-----------:|-------------:|
|        p50 |          7.6 |
|        p75 |          8.8 |
|        p90 |         12.7 |
|        p95 |         15.0 |
|        p99 |         22.3 |
|      p99.9 |         73.1 |
|     p99.99 |        218.8 |
|        max |       9851.0 |

**Conclusions:**

- **MemTable hit**: 540k ops/sec, p99: 6.2µs
- **SST hit**: 110k ops/sec, p99: 22.3µs
- Both well exceed requirements (p99 ≤ 50µs)

---

### Bloom Filter Effect

Measured Bloom filter effect when searching for non-existent keys.

| Search Type     |     ops/sec | p50 (µs) | p99 (µs) | False Positive Rate |
|:----------------|------------:|---------:|---------:|--------------------:|
| Negative lookup | **673,753** |  **1.2** |  **6.3** |           **0.00%** |
| Positive lookup |     108,196 |      7.6 |     22.3 |                   — |

**Latency Details (Negative Lookup):**

| Percentile | Latency (µs) |
|-----------:|-------------:|
|        p50 |          1.2 |
|        p75 |          1.4 |
|        p90 |          1.8 |
|        p95 |          2.4 |
|        p99 |          6.3 |
|      p99.9 |         19.6 |
|     p99.99 |        103.5 |
|        max |        186.9 |

**Conclusions:**

- Bloom filter provides **6.2x speedup** (108k → 674k ops/sec)
- False positive rate: 0.00% (better than theoretical 1%)
- Non-existent key search rejects immediately without disk I/O

---

### Range Query Performance

```kotlin
// Range query example
// range returns Sequence<MemRecord>
db.range(startKey, endKey).forEach { rec ->
    // process
}
```

| Range Size | Total Time | Avg/Entry (µs) |
|-----------:|-----------:|---------------:|
|        100 |      12 ms |          123.8 |
|         1k |       8 ms |            8.5 |
|        10k |      51 ms |            5.1 |
|       100k |     507 ms |            5.1 |

**Conclusions:**

- Linear scaling with range size
- Larger ranges more efficient (≈5.1 µs/entry)
- Small ranges have relatively larger initialization overhead

---

## Mixed Workloads

### Impact of Read/Write Ratio

| Read % | Write % | Total ops/sec | Read p99 (µs) | Write p99 (µs) |
|-------:|--------:|--------------:|--------------:|---------------:|
|    100 |       0 |   **295,457** |          10.5 |              — |
|     80 |      20 |       155,324 |          10.2 |           26.7 |
|     50 |      50 |        61,024 |          10.4 |           27.2 |
|     20 |      80 |        35,455 |          11.2 |           27.5 |
|      0 |     100 |        30,971 |             — |           26.1 |

**Conclusions:**

- Higher read ratio increases overall throughput
- Read p99 stable regardless of ratio (10-11µs)
- Write p99 stable regardless of ratio (26-28µs)
- Minimal lock contention impact

---

## Scalability

### Multi-threaded Scalability

Measured multi-threaded performance on write workload.

| Threads | ops/sec | Scale Factor | Efficiency |
|--------:|--------:|-------------:|-----------:|
|       1 |  32,507 |        1.00x |       100% |
|       2 |  31,825 |        0.98x |        49% |
|       4 |  30,905 |        0.95x |        24% |
|       8 |  30,714 |        0.94x |        12% |
|      16 |  30,705 |        0.94x |         6% |

**Conclusions:**

- Writes bottlenecked by WAL fsync, don't scale
- Increasing threads maintains throughput (no degradation)
- Better scaling expected for read workloads

---

## Typed API (AkkDSL) Performance

Measured performance of Typed API with serialization overhead (10,000 keys).

| Mode       | Write ops/sec | Write p99 (µs) | Read ops/sec | Read p99 (µs) |
|:-----------|--------------:|---------------:|-------------:|--------------:|
| NORMAL     |         3,952 |       13,454.6 |       45,058 |          45.2 |
| FAST       |        15,909 |           92.6 |       49,811 |          37.3 |
| ULTRA_FAST |        28,681 |           72.8 |       46,408 |          39.7 |

**Conclusions:**

- ULTRA_FAST mode achieves ~90% of raw API performance
- Serialization overhead minimal (≈3µs/op)
- Reads exceed 45k ops/sec in all modes

---

## Buffer Pool Statistics

Pool statistics at benchmark end:

```
Pool stats: Stats(hits=20,840,475, misses=3,886, created=3,886, dropped=0, retained=2,510)
```

| Metric   |      Value | Description                |
|:---------|-----------:|:---------------------------|
| hits     | 20,840,475 | Successful pool retrievals |
| misses   |      3,886 | New allocations            |
| created  |      3,886 | Total buffers created      |
| dropped  |          0 | Discarded when pool full   |
| retained |      2,510 | Currently in pool          |

**Hit Rate: 99.98%**

---

## Tuning Guide

### Write Tail Improvement

**Goal:** Lower p99 latency

**Methods:**

#### 1. Increase WalGroupN (batching effect)

```kotlin
AkkaraDB.Options(
    walGroupN = 512  // Default: 64
)
```

#### 2. Adjust WalGroupMicros (durability tradeoff)

```kotlin
AkkaraDB.Options(
    walGroupMicros = 50_000  // 50ms
)
```

#### 3. Enable FastMode

```kotlin
AkkaraDB.Options(
    walFastMode = true,      // Use fdatasync
    stripeFastMode = true
)
```

**Effects:**

| Metric      |    Before |     After |
|:------------|----------:|----------:|
| p99 Latency | 15,279 µs |     17 µs |
| Throughput  |  4k ops/s | 32k ops/s |

---

### Read Tail Improvement

**Goal:** Lower latency on cache miss

**Methods:**

#### 1. Enable Bloom Filter (enabled by default)

```kotlin
AkkaraDB.Options(
    bloomFPRate = 0.01  // Default
)
```

#### 2. Keep data in MemTable

- Increase flush threshold to improve MemTable hit rate
- Tradeoff with memory usage

**Effects:**

| Data Path    |    p99 |
|:-------------|-------:|
| MemTable     |  6.2µs |
| SST          | 22.3µs |
| Bloom reject |  6.3µs |

---

## Overall Assessment

### Goal Achievement

| Metric                   | v3 Target   | Achieved                |  Status   |
|:-------------------------|:------------|:------------------------|:---------:|
| **Write P99**            | ≤ 200 µs    | **≈ 17 µs**             | ✅ **Met** |
| **Read P99**             | ≤ 50 µs     | **≈ 6 µs (MemTable)**   | ✅ **Met** |
|                          |             | **≈ 22 µs (SST)**       | ✅ **Met** |
| **Sustained Throughput** | ≥ 10k ops/s | **≈ 32k ops/s (write)** | ✅ **Met** |
|                          |             | **≈ 540k ops/s (read)** | ✅ **Met** |
| **Crash Safe**           | WAL + fsync | ✅ Verified              | ✅ **Met** |

### Conclusion

AkkaraDB v3 achieves **production-grade** performance:

- ✅ Write p99: 17µs (**12x** better than 200µs requirement)
- ✅ Read p99: 6.2µs (**8x** better than 50µs requirement)
- ✅ Fully asynchronous, crash-safe
- ✅ Linear scalability (1M keys)
- ✅ Buffer pool hit rate 99.98%

### Future Optimization Areas

- [ ] Multi-threaded compaction
- [ ] Explicit block cache management
- [ ] Pin index/Bloom to memory
- [ ] Adaptive WAL group commit
- [ ] Multi-threaded scaling for read workloads

---

Next: [Build](./BUILD.md) | [API Reference](./API_REFERENCE.md)

[Back to Overview](./ABOUT.md)

---
# AkkaraDB

AkkaraDB is a JVM-native, ultra-low-latency embedded key–value store written in Kotlin.

- Predictable tail latency and simple operational model
- Crash-safe on a single node; optional redundancy via striped parity (k data + m parity lanes)
- Zero external runtime dependencies (JDK + Kotlin only)

## Installation

### Gradle (Kotlin DSL) with Plugin

```kotlin
plugins {
    id("dev.swiftstorm.akkaradb-plugin") version "0.1.0+rc.1"
}

repositories {
    mavenCentral()
    maven("https://repo.swiftstorm.dev/maven2/")
}

dependencies {
    implementation("dev.swiftstorm:akkaradb:0.2.0")
}
```

### Gradle (Groovy DSL) with Plugin

```groovy
plugins {
    id 'dev.swiftstorm.akkaradb-plugin' version '0.1.0+rc.1'
}

repositories {
    mavenCentral()
    maven { url 'https://repo.swiftstorm.dev/maven2/' }
}

dependencies {
    implementation 'dev.swiftstorm:akkaradb:0.2.0'
}
```

### Maven

```xml
<repositories>
    <repository>
        <id>swiftstorm</id>
        <url>https://repo.swiftstorm.dev/maven2/</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>dev.swiftstorm</groupId>
        <artifactId>akkaradb</artifactId>
        <version>0.2.0</version>
    </dependency>
</dependencies>
```

## Quick Start

### Typed API with AkkaraDSL

```kotlin
// Define a data model
data class User(val name: String, val age: Int)

val base = java.nio.file.Paths.get("./data/akkdb")
// Open a typed table with NORMAL preset (you can also use FAST/DURABLE/ULTRA_FAST)
val users = dev.swiftstorm.akkaradb.engine.AkkDSL.open<User>(base, dev.swiftstorm.akkaradb.engine.StartupMode.NORMAL)

// Put and get using a composite key: "namespace:id<US>16-byte-UUID"
val id = dev.swiftstorm.akkaradb.common.ShortUUID.generate()
users.put("user", id, User(name = "Taro", age = 42))
val got: User? = users.get("user", id)

users.close()
```

### Low-level API

```kotlin
val base = java.nio.file.Paths.get("./data/akkdb")
val db = dev.swiftstorm.akkaradb.engine.AkkaraDB.open(
  dev.swiftstorm.akkaradb.engine.AkkaraDB.Options(baseDir = base)
)

val key = dev.swiftstorm.akkaradb.common.ByteBufferL.wrap(
  java.nio.charset.StandardCharsets.UTF_8.encode("hello")
).position(0)
val value = dev.swiftstorm.akkaradb.common.ByteBufferL.wrap(
  java.nio.charset.StandardCharsets.UTF_8.encode("world")
).position(0)

db.put(key, value)
val read = db.get(key)

db.flush()
db.close()
```

## Architecture

### Modules

- `akkara/common` – shared primitives (ByteBufferL, hashing, small buffer pools)
- `akkara/format-api` – block/record view interfaces (AKHdr32 header, RecordView, Packer/Unpacker)
- `akkara/format-akk` – AKK v3 packer/unpacker, stripe writer/reader, parity coders
- `akkara/engine` – v3 storage engine (WAL, MemTable, SSTable, manifest, stripes) + AkkaraDSL
- `akkara/test` – property tests and basic unit tests

### Core Invariants (v3)

- Global sequence (u64) monotonically increases across the whole DB
- Replacement rule: higher seq wins; if equal, tombstone wins (no resurrection)
- Keys are ordered by byte-wise lexicographic order
- Durability boundary: a write is acknowledged when WAL is durable
- Crash recovery: last durable ≤ last sealed (manifest/WAL rules)

## API Reference

### Engine API (low-level)

- `put(key: ByteBufferL, value: ByteBufferL): Long`
- `delete(key: ByteBufferL): Long`
- `get(key: ByteBufferL): ByteBufferL?`
- `compareAndSwap(key: ByteBufferL, expectedSeq: Long, newValue: ByteBufferL?): Boolean`
- `iterator(range: MemTable.KeyRange = ALL): Sequence<MemRecord>`
- `flush()`, `close()`

Keys and values are treated as opaque byte sequences. Callers manage their own serialization (or use the DSL).

### Data Units

- In-memory: `MemRecord { key, value, seq, flags, keyHash, approxSizeBytes }`
- On-disk view: `RecordView` – zero-copy slices over packed block payloads
- Header (AKHdr32, LE, fixed 32 bytes):
  - `kLen:u16, vLen:u32, seq:u64, flags:u8, pad0:u8, keyFP64:u64, miniKey:u64`
  - `keyFP64` is a 64-bit fingerprint (SipHash-2-4), `miniKey` is the first 8 bytes of the key (LE)

### Block and Stripe Format

- Block (32 KiB):
  - `[0..3]` payloadLen (u32, LE)
  - `[4..N)` payload = repeated { AKHdr32 (32 B) + key + value }
  - `[N..-5]` zero padding
  - `[-4..-1]` crc32c over `[0 .. BLOCK_SIZE-4)`
- Stripe (atomic I/O group): k data lanes + m parity lanes, all lanes write block i at the same offset
- Parity options:
  - m=0 none, m=1 XOR, m=2 DualXor, m≥3 Reed–Solomon

### WAL, Manifest, Recovery

- WAL: append-only, group-commit by N or T (µs); a write is ACKed when WAL is durable
- Manifest: append-only events (StripeCommit, SSTSeal, Checkpoint, …)
- Recovery procedure:
  1. Read Manifest to locate last consistent boundaries
  2. Replay WAL up to durable tail
  3. Validate stripes lazily and load SSTables

### Defaults (tunable)

- k=4, m=2; blockSize=32 KiB
- wal.groupCommit: N=32 or T=500 µs
- stripe.flush: N=32 or T=500 µs (fastMode optional)
- mem.flushThreshold: 64 MiB or 50k entries
- bloom false positive rate ≈ 1%
- tombstone TTL = 24h (GC during compaction)

## Benchmarks

All tests were conducted on AkkaraDB v3 (pre-alpha) using the in-memory MemTable + WAL engine.
Hardware: NVMe SSD, Intel i5-12500H, JDK 21, Linux (Lubuntu).

### Write Performance (WAL Group Tuning)

| # | WalGroupN | WalGroupMicros |    ops/sec | p50 (µs) | p90 (µs) |   p99 (µs) | Notes                                 |
|--:|:---------:|:--------------:|-----------:|---------:|---------:|-----------:|:--------------------------------------|
| ① |    64     |     1 000      |      4 069 |      6.3 |     17.9 | **15 088** | fsync too frequent (fully sync-bound) |
| ② |    128    | 1 000 – 5 000  |      7 757 |      3.9 |     15.9 |        223 | clearly faster, fsync less dominant   |
| ③ |    128    |    ≥ 10 000    |      8 120 |      4.9 |     17.2 |        169 | time-driven batching stabilizes       |
| ④ |    256    |     1 000      |     14 457 |      4.4 |     22.1 |        144 | batch effect significant              |
| ⑤ |    256    | 5 000 – 10 000 |     16 108 |      5.9 |     20.1 |         72 | balanced, I/O efficient               |
| ⑥ |    512    | 1 000 – 10 000 |     28 131 |      6.7 |     23.8 |         77 | high-throughput mode                  |
| ⑦ |    512    |     50 000     | **30 529** |  **4.8** | **13.8** |   **57.9** | optimal point — p99 ≪ 200 µs          |

**Summary**

- Throughput scales ~linearly with WalGroupN.
- WalGroupMicros below 5 000 µs limits performance due to excessive fsync.
- Optimal configuration: **WalGroupN = 512, WalGroupMicros = 50 000**
  → *Throughput ≈ 30 k ops/s*, *p99 ≈ 58 µs*, *durability window = 50 ms*.

### Read Performance

| Path      | bench    |       N | valueSize |       ops/sec | p50 (µs) | p90 (µs) | p99 (µs) | Notes                     |
|:----------|:---------|--------:|----------:|--------------:|---------:|---------:|---------:|:--------------------------|
| In-Memory | read     | 100 000 |      64 B |   **362 152** |  **1.0** |  **2.0** | **12.1** | MemTable hit (no I/O)     |
| SST (hot) | read-sst | 100 000 |      64 B | **70 594.85** | **11.8** | **17.8** | **34.0** | SST read, block-cache hot |

**Summary**

- In-memory lookups are fully CPU-bound and reach very high throughput with minimal latency.
- SST reads show stable, low-latency performance with the block cache warmed.
- Both paths exhibit consistent behavior across large key counts.

### Overall Evaluation

| Metric               | Target       | Achieved                                            |
|:---------------------|:-------------|:----------------------------------------------------|
| Write durability p99 | ≤ 200 µs     | **≤ 60 µs**                                         |
| Read latency p99     | ≤ 20 µs      | **≈ 12 µs**                                         |
| Sustained throughput | ≥ 10 k ops/s | **≈ 30 k ops/s (write)** / **≈ 360 k ops/s (read)** |
| Crash safety         | WAL + fsync  | ✅ verified                                          |

AkkaraDB v3's write path is now *production-grade*:
fully asynchronous, crash-safe, and performant under realistic durability windows.

## Build from Source

Requirements:
- JDK 17+
- Kotlin 2.1+
- Gradle

```bash
./gradlew build
```

## License

GNU Lesser General Public License v3.0 (LGPL-3.0)

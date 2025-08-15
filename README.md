# AkkaraDB

AkkaraDB is a JVM-native, ultra-low-latency embedded KV store written in Kotlin.
Design goals: predictable P99, minimal dependencies, crash-safe on a single node, optional redundancy via striped parity.

## Why

* **Latency first**: P99 reads 10–50 µs (≈1M keys, page-cache hits), writes ≤200 µs (WAL + group commit).
* **Simple & robust**: append-only data path, fsync batching, manifest-based recovery.
* **Zero external deps**: pure JDK + Kotlin.

## Features (current)

* **Write path**: `Record → TLV encode → WAL (group commit) → MemTable → Stripe fan-out (32 KiB blocks × k lanes, optional parity m)`.
* **Read path**: `MemTable → SSTable (Bloom+Index) → Stripe cache/scan fallback`.
* **SSTable**: fixed-format blocks (`[len][payload][crc]`), mini-index per block, bloom & outer index always-resident.
* **Compaction**: L0 → L1, tombstone TTL (planned), newest-seq wins per key.
* **Manifest**: append-only events (`StripeCommit`, `SSTSeal`, `Checkpoint`) for recovery and bookkeeping.
* **Parity (optional)**: m=1 XOR, m=2 DualXor, m≥3 Reed–Solomon.

> Status: pre-alpha. APIs and file formats may change.

## Performance targets

* Read P99: 10–50 µs
* Write P99: ≤200 µs (WAL + fsync batching)
* Consistency: `put` ACKs when **WAL durable** (memtable updated in the same tx).

## On-disk layout

* **Record TLV (fixed header)**: `[kLen:u16][vLen:u32][seq:u64 LE][flags:u8][key][value]`
* **Block (32 KiB)**: `[4B payloadLen][payload...][padding][4B crc32(payload)]`
* **Stripe**: `k` data lanes + `m` parity lanes, blocks aligned per offset.
* **SSTable**:

    * Data blocks (32 KiB, keys asc)
    * Mini-index per block: `[count:u16][offset:u32]×count (LE)`
    * Outer index (fixed key-prefix + i64 offset, LE)
    * Bloom filter (resident)
    * Footer: `{ magic='AKSS', indexOff, bloomOff }`

## Defaults

```
k = 4, m = 2
blockSize = 32 KiB
memFlushThreshold = 64 MiB or 50k entries
bloomFalsePositive ≈ 1%
compaction.L0.limit = 4
stripe.flush.batch = N=32 or T=500µs
wal.groupCommit    = N=32 or T=500µs
tombstoneTTL = 24h (planned)
```

## Quick start

```kotlin
import dev.swiftstorm.akkaradb.engine.AkkaraDB
import dev.swiftstorm.akkaradb.common.Record
import java.nio.file.Paths

fun main() {
    val base = Paths.get("data")
    val db = AkkaraDB.open(base, k = 4, m = 2)

    // seq is monotonic u64 (app/user supplied or db.nextSeq() if you provide it)
    db.put(Record("hello", "world", seqNo = 1))

    val v = db.get(java.nio.charset.StandardCharsets.UTF_8.encode("hello"))
    println("value = " + (v?.let { java.nio.charset.StandardCharsets.UTF_8.decode(it) }))

    db.flush()
    db.close()
}
```

## Recovery & manifest

* `StripeCommit(after=N)`: written after each group fsync; on startup we `seek/truncate` to this stripe.
* `SSTSeal(level, file, entries, first/last)`: written after SST build (L0) and compaction (L1+).
* `Checkpoint(name, ts[, stripe, lastSeq])`: optional markers after flush/compaction/WAL seal to shorten WAL replay.

## Build

* JDK 17+
* Kotlin 2.1.0+
* Gradle (KTS)

```
./gradlew build
```

## Roadmap

1. Range scan & secondary index
2. Metrics (P50/P90/P99/P999)
3. Async RS offload (GPU)
4. RAFT replication

## License

ARR (temporary, will be changed to another OSS license later)

---

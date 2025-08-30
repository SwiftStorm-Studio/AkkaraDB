なるほど、README の「Performance targets」とその上の説明部分を消して、代わりに**実測値のテーブルを載せられるように整形したい**ってことだね 👍
こうすると、今後ベンチマークの数字をすぐ差し替えられる形になる。

書き換え例を出すよ👇

---

```markdown
# AkkaraDB

AkkaraDB is a JVM-native, ultra-low-latency embedded KV store written in Kotlin.  
Design goals: predictable tail latency, minimal dependencies, crash-safe on a single node, optional redundancy via striped parity.

## Why

* **Robust storage engine** – Append-only writes, batched fsync, manifest-based recovery.
* **Minimal external deps** – Core runs on pure JDK + Kotlin; only optional logging (SLF4J) and the built-in BinPack module.

## Features (current)

* **Write path**: `Record → TLV encode → WAL (group commit) → MemTable → Stripe fan-out (32 KiB blocks × k lanes, optional parity m)`.
* **Read path**: `MemTable → SSTable (Bloom+Index) → Stripe cache/scan fallback`.
* **SSTable**: fixed-format blocks (`[len][payload][crc]`), mini-index per block, bloom & outer index always-resident.
* **Compaction**: L0 → L1, tombstone TTL (planned), newest-seq wins per key.
* **Manifest**: append-only events (`StripeCommit`, `SSTSeal`, `Checkpoint`) for recovery and bookkeeping.
* **Parity (optional)** – Reed–Solomon (RS) coding for any number of parity lanes (m ≥ 1), supporting recovery from up to m simultaneous lane losses.

> Status: pre-alpha. APIs and file formats may change.

## Benchmark (preliminary)

| Op   | n     | Avg (µs) | P50 (µs) | P90 (µs) | P99 (µs) | P99.9 (µs) | P100 (µs) |
|------|-------|----------|----------|----------|----------|------------|-----------|
| ALL  | 10000 | 9.386    | 6.3      | 17.7     | 46.8     | 161.5      | 2004.3    |
| GET  | 4103  | 5.958    | 4.3      | 10.0     | 29.5     | 121.8      | 627.8     |
| PUT  | 5897  | 11.771   | 8.1      | 22.0     | 53.6     | 255.7      | 2004.3    |

*(fast-mode, pre-alpha implementation; numbers subject to change)*

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
wal.groupCommit = N=128 or T=5000µs
tombstoneTTL = 24h (planned?)

````

## Quick start

```kotlin
import dev.swiftstorm.akkaradb.engine.AkkaraDB
import dev.swiftstorm.akkaradb.common.Record
import java.nio.file.Paths

fun main() {
    val base = Paths.get("data")
    val db = AkkaraDB.open(base, k = 4, m = 2)

    db.put(Record("hello", "world", seqNo = 1))

    val v = db.get(java.nio.charset.StandardCharsets.UTF_8.encode("hello"))
    println("value = " + (v?.let { java.nio.charset.StandardCharsets.UTF_8.decode(it) }))

    db.flush()
    db.close()
}
````

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

## License

ARR (temporary, will be changed to another OSS license later)
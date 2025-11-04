# AkkaraDB

AkkaraDB is a JVM-native, ultra-low-latency embedded key–value store written in Kotlin.

- Predictable tail latency and simple operational model
- Crash-safe on a single node; optional redundancy via striped parity (k data + m parity lanes)
- Zero external runtime dependencies (JDK + Kotlin only)

Status: pre-alpha. Public APIs and on-disk formats are still evolving in v3.

## What’s inside

- Modules
  - akkara/common – shared primitives (ByteBufferL, hashing, small buffer pools)
  - akkara/format-api – block/record view interfaces (AKHdr32 header, RecordView, Packer/Unpacker)
  - akkara/format-akk – AKK v3 packer/unpacker, stripe writer/reader, parity coders
  - akkara/engine – v3 storage engine (WAL, MemTable, SSTable, manifest, stripes) + AkkaraDSL
  - akkara/test – property tests and basic unit tests

## Core invariants (v3)

- Global sequence (u64) monotonically increases across the whole DB
- Replacement rule: higher seq wins; if equal, tombstone wins (no resurrection)
- Keys are ordered by byte-wise lexicographic order
- Durability boundary: a write is acknowledged when WAL is durable
- Crash recovery: last durable ≤ last sealed (manifest/WAL rules)

## Engine API (low-level)

- put(key: ByteBufferL, value: ByteBufferL): Long
- delete(key: ByteBufferL): Long
- get(key: ByteBufferL): ByteBufferL?
- compareAndSwap(key: ByteBufferL, expectedSeq: Long, newValue: ByteBufferL?): Boolean
- iterator(range: MemTable.KeyRange = ALL): Sequence<MemRecord>
- flush(), close()

Keys and values are treated as opaque byte sequences. Callers manage their own serialization (or use the DSL below).

## Data units

- In-memory: MemRecord { key, value, seq, flags, keyHash, approxSizeBytes }
- On-disk view: RecordView – zero-copy slices over packed block payloads
- Header (AKHdr32, LE, fixed 32 bytes):
  - kLen:u16, vLen:u32, seq:u64, flags:u8, pad0:u8, keyFP64:u64, miniKey:u64
  - keyFP64 is a 64-bit fingerprint (SipHash-2-4), miniKey is the first 8 bytes of the key (LE)

## Block and stripe format

- Block (32 KiB):
  - [0..3] payloadLen (u32, LE)
  - [4..N) payload = repeated { AKHdr32 (32 B) + key + value }
  - [N..-5] zero padding
  - [-4..-1] crc32c over [0 .. BLOCK_SIZE-4)
- Stripe (atomic I/O group): k data lanes + m parity lanes, all lanes write block i at the same offset
- Parity options:
  - m=0 none, m=1 XOR, m=2 DualXor, m≥3 Reed–Solomon

## WAL, manifest, recovery

- WAL: append-only, group-commit by N or T (µs); a write is ACKed when WAL is durable
- Manifest: append-only events (StripeCommit, SSTSeal, Checkpoint, …)
- Recovery procedure:
  1) Read Manifest to locate last consistent boundaries
  2) Replay WAL up to durable tail
  3) Validate stripes lazily and load SSTables

## Defaults (can be tuned)

- k=4, m=2; blockSize=32 KiB
- wal.groupCommit: N=32 or T=500 µs
- stripe.flush: N=32 or T=500 µs (fastMode optional)
- mem.flushThreshold: 64 MiB or 50k entries
- bloom false positive rate ≈ 1%
- tombstone TTL = 24h (GC during compaction)

## Quick start

### A) Typed API with AkkaraDSL

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

### B) Low-level API

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

## Build

- JDK 17+
- Kotlin 2.1+
- Gradle (KTS)

Run: ./gradlew build

## License

GNU Lesser General Public License v3.0 (LGPL-3.0)
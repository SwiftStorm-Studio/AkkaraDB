# Architecture

Detailed description of AkkaraDB's internal design and each component.

## 📚 Table of Contents

- [Overall Structure](#overall-structure)
- [Core Components](#core-components)
    - [MemTable](#memtable)
    - [WAL (Write-Ahead Log)](#wal-write-ahead-log)
    - [SSTable](#sstable)
    - [Stripe](#stripe)
    - [Manifest](#manifest)
- [Data Flow](#data-flow)
- [Block Format](#block-format)
- [Recovery Mechanism](#recovery-mechanism)
- [Compaction](#compaction)

---

## Overall Structure

AkkaraDB consists of 5 main components:

```
┌─────────────────────────────────────────────────────┐
│                   AkkaraDB Engine                   │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌──────────┐  ┌─────┐  ┌─────────┐  ┌─────────┐    │
│  │ MemTable │→ │ WAL │→ │ Stripe  │→ │Manifest │    │
│  └────┬─────┘  └─────┘  └─────────┘  └─────────┘    │
│       │ flush                                       │
│       ↓                                             │
│  ┌─────────────────────┐                            │
│  │      SSTable        │                            │
│  │  ┌────┬────┬────┐   │                            │
│  │  │ L0 │ L1 │ L2 │   │                            │
│  │  └────┴────┴────┘   │                            │
│  └─────────────────────┘                            │
│          │compaction│                               │
│          └──────────┘                               │
└─────────────────────────────────────────────────────┘
```

### Directory Structure

```
baseDir/
├── wal.akwal           # Write-Ahead Log
├── manifest.akmf       # Manifest file
├── sst/                # SSTable directory
│   ├── L0/            # Level 0 (newest)
│   ├── L1/            # Level 1
│   └── L2/            # Level 2 ...
└── lanes/              # Stripe lane directory
    ├── data_0.akd     # Data lane 0
    ├── data_1.akd     # Data lane 1
    ├── data_2.akd     # Data lane 2
    ├── data_3.akd     # Data lane 3
    ├── parity_0.akp   # Parity lane 0
    └── parity_1.akp   # Parity lane 1
```

---

## Core Components

### MemTable

In-memory write buffer. Enables fast read/write operations.

#### Design

- **Sharded TreeMap**: Divided into multiple shards to reduce contention
- **Multi-version**: Maintains multiple versions of the same key
- **Sequence Number**: Globally monotonically increasing (u64)

#### Internal Structure

```kotlin
class MemTable(
    private val shardCount: Int = Runtime
        .getRuntime()
        .availableProcessors()
        .coerceAtLeast(2)
        .coerceAtMost(8),
    private val thresholdBytesPerShard: Long = (64L * 1024 * 1024) / shardCount,
    private val onFlush: (List<MemRecord>) -> Unit
) {
    private val shards: Array<Shard>
    private val flusher: Flusher
    private val seqGen = AtomicLong(0)
}
```

Each shard manages:

- `active`: Currently writable TreeMap
- `immutables`: List of immutable TreeMaps awaiting flush
- `bytes`: Current memory usage

#### Flush Mechanism

1. **Trigger Conditions**:

- Shard byte count exceeds `thresholdBytesPerShard`
- Explicit `flushHint()` call

2. **Flush Process**:
   ```kotlin
   // 1. Seal active and add to immutables
   sealed = active
   immutables.add(sealed)
   active = TreeMap()
   
   // 2. Queue to background flusher
   queue.offer(batch)
   
   // 3. Write as sorted list of MemRecords
   val sorted = sealed.values.toList()
   onFlush(sorted)
   ```

3. **Write Destinations**:

- **SSTable**: Persisted to disk at L0 level
- **Stripe**: Saved with parity for redundancy (optional)

---

### WAL (Write-Ahead Log)

Write-ahead log that guarantees data recovery on crash.

#### Format

Each entry has the following structure:

```
[u32 length] [payload bytes] [u32 crc32c]
```

**Payload Types**:

- `WalOp.Add`: Key, value, sequence, flags, keyFP64, miniKey
- `WalOp.Delete`: Key, sequence, tombstone flag, keyFP64, miniKey

#### Group Commit

For high throughput, multiple writes are fsynced together.

```kotlin
class WalWriter(
    private val groupN: Int = 64,
    private val groupTmicros: Long = 1_000,
    private val fastMode: Boolean = true
) {
    // Fsync on N entries or T µs elapsed
}
```

**Fast Mode**:

- `force(false)`: fdatasync equivalent (no metadata sync)
- Batch processing in background thread

**Durable Mode**:

- `force(true)`: fsync equivalent (full sync)
- Immediate sync on each write

#### Recovery

On startup, replay WAL to rebuild MemTable:

```kotlin
fun replay(walPath: Path, mem: MemTable): ReplayResult {
    // 1. Read WAL entries sequentially
    // 2. Apply each WalOp to MemTable
    // 3. Restore sequence numbers
}
```

---

### SSTable

Immutable ordered table on disk.

#### File Layout

```
┌─────────────────────────────────┐
│      Data Blocks (32 KiB each)  │
│  ┌──────────────────────────┐   │
│  │ Block 0                  │   │
│  │  [u32 payloadLen]        │   │
│  │  [records...]            │   │
│  │  [padding]               │   │
│  │  [u32 crc32c]            │   │
│  ├──────────────────────────┤   │
│  │ Block 1                  │   │
│  │  ...                     │   │
│  └──────────────────────────┘   │
├─────────────────────────────────┤
│      Index Block ('AKIX')       │
│  [u32 magic='AKIX']             │
│  [u32 entries]                  │
│  [array of (firstKey32, offset)]│
│  [u32 crc32c]                   │
├─────────────────────────────────┤
│    Bloom Filter ('AKBL')        │
│  [u32 magic='AKBL']             │
│  [u32 bitsSize]                 │
│  [u32 hashCount]                │
│  [bits array]                   │
│  [u32 crc32c]                   │
├─────────────────────────────────┤
│       Footer ('AKSS', 32B)      │
│  [u32 magic='AKSS']             │
│  [u8 ver=1] [u24 pad]           │
│  [u64 indexOff]                 │
│  [u64 bloomOff]                 │
│  [u32 entries]                  │
│  [u32 crc32c]                   │
└─────────────────────────────────┘
```

#### AKHdr32 Header (32 bytes)

Each record begins with a 32-byte fixed header:

```
Offset  Size  Field       Description
──────────────────────────────────────────────────
0       2     kLen        Key length (u16)
2       4     vLen        Value length (u32)
6       8     seq         Sequence number (u64)
14      1     flags       Flags (u8, 0x01=TOMBSTONE)
15      1     pad0        Padding (=0)
16      8     keyFP64     SipHash-2-4(key) (u64)
24      8     miniKey     LE representation of first 8 bytes of key (u64)
```

**miniKey**: First up to 8 bytes of key packed as little-endian, padded with zeros. Used for range query optimization.

**keyFP64**: Key fingerprint via SipHash-2-4. Used for Bloom Filter seed and index fast pre-check.

#### Index Block

Stores first key and offset of each data block:

```kotlin
data class IndexEntry(
    val firstKey32: ByteArray,  // First key in block (32 bytes)
    val blockOff: Long          // Block offset in file
)
```

Used for fast block identification during range queries.

#### Bloom Filter

Probabilistically determines key existence:

```kotlin
class BloomFilter(
    expectedInsertions: Long,
    fpRate: Double = 0.01  // False positive rate
) {
    private val bits: BitSet
    private val hashCount: Int
}
```

- **False Positive**: Yes (may say non-existent key exists)
- **False Negative**: No (never says existent key doesn't exist)

---

### Stripe

RAID-like striping mechanism for redundancy.

#### Structure

```
Stripe = k data blocks + m parity blocks

Example: k=4, m=2
┌────────┬────────┬────────┬────────┬──────────┬──────────┐
│ data_0 │ data_1 │ data_2 │ data_3 │ parity_0 │ parity_1 │
└────────┴────────┴────────┴────────┴──────────┴──────────┘
    ↓        ↓        ↓        ↓          ↓          ↓
  Lane 0   Lane 1   Lane 2   Lane 3    Lane 4    Lane 5
```

#### Parity Encoding

**Reed-Solomon Code** (m ≥ 1):

```kotlin
class RSParityCoder(private val m: Int) : ParityCoder {
    // Generate m parity blocks from k data blocks
    // Can recover from up to m lane failures
}
```

#### Group Commit

Stripe also uses group commit:

```kotlin
data class FlushPolicy(
    val maxBlocks: Int = 32,      // Block count threshold
    val maxMicros: Long = 1000    // Time threshold (µs)
)
```

**Seal Process**:

1. k data blocks complete
2. Compute m parity blocks
3. Write to all lanes
4. Fsync according to group commit policy

#### Recovery

On startup, validate lane consistency:

```kotlin
fun recover(): RecoveryResult {
    // 1. Scan tail of each lane
    // 2. CRC validation
    // 3. Truncate incomplete stripes
    // 4. Recover from parity (if needed)
}
```

---

### Manifest

Metadata log recording database state.

#### Event Types

```kotlin
sealed class ManifestEvent {
    data class StripeCommit(val after: Long)
    data class SstSeal(val level: Int, val file: String, ...)
    data class Checkpoint(val name: String?, val stripe: Long?, val lastSeq: Long?)
    data class CompactionStart(val level: Int, val inputs: List<String>)
    data class CompactionEnd(val level: Int, val output: String, ...)
    data class SSTDelete(val file: String)
    data class Truncate(val note: String?)
    data class FormatBump(val major: Int, val minor: Int)
}
```

#### Fast Mode

Batch write in background thread:

```kotlin
private fun runFlusher() {
    while (running.get() || q.isNotEmpty()) {
        // Collect batch (N events or T µs)
        val batch = collectBatch()

        // Write at once
        for (event in batch) {
            writeOne(ch, event)
        }
        ch.force(false)  // fdatasync

        // Periodically sync metadata with force(true)
        if (shouldStrongSync()) {
            ch.force(true)
        }
    }
}
```

#### Rotation

Switch to new file when exceeds size threshold:

```
manifest.akmf
manifest.20250112-153045
manifest.20250112-160132
...
```

---

## Data Flow

### Write Flow

```
┌─────────┐
│  put()  │
└────┬────┘
     │
     ↓
┌─────────────────────┐
│ 1. mem.nextSeq()    │  Generate sequence number
└────┬────────────────┘
     │
     ↓
┌─────────────────────┐
│ 2. wal.append()     │  Write to WAL (durable before apply)
└────┬────────────────┘
     │
     ↓
┌─────────────────────┐
│ 3. mem.put()        │  Add to MemTable
└────┬────────────────┘
     │
     ↓ (when threshold reached)
┌─────────────────────┐
│ 4. mem.flush()      │
└────┬────────────────┘
     │
     ├──→ SSTableWriter → L0/xxx.sst
     │
     └──→ StripeWriter → lanes/data_*, lanes/parity_*
          │
          ↓
     ┌──────────────────┐
     │ manifest.sstSeal │  Record metadata
     └──────────────────┘
```

### Read Flow

```
┌─────────┐
│  get()  │
└────┬────┘
     │
     ↓
┌──────────────────────┐
│ 1. mem.get()         │  Search MemTable (fast path)
└────┬─────────────────┘
     │ Found?
     ├─ Yes → Return
     │
     ↓ No
┌──────────────────────┐
│ 2. SSTable search    │  Search newest to oldest
│    L0 → L1 → L2 ...  │
└────┬─────────────────┘
     │ Found?
     ├─ Yes → Return
     │
     ↓ No (if useStripeForRead=true)
┌──────────────────────┐
│ 3. Stripe search     │  Fallback
└────┬─────────────────┘
     │
     ↓
   Return
```

### Range Query Flow

```
┌────────────┐
│  range()   │
└─────┬──────┘
      │
      ↓
┌──────────────────────────────┐
│ 1. Create iterators           │
│    - MemTable iterator        │
│    - Each SSTable iterator    │
└─────┬────────────────────────┘
      │
      ↓
┌──────────────────────────────┐
│ 2. K-way merge                │
│    - Sort by key order        │
│    - Prioritize newest (max seq)
│    - Skip tombstones          │
└─────┬────────────────────────┘
      │
      ↓
   Sequence<MemRecord>
```

---

## Block Format

### Data Block (32 KiB)

```
Offset      Content
─────────────────────────────────────
0..3        payloadLen (u32 LE)
4..4+N      payload = repeat {
              AKHdr32 (32B)
              + key bytes
              + value bytes
            }
4+N..-5     zero padding
-4..-1      CRC32C
```

### Record Packing

```kotlin
class AkkBlockPacker {
    fun tryAppend(
        key: ByteBufferL,
        value: ByteBufferL,
        seq: U64,
        flags: Int,
        keyFP64: U64,
        miniKey: U64
    ): Boolean {
        val needed = 32 + key.remaining + value.remaining
        if (payloadPos + needed > PAYLOAD_LIMIT) {
            return false  // Does not fit in block
        }

        // 1. Write AKHdr32
        buf.putHeader32(...)

        // 2. Write key
        buf.put(key)

        // 3. Write value
        buf.put(value)

        payloadPos += needed
        return true
    }

    fun endBlock() {
        // 1. Write payloadLen
        buf.at(0).i32 = payloadPos - 4

        // 2. Zero padding
        buf.fillZero(BLOCK_SIZE - 4 - payloadPos)

        // 3. Write CRC32C
        val crc = buf.crc32cRange(0, BLOCK_SIZE - 4)
        buf.at(BLOCK_SIZE - 4).i32 = crc

        // 4. Write completed block
        onBlockReady(buf)
    }
}
```

---

## Recovery Mechanism

### Startup Recovery Sequence

```
┌──────────────────────┐
│ 1. Load Manifest     │
│    - Last Checkpoint │
│    - Live SST list   │
└─────┬────────────────┘
      │
      ↓
┌──────────────────────┐
│ 2. Replay WAL        │
│    - Rebuild MemTable│
└─────┬────────────────┘
      │
      ↓
┌──────────────────────┐
│ 3. Recover Stripe    │
│    - CRC validation  │
│    - Parity recovery │
└─────┬────────────────┘
      │
      ↓
┌──────────────────────┐
│ 4. Open SSTables     │
│    - Load index      │
│    - Load Bloom      │
└──────────────────────┘
```

### Crash Scenarios

#### Scenario 1: MemTable Flush In Progress

```
State: MemTable → SST write in progress → Crash
Recovery:
1. Write remains in WAL
2. Rebuild MemTable via WAL replay
3. Ignore partial SST (not recorded in Manifest)
```

#### Scenario 2: Stripe Write In Progress

```
State: Data lanes written, parity lane write → Crash
Recovery:
1. Stripe.recover() detects incomplete stripe
2. Return truncatedTail=true
3. Truncate incomplete stripe
4. Recover from parity (if possible)
```

#### Scenario 3: Compaction In Progress

```
State: New SST creation → Crash
Recovery:
1. No CompactionEnd event in Manifest
2. Input SSTables still recorded as "live"
3. Ignore partial output SST
4. Rerun compaction on next startup
```

---

## Compaction

### Leveled Compaction

```
L0: New SSTables (unsorted, overlapping)
L1: Sorted, non-overlapping
L2: Larger sorted files
...
```

#### Trigger Conditions

Runs when file count exceeds threshold at each level:

```kotlin
class SSTCompactor(
    private val maxPerLevel: Int = 10
) {
    fun compact() {
        while (true) {
            val levelToCompact = levels.firstOrNull { level ->
                listSstFiles(levelPath(level)).size > maxPerLevel
            } ?: break

            compactLevel(levelToCompact)
        }
    }
}
```

#### Merge Process

```kotlin
private fun compactInto(
    inputs: List<Path>,
    output: Path,
    isBottomLevel: Boolean
): Triple<Long, String?, String?> {
    // 1. Create iterators from all input SSTables
    val iterators = inputs.map { SSTIterator(it) }

    // 2. K-way merge
    val merged = merge(
        iterators = iterators,
        nowMillis = System.currentTimeMillis(),
        isBottomLevelCompaction = isBottomLevel,
        ttlMillis = 24L * 60 * 60 * 1000
    )

    // 3. Write to new SSTable
    SSTableWriter(output).use { writer ->
        writer.writeAll(merged)
        writer.seal()
    }

    // 4. Delete input SSTables
    inputs.forEach { Files.deleteIfExists(it) }
}
```

### Tombstone GC

Deleted keys are recorded as tombstones. Remove old tombstones during compaction:

```kotlin
private fun shouldDropTombstone(
    tomb: SSTRecord,
    nowMillis: Long,
    ttlMillis: Long,
    isBottomLevelCompaction: Boolean
): Boolean {
    // 1. Get deletion time (timestamp or estimate from seq)
    val deleteAt = tomb.deleteAtMillisOrNull()

    // 2. Check TTL elapsed
    val expired = (deleteAt != null) && (nowMillis - deleteAt >= ttlMillis)

    // 3. Can only drop at bottom level
    val canDropHere = isBottomLevelCompaction

    return expired && canDropHere
}
```

**Important**: Deleting tombstones at intermediate levels causes old versions remaining at lower levels to resurface, so deletion only happens at the bottom
level.

---

Next: [Benchmarks](./BENCHMARKS.md) | [API Reference](./API_REFERENCE.md)

[Back to Overview](./ABOUT.md)

---
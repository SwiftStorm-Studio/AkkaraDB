# AkkaraDB v1 - v3 Specification

**Version:** 0.4.0
**Status:** Production Candidate  
**Date:** 2025-12-17
**Authors:** Swift Storm Studio

---

## Table of Contents

- [1. Introduction](#1-introduction)
- [2. Core Invariants](#2-core-invariants)
- [3. Data Formats](#3-data-formats)
    - [3.1 AKHdr32 (32-Byte Record Header)](#31-akhdr32-32-byte-record-header)
    - [3.2 Block Format (32 KiB)](#32-block-format-32-kib)
    - [3.3 WAL Format](#33-wal-format)
    - [3.4 SSTable Format (AKSS)](#34-sstable-format-akss)
    - [3.5 Stripe Format](#35-stripe-format)
    - [3.6 Manifest Format](#36-manifest-format)
- [4. Algorithms](#4-algorithms)
    - [4.1 Key Ordering](#41-key-ordering)
    - [4.2 Replacement Rule](#42-replacement-rule)
    - [4.3 SipHash-2-4](#43-siphash-2-4)
    - [4.4 Parity Coding](#44-parity-coding)
- [5. Recovery Protocol](#5-recovery-protocol)
- [6. Versioning and Compatibility](#6-versioning-and-compatibility)
- [7. Error Codes](#7-error-codes)
- [8. References](#8-references)

---

## 1. Introduction

AkkaraDB v3 is a crash-safe, append-only key-value storage engine optimized for predictable tail latency on modern NVMe SSDs. This specification defines the
on-disk formats, invariants, and algorithms necessary for implementing a v3-compatible engine or tooling.

**Design Goals:**

- P99 write latency ≤ 200 µs (with WAL group commit)
- P99 read latency ≤ 50 µs (MemTable hit)
- Crash-safe single-node durability
- Optional k+m parity-based redundancy (Stripe)
- Zero external runtime dependencies (JDK + Kotlin only)

---

## 2. Core Invariants

The following invariants MUST be maintained by all v3 implementations:

### INV-1: Global Sequence Monotonicity

Every write operation is assigned a globally unique, monotonically increasing 64-bit unsigned sequence number (`seq: u64`). Once assigned, a sequence number
MUST NOT be reused.

### INV-2: Replacement Rule

For a given key, when multiple records exist with different `seq` values:

1. The record with the **highest `seq`** wins.
2. If two records have equal `seq`, the **tombstone** (flags & 0x01) wins.
3. A tombstone MUST NOT be replaced by a non-tombstone with the same `seq` (no resurrection).

### INV-3: Key Ordering

Keys are ordered by **bytewise lexicographic comparison**. All on-disk structures (SSTable, Index) MUST respect this ordering.

### INV-4: Durability Boundary

A write is acknowledged to the client **if and only if** the WAL entry has been successfully `fsync`'d to durable storage.

### INV-5: Recovery Consistency

After crash recovery:

```
last_durable_WAL_seq ≤ last_sealed_manifest_seq
```

The Manifest MUST NOT reference state beyond what the WAL can reconstruct.

---

## 3. Data Formats

All multi-byte integers are encoded in **Little-Endian (LE)** byte order unless explicitly stated otherwise.

### 3.1 AKHdr32 (32-Byte Record Header)

Every record (on-disk and in WAL) is prefixed by a fixed 32-byte header:

```
Offset  Size  Field       Type   Description
------  ----  ----------  -----  -----------
0       2     kLen        u16    Key length in bytes (0..65535)
2       4     vLen        u32    Value length in bytes
6       8     seq         u64    Global sequence number
14      1     flags       u8     Bit flags (see below)
15      1     pad0        u8     Reserved (MUST be 0)
16      8     keyFP64     u64    SipHash-2-4 fingerprint of key
24      8     miniKey     u64    First ≤8 bytes of key (LE-packed)
------  ----
Total: 32 bytes
```

**Flags (u8):**

```
Bit 0 (0x01): TOMBSTONE - Record is a deletion marker
Bit 1-7:      Reserved (MUST be 0)
```

**keyFP64 Calculation:**

```
keyFP64 = SipHash-2-4(key, seed=DEFAULT_SIPHASH_SEED)
DEFAULT_SIPHASH_SEED = 0x5AD6DCD676D23C25 (u64)
```

**miniKey Construction:**
Pack the first min(8, kLen) bytes of the key into a u64, little-endian:

```
miniKey[7:0]   = key[0]
miniKey[15:8]  = key[1]
...
miniKey[63:56] = key[7]
```

Unused high bytes MUST be zero.

---

### 3.2 Block Format (32 KiB)

A block is a fixed-size (32,768 bytes) container for multiple records.

```
Offset        Size        Content
------------  ----------  -------
[0..3]        4           payloadLen (u32 LE) — length of payload region
[4..4+N)      N           payload — repeated { AKHdr32 + key + value }
[4+N..-5]     32764-N-4   zero padding
[-4..-1]      4           crc32c (u32 LE) — CRC32C over bytes [0..32764)
```

**Constants:**

```
BLOCK_SIZE     = 32768  (32 KiB)
PAYLOAD_LIMIT  = 32764  (BLOCK_SIZE - 4)
```

**CRC32C:**

- Algorithm: CRC-32C (Castagnoli polynomial: 0x1EDC6F41)
- Range: bytes [0 .. BLOCK_SIZE-4)
- Stored at: bytes [BLOCK_SIZE-4 .. BLOCK_SIZE)

**Record Packing:**
Records are packed sequentially in the payload region:

```
[AKHdr32 (32B)][key (kLen bytes)][value (vLen bytes)]
[AKHdr32 (32B)][key (kLen bytes)][value (vLen bytes)]
...
```

---

### 3.3 WAL Format

The Write-Ahead Log (WAL) is an append-only file with framed entries.

**File:** `wal.akwal`

**Entry Format:**

```
[length:u32][payload:length bytes][crc32c:u32]
```

**Fields:**

- `length` (u32 LE): Length of payload in bytes (excludes header/trailer)
- `payload`: Encoded WalOp (see below)
- `crc32c` (u32 LE): CRC32C over payload only

**WalOp Encoding:**
WAL operations reuse the AKHdr32 format:

**ADD Operation:**

```
[AKHdr32 { kLen, vLen, seq, flags=0, keyFP64, miniKey }]
[key (kLen bytes)]
[value (vLen bytes)]
```

**DELETE Operation:**

```
[AKHdr32 { kLen, vLen=0, seq, flags=0x01, keyFP64, miniKey }]
[key (kLen bytes)]
```

**Recovery Behavior:**

- Read entries sequentially until:
    - EOF reached, OR
    - Incomplete frame detected (partial `length` or `payload`), OR
    - CRC32C mismatch
- Partial tail is **silently ignored** (graceful truncation).

---

### 3.4 SSTable Format (AKSS)

SSTables are immutable, sorted files containing data blocks, an index, an optional Bloom filter, and a footer.

**File Extension:** `.sst`

**Structure:**

```
[Data Block 0: 32 KiB]
[Data Block 1: 32 KiB]
...
[Data Block N-1: 32 KiB]
[Index Block]
[Bloom Filter] (optional)
[AKSS Footer: 32 bytes]
```

**Index Block:**
Repeated entries of:

```
[blockOffset:u64][firstKey32:32 bytes]
```

- `blockOffset`: Absolute byte offset of the data block
- `firstKey32`: First key in the block, normalized to 32 bytes (zero-padded or truncated)

**Bloom Filter:**

- Bit array: `entries × 10 bits`
- Hash functions: 7
- False positive rate: ≈ 1%
- Seeded with `keyFP64` values

**AKSS Footer (32 bytes):**

```
Offset  Size  Field      Type   Description
------  ----  ---------  -----  -----------
0       4     magic      u32    0x414B5353 ('AKSS')
4       1     version    u8     1
5       3     padding    u24    0
8       8     indexOff   u64    Byte offset of Index Block
16      8     bloomOff   u64    Byte offset of Bloom (0 if none)
24      4     entries    u32    Total number of records
28      4     crc32c     u32    CRC32C over [0..fileSize-4), or 0
```

**File-Level CRC:**
If `crc32c ≠ 0`, it covers all bytes from offset 0 to `fileSize - 4`.

---

### 3.5 Stripe Format

Stripes provide optional redundancy by writing data blocks across `k` data lanes and `m` parity lanes.

**Directory:** `lanes/`

**Files:**

```
data_0, data_1, ..., data_{k-1}
parity_0, parity_1, ..., parity_{m-1}
```

**Structure:**
Each lane file is a sequence of 32 KiB blocks. Stripe `i` is located at byte offset `i × 32768` in each lane.

**Write Protocol:**

1. Accumulate `k` data blocks
2. Compute `m` parity blocks using the configured ParityCoder
3. Write all `k + m` blocks atomically at the same stripe index
4. Optionally `fsync` based on group commit policy

**Parity Schemes:**

**m=0 (No Parity):**
No parity lanes. No redundancy.

**m=1 (XOR):**

```
parity_0 = data_0 ⊕ data_1 ⊕ ... ⊕ data_{k-1}
```

Tolerates 1 lane failure.

**m=2 (DualXOR):**

```
parity_0 = data_0 ⊕ data_1 ⊕ data_2 ⊕ data_3
parity_1 = (1×data_0) ⊕ (2×data_1) ⊕ (3×data_2) ⊕ (4×data_3)
```

Tolerates 2 lane failures. Multiplication in GF(2^8).

**m≥3 (Reed-Solomon):**
Full Reed-Solomon erasure coding in GF(2^8). Tolerates `m` lane failures.

**Recovery:**

1. Read all `k + m` lanes at stripe index `i`
2. If any lanes are missing/corrupt, reconstruct using parity
3. Validate CRC32C of reconstructed blocks

---

### 3.6 Manifest Format

The Manifest is an append-only log of system events.

**File:** `manifest.akman.N` (rotates at 32 MiB)

**Entry Format:**

```
[length:u32][json_payload:length bytes][crc32c:u32]
```

**Event Types (JSON):**

**StripeCommit:**

```json
{
  "type": "StripeCommit",
  "after": 123,
  "ts": 1704067200000
}
```

**SSTSeal:**

```json
{
  "type": "SSTSeal",
  "level": 0,
  "file": "L0/sst_001.sst",
  "entries": 1000,
  "firstKeyHex": "48656c6c6f",
  "lastKeyHex": "576f726c64",
  "ts": 1704067200000
}
```

**CompactionStart:**

```json
{
  "type": "CompactionStart",
  "level": 0,
  "inputs": [
    "L0/sst_001.sst",
    "L0/sst_002.sst"
  ],
  "ts": 1704067200000
}
```

**CompactionEnd:**

```json
{
  "type": "CompactionEnd",
  "level": 1,
  "output": "L1/sst_003.sst",
  "entries": 2000,
  "firstKeyHex": "...",
  "lastKeyHex": "...",
  "ts": 1704067200000
}
```

**SSTDelete:**

```json
{
  "type": "SSTDelete",
  "file": "L0/sst_001.sst",
  "ts": 1704067200000
}
```

**Checkpoint:**

```json
{
  "type": "Checkpoint",
  "name": "memFlush",
  "stripe": 456,
  "lastSeq": 789,
  "ts": 1704067200000
}
```

**Truncate:**

```json
{
  "type": "Truncate",
  "reason": "manual",
  "ts": 1704067200000
}
```

**FormatBump:**

```json
{
  "type": "FormatBump",
  "oldVer": 2,
  "newVer": 3,
  "ts": 1704067200000
}
```

**Rotation:**
When a manifest file exceeds 32 MiB, a new file `manifest.akmf.{N+1}` is created and a `Rotate` event is written.

---

## 4. Algorithms

### 4.1 Key Ordering

Keys are compared **bytewise lexicographically**:

```
function lexCompare(a: ByteArray, b: ByteArray) -> Int:
    for i in 0 .. min(a.length, b.length) - 1:
        if a[i] != b[i]:
            return (a[i] & 0xFF) - (b[i] & 0xFF)
    return a.length - b.length
```

### 4.2 Replacement Rule

```
function shouldReplace(old: Record, new: Record) -> Boolean:
    if new.seq > old.seq:
        return true
    if new.seq < old.seq:
        return false
    // seq equal: tombstone wins
    return new.isTombstone && !old.isTombstone
```

### 4.3 SipHash-2-4

SipHash-2-4 is used to compute `keyFP64`. Implementation details:

**Constants:**

```
k0 = DEFAULT_SIPHASH_SEED.raw (u64)
k1 = k0 XOR 0x9E3779B97F4A7C15
```

**Initialization:**

```
v0 = 0x736f6d6570736575 XOR k0
v1 = 0x646f72616e646f6d XOR k1
v2 = 0x6c7967656e657261 XOR k0
v3 = 0x7465646279746573 XOR k1
```

**Compression (SipRound):**

```
SipRound(v0, v1, v2, v3):
    v0 += v1; v2 += v3
    v1 = ROTL(v1, 13); v3 = ROTL(v3, 16)
    v1 ^= v0; v3 ^= v2
    v0 = ROTL(v0, 32)
    v2 += v1; v0 += v3
    v1 = ROTL(v1, 17); v3 = ROTL(v3, 21)
    v1 ^= v2; v3 ^= v0
    v2 = ROTL(v2, 32)
```

**Processing:**

1. Process full 8-byte blocks with 2 SipRounds per block
2. Pack final 1-7 bytes into a u64 (LE, length in high byte)
3. XOR with `v3`, run 2 SipRounds, XOR with `v0`
4. Finalize: `v2 ^= 0xFF`, run 4 SipRounds
5. Return: `v0 ^ v1 ^ v2 ^ v3`

### 4.4 Parity Coding

**XOR (m=1):**

```
parity = block_0 ⊕ block_1 ⊕ ... ⊕ block_{k-1}
```

**DualXOR (m=2):**

```
parity_0 = ⊕_{i=0}^{k-1} block_i
parity_1 = ⊕_{i=0}^{k-1} (i+1) × block_i  // GF(2^8) multiplication
```

**Reed-Solomon (m≥3):**
Use systematic RS codes in GF(2^8) with generator polynomial. Implementations MUST use the same generator matrix for interoperability.

---

## 5. Recovery Protocol

### 5.1 Startup Recovery

**Step 1: Manifest Replay**

1. Read all `manifest.akman.*` files in order
2. Apply events to rebuild in-memory state:
    - `liveSst`: set of active SSTable files
    - `stripesWritten`: last sealed stripe index
    - `lastCheckpoint`: last checkpoint state

**Step 2: WAL Replay**

1. Open `wal.akwal`
2. Read framed entries sequentially (stop at partial/corrupt frame)
3. Apply operations to MemTable:
   ```
   if flags & TOMBSTONE:
       MemTable.delete(key, seq)
   else:
       MemTable.put(key, value, seq)
   ```

**Step 3: Stripe Validation**

1. Check all lane files for consistent tail lengths
2. If inconsistent:
    - Truncate all lanes to `min_length`
    - Mark as `truncatedTail = true`
3. Optionally verify CRC32C of all sealed stripes

**Step 4: SSTable Loading**

1. Scan `sst/L*/*.sst` directories
2. Open files listed in `liveSst`
3. Build in-memory readers (newest-first order)

### 5.2 Crash Scenarios

**Scenario A: Power loss during WAL write**

- Partial WAL frame ignored during replay
- Data loss limited to uncommitted writes (< group commit window)

**Scenario B: Power loss during MemTable flush**

- WAL contains all data; MemTable reconstructed on restart
- Partial SSTable file ignored (not in Manifest)

**Scenario C: Power loss during Compaction**

- Input files remain in Manifest (no CompactionEnd event)
- Compaction restarted on next startup

---

## 6. Versioning and Compatibility

### 6.1 Version Numbers

**Format Version:** Encoded in:

- AKSS Footer: `version` field
- AKHdr32: Implied by magic/structure (no version field)
- WAL: Implied by frame structure (no version field)

**Current Version:** 3

### 6.2 Forward Compatibility

v3 readers MUST reject files with `version > 3` and return `FORMAT_UNSUPPORTED` error.

### 6.3 Backward Compatibility

**v3 → v2 Migration:**
An offline compactor tool MUST be provided to convert v3 SSTables to v2 format.

**v4 → v3 Migration:**
Future v4 may use `FormatBump` manifest events for online migration.

---

## 7. Error Codes

| Code                    | Name                   | Description                                         |
|-------------------------|------------------------|-----------------------------------------------------|
| `IO_CORRUPT`            | I/O Corruption         | CRC mismatch or invalid data structure              |
| `PARITY_MISMATCH`       | Parity Failure         | Stripe parity verification failed                   |
| `WAL_TRUNCATED`         | WAL Truncated          | Partial WAL tail detected (not an error)            |
| `FORMAT_UNSUPPORTED`    | Unsupported Format     | Version number not supported by this implementation |
| `MANIFEST_INCONSISTENT` | Manifest Inconsistency | Manifest replay resulted in invalid state           |

---

## 8. References

### 8.1 Algorithms

- **SipHash-2-4:** Aumasson, J.-P., & Bernstein, D. J. (2012). SipHash: a fast short-input PRF. INDOCRYPT 2012.
- **CRC32C:** RFC 3720 (iSCSI), Castagnoli polynomial 0x1EDC6F41
- **Reed-Solomon:** Reed, I. S., & Solomon, G. (1960). Polynomial Codes Over Certain Finite Fields. Journal of SIAM.

### 8.2 Related Specifications

- **LSM-tree:** O'Neil, P., Cheng, E., Gawlick, D., & O'Neil, E. (1996). The Log-Structured Merge-Tree (LSM-Tree). Acta Informatica.
- **LevelDB:** Google. LevelDB Documentation. https://github.com/google/leveldb/blob/main/doc/table_format.md

### 8.3 AkkaraDB Documentation

- User Documentation: `readme/en/` (English), `readme/ja/` (Japanese)
- Requirements: `AkkaraDB-Requirements-v3.md`

---

**End of Specification**

---

**Document Metadata:**

- Version: 3.0
- Last Updated: 2025-12-12
- License: LGPL-3.0
- Copyright: © 2025 Swift Storm Studio
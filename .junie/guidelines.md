AkkaraDB v1 — Requirements v3 (draft / 2025-10-16, Asia/Tokyo)

0. GOALS / SLO

- Read (page cache hit):   P99 ≤ 50 µs (@ ~1M keys)
- Write (WAL durable ACK): P99 ≤ 200 µs (NVMe, group-commit)
- Availability: crash-safe on single node; optional stripe redundancy m ≥ 1
- Simplicity: zero external deps (optional metrics export only)
- Evolvability: on-disk format versioned, backward-readable by v3 readers

1. CORE INVARIANTS

- Global seq: u64 monotonically increasing across the whole DB
- Replacement rule: higher seq wins; if seq equal → tombstone wins (no resurrection)
- Key ordering: byte-wise lexicographic (dictionary order)
- Durability boundary: a write is acknowledged when WAL is durable;
  stripe durability is performed under its own N-or-T policy (configurable)
- Crash recovery: last durable ≤ last sealed (manifest/WAL rules)

2. DATA UNITS (TWO-LEVEL MODEL)
   2.1 In-memory unit: MemRecord (in common)
  - fields: key(ByteBufferL), value(ByteBufferL), seq(u64), flags(u8),
    keyHash(u32), approxSizeBytes(int)
  - helpers: shouldReplace(old,new), lexCompare(a,b), fnv1a32(buf),
    estimateMemFootprint(key,value)
  - flags: TOMBSTONE (0x01) [reserved: SECONDARY, BLOB_CHUNK]

2.2 On-disk view: RecordView (in format-api)

- zero-copy slices over a block payload
- derived from AKHdr32 + key + value; never owns memory

2.3 Header32 (AKHdr32, fixed 32 bytes, LE)

- kLen:u16, vLen:u32, seq:u64, flags:u8, pad0:u8,
  keyFP64:u64, miniKey:u64
- keyFP64: 64-bit fingerprint (e.g., SipHash-2-4)
- miniKey: first 8B key prefix (LE) to speed bloom/secondary checks
- Versioned: hdr.magic="AKH3", hdr.ver=3 (implicit via reader)

3. BLOCK FORMAT (32 KiB)

- Layout:
  [0..3]    : payloadLen (u32, LE)
  [4..4+N)  : payload = repeated { AKHdr32 (32B) + key + value }
  [4+N..-5] : zero padding
  [-4..-1]  : crc32c over [0 .. BLOCK_SIZE-4)
- Constraints:
  N ≤ PAYLOAD_LIMIT (BLOCK_SIZE-8)
  keyLen ≤ 65535, valueLen ≤ 4GiB-1 (u32)
- CRC policy: single pass at seal time; reader must verify before unpack

4. STRIPE (ATOMIC I/O GROUP)

- k data lanes + m parity lanes (m≥0)
- All lanes write block i at the same offset
- Parity:
  m=0: none
  m=1: XOR
  m=2: DualXor
  m≥3: Reed–Solomon (RS( k+m, k ))
- Group commit: N blocks OR T µs → force(fdatasync-ish). Lane writes are consecutive.
- Manifest event: StripeCommit{stripeId, k, m, blocks, crc, ts}
- Failure handling: CRC fail → reconstruct; unrecoverable → IO_CORRUPT

5. WAL & MANIFEST

- WAL: append-only, group-commit (N or T). Record of Ops sufficient to rebuild memtables.
- WAL entry: [len:u32][payload(bytes)][crc32c]
- Manifest (append-only):
  EventType ∈ { StripeCommit, SSTSeal, Checkpoint, Truncate, FormatBump }
  Each event fsync’ed; format version carried for reader gating.
- Recovery procedure:
  1) Read Manifest → locate last consistent boundaries
  2) Replay WAL up to durable tail
  3) Validate stripes (lazy) and rebuild MemTable snapshot or load SSTs

6. API SURFACE (ENGINE)

- AkkDB:
  put(key, value): seq
  delete(key): seq
  get(key): value?
  compareAndSwap(key, expectedSeq, newValue?): Boolean
  iterator(range: KeyRange): Cursor
  close()
- Options (defaults):
  k=4, m=2
  blockSize=32KiB
  wal.groupCommit = N=32 or T=500µs
  stripe.flush = N=32 or T=500µs
  mem.flushThreshold = 64 MiB or 50k entries
  bloomFalsePositive ≈ 1%
  tombstoneTTL = 24h
  index.residency = directBuffer
  fastMode = true (async stripe force; WAL still durable)

7. PACKER / UNPACKER (format-api)

- BlockPacker.tryAppend(key: ByteBufferL, value: ByteBufferL,
  seq: U64, flags: Int, keyFP64: U64, miniKey: U64): Boolean
  beginBlock(), endBlock(), flush(), close()
  → header32 in-place, copy key/value once, zero-pad, crc stamp
- BlockUnpacker.cursor(block32k): RecordCursor
  unpackInto(block, out: MutableList<RecordView>)
  → validates payload bounds, returns zero-copy views

8. MEMTABLE

- Backing: ConcurrentSkipListMap<ByteArrayComparable, MemRecord> or lock-sharded hash+skip
- Insert rule: shouldReplace(prev, new)
- Flush trigger: approxSizeBytes OR entries ≥ threshold (OR condition)
- Flush output: stream MemRecord → BlockPacker (retry on false with end+begin)

9. SSTABLE

- Layout:
  data blocks (32 KiB, keys in ascending order)
  index block  (firstKey, fileOff)
  bloom filter (bitset)
  footer { magic='AKSS', indexOff, bloomOff, entryCnt, crc }
- Residency: DirectByteBuffer for bloom/index (avoid THP)
- Compaction:
  L0 merge when count ≥ 4
  tombstone GC after TTL (24h) during compaction or GC thread
  Generations logged in Manifest for simpler recovery

10. THREADING MODEL (MINIMAL & FAST)

- Flusher: 1 thread (produce 32 KiB blocks via BlockPacker)
- StripeWriter: 1 thread (collect k blocks, encode parity, write, force N-or-T)
- Optional Fsync thread (durable mode)
- Reader: pool (I/O + verify + unpack); MemTable ops are lock-sharded

11. MEMORY & BUFFERS

- BufferPool: DirectByteBuffer pooling (32 KiB blocks + small staging)
- ByteBufferL: LE-fixed, VarHandle-backed, safe absolute/relative ops, crc32cRange(), fillZero()
- No exposure of direct buffers to external callers (except read-only views)

12. METRICS / OBSERVABILITY (NO EXTERNAL DEPS REQUIRED)

- Counters: sealedStripes, durableStripes, walCommits, compactions, tombstoneGC
- Timers (µs): laneWrite, parityEncode, groupForce, walForce, recoverScan
- Gauges: memtableBytes, memtableEntries, cacheHit, l0Count
- Export: JSON snapshot API (pull), optional Prometheus text if enabled

13. ERROR MODEL

- IO_CORRUPT: unrecoverable CRC/parity failure
- PARITY_MISMATCH: reconstruct failed validation
- WAL_TRUNCATED: partial record on restart → discard tail safely
- FORMAT_UNSUPPORTED: reader < writer (version gate)
- Clear messages; include stripeId/file/off/seq for forensics

14. TEST MATRIX (MUST PASS BEFORE RELEASE)

- Property tests: encode→pack→read→unpack round-trip (≥ 1e6 cases)
- Crash injection: kill at random during WAL/stripe write → recover invariants
- Fault injection: bitflip, missing lane (≤ m), partial block tail
- Performance: NVMe, 4k sector, fastMode=true/false
- Long run: 24h with mixed workload; no leaks; stable P99/P999

15. SECURITY / SAFETY

- Zeroization not required by default; optional data-sanitizing on delete (config)
- No unbounded logs; manifest rotation policy
- Defensive bounds checks on all external inputs (tools/repair)

16. COMPATIBILITY / VERSIONING

- On-disk:
  Block magic/version implicit via AKHdr32/footers; reader must deny newer major
- In-memory:
  MemRecord is not serialized across processes (no network semantics yet)
- Migration:
  v2 → v3 offline compactor provided (tool) — re-encodes into AKHdr32 blocks

17. NON-GOALS (v3 scope-out)

- Distributed consensus/replication (RAFT) — future version
- Secondary indexes & range queries beyond primary key — experimental only
- GPU parity encoding — research track
- Encryption-at-rest — plugin space, not core

18. TUNING GUIDE (MINIMUM)

- If P99 write > 200 µs:
  Increase N in group-commit, ensure fastMode=true, verify NVMe writeback cache
- If read tail high:
  Pin index/bloom as direct, enlarge OS page cache, check THP off
- If compaction stalls write:
  Lower L0 limit or add compaction thread; consider tombstone TTL

19. DELIVERABLES

- Code (core + tests) with KDoc in English
- Spec.md mirroring this requirements file
- Bench harness (JMH + e2e microbench)
- Crash/fault injection harness

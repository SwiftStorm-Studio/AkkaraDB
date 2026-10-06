# AkkaraDB Native - Technical Specification

> Current native implementation specification for the C++23 AkkaraDB tree.
> This document is intended to serve as the project-level `Specification.md`:
> it records the implemented contracts, important internal formats, operational
> behavior, and known non-goals. It is not a roadmap.

## Table of Contents

1. [Authority and Scope](#1-authority-and-scope)
2. [System Model](#2-system-model)
3. [Repository and Build Model](#3-repository-and-build-model)
4. [Storage Layout](#4-storage-layout)
5. [Records, Keys, and Ordering](#5-records-keys-and-ordering)
6. [Visibility, Sequences, and Durability](#6-visibility-sequences-and-durability)
7. [Write Path](#7-write-path)
8. [Read Path](#8-read-path)
9. [MemTable](#9-memtable)
10. [WAL](#10-wal)
11. [Blob Manager](#11-blob-manager)
12. [SST and Manifest](#12-sst-and-manifest)
13. [Version Log](#13-version-log)
14. [Generation Layout](#14-generation-layout)
15. [Cluster and Replication](#15-cluster-and-replication)
16. [API Servers and Wire Protocol](#16-api-servers-and-wire-protocol)
17. [High-Level Typed API](#17-high-level-typed-api)
18. [BinPack and Query Model](#18-binpack-and-query-model)
19. [JVM and JNI Integration](#19-jvm-and-jni-integration)
20. [Configuration Reference](#20-configuration-reference)
21. [Statistics](#21-statistics)
22. [Threading and Lifecycle](#22-threading-and-lifecycle)
23. [Error Handling and Recovery](#23-error-handling-and-recovery)
24. [Persistence and Wire Format Reference](#24-persistence-and-wire-format-reference)
25. [Testing and Benchmarks](#25-testing-and-benchmarks)
26. [Explicit Non-Guarantees](#26-explicit-non-guarantees)

## 1. Authority and Scope

This document specifies the behavior implemented by the current native
AkkaraDB source tree. It covers the C++ storage engine, typed C++ API, native
server backends, cluster runtime contracts, disk and wire formats, and the JNI
boundary where it depends on native contracts.

When this document and code disagree, the following sources are authoritative,
in order:

1. Public headers under `akkara/akkaradb/include`, `akkara/akkengine/include`,
   and enabled server headers.
2. The corresponding implementation files and smoke/integration tests.
3. This document.

The supported application-facing native API is the high-level
`akkaradb::AkkaraDB` / `PackedTable` API and the low-level
`akkaradb::engine::AkkEngine` API. Headers under `akkara/akkengine/include/akk`
are public to the source tree, but the low-level C++ ABI, implementation class
layout, and persisted-file compatibility are not promised across arbitrary
releases unless a compatibility line or migration explicitly says so.

This specification describes implemented behavior only. It may mention
configuration fields that are already serialized or exposed even when a
particular advanced runtime path is intentionally partial; those cases are
called out as non-guarantees.

## 2. System Model

AkkaraDB is a binary-safe ordered key/value store. Keys and values are byte
arrays. Raw engine keys are ordered lexicographically by byte value. The storage
engine is LSM-based:

```text
Application
  |
  +-- AkkaraDB / PackedTable typed API
  |      |
  |      +-- BinPack entity encoding
  |      +-- typed indexes, row ids, references, query helpers
  |
  +-- AkkEngine raw byte API
         |
         +-- MemTable, sequence allocation, visibility snapshots
         +-- WAL writer
         +-- optional Blob externalization
         +-- optional SST manager and Manifest
         +-- optional VersionLog
         +-- optional Cluster runtime
         +-- optional HTTP/TCP/gRPC API server
```

### 2.1 Core Properties

| Property | Implemented contract |
|---|---|
| Data model | Binary key/value records with bytewise lexicographic key order |
| Mutation identity | Every mutation receives a 64-bit sequence number |
| Delete model | Deletes are tombstones that suppress older visible records |
| Reads | MemTable first, then SST, with Blob dereference when needed |
| Persistence | WAL, SST, Manifest, Blob files, and VersionLog are configurable |
| Integrity | Persisted structures use CRC32C and explicit magic/version fields |
| Concurrency | Public `AkkEngine` operations are thread-safe |
| Typed C++ API | `PackedTable<&T::primaryKey>` maps entities to the raw KV engine |
| JVM bridge | JNI calls the same native engine; it is not a separate store |
| Network access | HTTP, TCP, and gRPC backends are optional build/runtime components |
| Cluster | Replication placement, acknowledgement, secure/plain transport, and Raft-style config contracts exist |

### 2.2 Component Defaults

`AkkEngineOptions` defaults enable the embedded durable local store:

| Component | Default | Role |
|---|---:|---|
| MemTable | enabled | Mutable in-memory ordered index and sequence allocator |
| WAL | enabled | Write-ahead persistence and recovery source |
| Blob manager | enabled | External storage for values at or above the configured threshold |
| SST manager | enabled | Immutable sorted tables, lookup, flush, and compaction |
| Manifest | enabled | Durable SST lifecycle and checkpoint log |
| VersionLog | disabled | Per-key historical reads and rollback source |
| Cluster runtime | disabled | Placement, replication, and consistency runtime |
| API server | disabled | HTTP/TCP/gRPC frontends |

### 2.3 Non-Goals

- SQL compatibility.
- Distributed transactions across Cluster nodes.
- Public compare-and-swap operations (transactions use optimistic validation).
- Cross-language object identity.
- A stable ABI for private implementation classes.
- A promise that low-level persisted formats can be read by arbitrary future
  releases without an explicit compatibility/migration contract.

## 3. Repository and Build Model

The native repository root is `akkaradb-native`.

```text
akkara/akkaradb/      High-level C++ API and BinPack/query helpers
akkara/akkengine/     Low-level engine, storage, cluster, crypto, platform code
akkara/akkserver/     Optional API server backends and wire protocol
akkara/jni/           JNI bridge and native exports
benchmarks/           Smoke tests, API tests, and throughput benchmarks
cmake/                Build, dependency, packaging, and target definitions
```

The sibling `akkaradb-jvm` project provides Kotlin/JVM wrappers, query DSL
objects, schema/options serializers, and integration tests. Its engine module
loads/calls the native JNI library rather than defining an independent
persistent data format.

### 3.1 Build Requirements

| Item | Requirement |
|---|---|
| Language | C++23 |
| Build system | CMake project with presets and modular CMake includes |
| Official compiler family | LLVM Clang; `clang-cl` on Windows and `clang++` on Linux/macOS |
| Windows linker/toolchain | Visual Studio developer environment for MSVC linker and Windows SDK |
| Core third-party libraries | Zstd, Boost.PFR, mbedTLS; gRPC/protobuf when gRPC backend is enabled |

### 3.2 CMake Options

Defined build options include:

| Option | Default | Meaning |
|---|---:|---|
| `AKKARADB_BUILD_SHARED_LIBS` | `ON` | Build shared native libraries and force `BUILD_SHARED_LIBS` |
| `AKKARADB_BUILD_TESTS` | `OFF` | Build smoke tests and benchmarks |
| `AKKARADB_BUILD_JNI` | `ON` | Build JNI bridge |
| `AKKARADB_PACKAGE_JNI_WITH_NATIVE` | `OFF` | Package JNI beside the native artifact |
| `AKKARADB_BUILD_API_SERVERS` | `ON` | Build API server shared library layer |
| `AKKARADB_BUILD_API_HTTP` | `ON` | Build HTTP transport backend |
| `AKKARADB_BUILD_API_TCP` | `ON` | Build TCP transport backend |
| `AKKARADB_BUILD_API_GRPC` | `ON` | Build gRPC transport backend when dependencies are available |
| `AKKARADB_WARNINGS_AS_ERRORS` | `ON` | Treat AkkaraDB compiler warnings as errors |

If `AKKARADB_BUILD_API_SERVERS=OFF`, individual API backends are forced off. If
the API server layer remains on while all HTTP/TCP/gRPC backend toggles are off,
configuration fails.

### 3.3 Build Outputs

Release artifact names are derived from `version.properties`, the revision
counter, target OS, target architecture, compatibility line, and JNI ABI. The
native build produces:

- `akkaradb` core target.
- `akkaradb_api` server provider layer when API servers are enabled.
- `akkaradb_api_http`, `akkaradb_api_tcp`, `akkaradb_api_grpc` optional backends.
- `akkaradb_cluster` cluster runtime backend.
- `akkaradb_jni` JNI library when JNI is enabled.
- `akkaradb_vlog_tool` maintenance executable for offline VersionLog validation
  and sidecar-index rebuilds.
- SDK/native distribution archives through the packaging CMake scripts.

Consumers should link the exported CMake targets instead of relying on private
source file paths.

## 4. Storage Layout

`AkkEngine::open(AkkEngineOptions)` accepts explicit component paths or derives
them from `paths.dataDir`.

### 4.1 Default Paths

When `paths.dataDir` is set and a component-specific path is empty, defaults are:

| Setting | Default path |
|---|---|
| `paths.walDir` | `{dataDir}/wal` |
| `paths.blobDir` | `{dataDir}/blobs` |
| `paths.sstDir` | `{dataDir}/sstable` |
| `paths.manifestPath` | `{dataDir}/manifest.akmf` |
| `paths.versionLogPath` | `{dataDir}/history.akvlog` |
| `paths.rollbackJournalPath` | `{dataDir}/rollback-journal.akrb` |
| `paths.clusterConfigPath` | `{dataDir}/cluster.akcc` |
| `paths.nodeIdPath` | `{dataDir}/node.id` |

If a component is enabled and its path cannot be derived or created, `open`
fails rather than silently disabling the component.

### 4.2 Legacy Flat Layout

With `runtime.generationLayoutEnabled == false`, mutable storage files are kept
directly under the derived paths listed above. This preserves the pre-generation
directory shape.

### 4.3 Generation Layout

With `runtime.generationLayoutEnabled == true`, `paths.dataDir` is a database
root. Mutable component files are placed under the active generation directory.

```text
{dataDir}/
  current.akgen
  generations/
    gen-1/
      wal/
      blobs/
      sstable/
      manifest.akmf
      history.akvlog
      cluster.akcc
      node.id
  staging/
```

For an empty root, the initial active generation is `generations/gen-1`.
Staging generations are not made active until the generation manager activates
them. Enabling generation layout on a nonempty legacy directory is rejected; a
migration into a generation directory must be explicit.

## 5. Records, Keys, and Ordering

Every mutation is represented as one logical record:

- normal value,
- tombstone,
- or Blob reference.

Tombstones suppress older values in MemTable and SST reads. A Blob reference is
stored as the record value with the Blob flag set; public reads return the
dereferenced application value.

### 5.1 Sequence Numbers

Sequences are unsigned 64-bit mutation identifiers. They provide:

- newest-visible selection for one key,
- read snapshot upper bounds,
- WAL/SST/VersionLog replay ordering,
- Blob id assignment for values externalized by local writes,
- cluster write identity and acknowledgement tracking.

Sequence numbers alone do not provide transactions. Internal protocol bulk
writes reserve a range without atomicity. Standalone transactions publish
one commit sequence for all keys, with durable redo and validation.

### 5.2 MemHdr16

`core::MemHdr16` is a naturally aligned 16-byte in-memory header. It is not a
wire or disk file format.

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 8 | `seq` | Mutation sequence |
| 8 | 2 | `kLen` | Key byte length, maximum 65,535 |
| 10 | 2 | `vLen` | Stored-value byte length, maximum 65,535 |
| 12 | 1 | `flags` | `0x00` normal, `0x01` tombstone, `0x02` Blob reference |
| 13 | 1 | `version` | Header version, currently 1 |
| 14 | 2 | `reserved` | Written as zero |

The associated payload is contiguous `[key][stored value]`.

### 5.3 OwnedRecord

`core::OwnedRecord` is exactly 64 bytes:

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 16 | `MemHdr16 hdr` | Sequence, lengths, flags |
| 16 | 8 | `keyFp64` | 64-bit key fingerprint |
| 24 | 8 | `miniKey` | First up to 8 key bytes, little-endian packed |
| 32 | 32 | `SmallBuffer data` | Inline or arena-backed `[key][value]` |

`OwnedRecord` owns its bytes and is not a thread-safe shared object.
Fingerprints and mini keys are optimization hints only. Full key comparison is
the correctness authority.

### 5.4 SSTHdr32

`core::SSTHdr32` is the 32-byte per-record header used inside SST data blocks:

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 8 | `seq` | Mutation sequence |
| 8 | 2 | `kLen` | Key length |
| 10 | 2 | `vLen` | Stored-value length |
| 12 | 1 | `flags` | normal/tombstone/Blob flags |
| 13 | 1 | `reserved0` | Written as zero |
| 14 | 2 | `reserved1` | Written as zero |
| 16 | 8 | `keyFp64` | 64-bit key fingerprint |
| 24 | 8 | `miniKey` | Key prefix hint |

The complete SST record is `[SSTHdr32][key][stored value]`.

### 5.5 BlobRef

When the Blob manager externalizes a value, MemTable, WAL, SST, and VersionLog
store this 20-byte `BlobRef` as the record value:

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 8 | `blobId` | Blob identifier, currently the creating sequence |
| 8 | 8 | `totalSize` | Original uncompressed content size |
| 16 | 4 | `contentCrc32c` | CRC32C of original content bytes |

### 5.6 Raw Key Ordering

Raw engine keys are compared lexicographically as unsigned bytes. Empty start
or end keys in range APIs represent an unbounded side of the range.

### 5.7 Typed Table Key Layout

`PackedTable<PrimaryKeyPtr>` maps typed rows into raw keys.

Primary entry:

```text
[table-prefix:u64le][encoded-primary-key] -> BinPack(entity)
```

`table-prefix` is `FNV-1a-64(table name)`.

Secondary index entry:

```text
[index-prefix:u64le][field-length:u32le][encoded-field][encoded-primary-key] -> empty value
```

Secondary indexes are non-unique. The primary-key suffix distinguishes rows
with equal indexed field values.

Row-id metadata:

```text
[pk2row-prefix:u64le][encoded-primary-key] -> BinPack(RowId)
[row2pk-prefix:u64le][encoded-row-id]       -> encoded-primary-key
[nextrow-prefix:u64le]                      -> BinPack(next-row-id)
```

Integral primary keys up to eight bytes use fixed-width sortable big-endian
encoding. Signed integral keys flip the sign bit before encoding. Other primary
keys use BinPack. This preserves numeric order for integral primary-key scans
while leaving the raw engine byte-ordered.

Secondary index field values use sortable encodings where the query planner
requires bytewise range order: unsigned big-endian for unsigned integrals,
sign-bit-flipped big-endian for signed integrals, sortable IEEE-754 transforms
for floating point, and BinPack for other values.

Prefix indexes for string-like fields store prefix-searchable string segments
under a prefix-specific key namespace.

## 6. Visibility, Sequences, and Durability

AkkaraDB separates three write concerns:

1. Admission: whether local writes are serialized or use an allowed concurrent
   path.
2. Visibility: when a mutation may be observed by readers.
3. Durability acknowledgement: how far the WAL path must progress before the
   mutation call returns.

### 6.1 Write Policy Presets

`runtime.writePolicy` resolves shorthand presets:

| Preset | Durability | Visibility |
|---|---|---|
| `CUSTOM` | Use explicit `runtime.writeDurability` | Use explicit visibility fields |
| `SAFE` | `SYNCED` | `COMMIT_ORDER` |
| `BALANCED` | `WRITTEN` | `COMMIT_ORDER` |
| `FAST` | `ENQUEUED` with WAL, otherwise `MEMORY` | `APPLIED` |

After preset resolution, `runtime.visibility.readVisibility` overrides the
legacy `runtime.writeVisibility` value when it is not `AUTO`. `AUTO` preserves
the legacy field for source compatibility. `stats().config` reports the
resolved settings as numeric enum values.

### 6.2 Durability Modes

| Mode | Return boundary |
|---|---|
| `MEMORY` | No WAL acknowledgement; valid only when WAL is disabled |
| `ENQUEUED` | Async WAL queue accepted the record |
| `WRITTEN` | WAL batch reached the OS file cache |
| `SYNCED` | WAL batch completed `fdatasync` or the platform equivalent |

`ENQUEUED` may make a write visible before the WAL persists it. If the async
flusher later fails, the WAL writer is poisoned; queued work fails, later data
operations rethrow the stored failure, and `stats()` remains available.

### 6.3 Visibility Modes

`COMMIT_ORDER` publishes only the largest contiguous completed sequence prefix.
Readers do not observe sequence `n + 1` while sequence `n` is still incomplete.

`APPLIED` exposes an entry after it is applied to the MemTable. It does not wait
for earlier writers. It is suitable for independent-key ingestion where global
sequence-prefix visibility is not required.

```text
Writer A reserves seq 100 and stalls.
Writer B reserves seq 101 and applies it.

COMMIT_ORDER: readers cannot observe 101 yet.
APPLIED:      readers may observe 101 before 100.
```

`APPLIED` is thread-safe but intentionally not a global prefix view.

### 6.4 Read Snapshots

Each point read, `exists`, `count`, and `scan` captures one visibility snapshot
before accessing MemTable and SST state. `getBatch` captures exactly one
snapshot when the call begins and uses it for every requested key.

For `COMMIT_ORDER`, the snapshot is the committed contiguous sequence. For
`APPLIED`, it is the largest sequence currently reserved by the MemTable
allocator. With thread-local ranges, that upper bound may contain holes while
writers are in flight. `EngineStats::currentSeq` reports the same read-snapshot
upper bound; it is not a count of completed writes.

The visibility snapshot is an upper bound for the current read operation. It is
not a durable historical-read contract for the MemTable or SST layers. Those
layers only apply the bound to records they currently retain; persistent
historical reads are provided by VersionLog.

### 6.5 Write Admission

| Mode | Behavior |
|---|---|
| `SERIAL` | Use the engine write mutex |
| `PARALLEL` | Permit concurrent local writes when Blob and cluster are disabled; WAL may remain enabled. VersionLog admission is configured independently. |
| `AUTO` | Use the concurrent in-memory fast path only when `relaxedConcurrentWrites` is true and WAL, Blob, and cluster are disabled; otherwise serialize |

Invalid component combinations fail during `open`.

`runtime.parallelWriteOrder` controls same-key ordering on the parallel path:

| Mode | Behavior |
|---|---|
| `APPLY_ORDER` | Default fast path. Reads follow the order writes reached the MemTable. |
| `KEY_SEQUENCE` | Requires `sequence.allocation=GLOBAL_ATOMIC` and `COMMIT_ORDER`. Writes on the same internal ordering stripe reserve and apply their sequence while holding that stripe's mutex. Writes on different stripes remain concurrent. Reads wait for the prefix reserved at read start to commit, so they do not expose an earlier cross-key sequence gap. |

### 6.6 Sequence Allocation

| Mode | Contract |
|---|---|
| `GLOBAL_ATOMIC` | Allocate from the shared MemTable allocator; required for `COMMIT_ORDER` |
| `THREAD_LOCAL_RANGES` | Reserve a range per thread; requires `APPLIED`, `threadLocalRangeSize > 1`, disabled cluster, and disabled VersionLog |

`sequence.commitWindowSize == 0` selects the default 65,536-slot commit window.
Explicit values are rounded up to a power of two and constrained to the
implementation range of 64 through 2^24 slots.

### 6.7 Engine Backpressure

`runtime.backpressure` throttles new writes independently from WAL queue
backpressure.

| Limit | `0` means | Trigger |
|---|---|---|
| `maxMemtableImmutableTables` | disabled | Immutable MemTable backlog |
| `maxSstL0Files` | disabled | L0 SST backlog |

Each trigger can `BLOCK` or `FAIL_FAST`. Blocking polls every `waitMicros` and
is bounded by `timeoutMs`, which defaults to 30,000 ms. `timeoutMs == 0`
permits an unbounded wait. Blocking L0 backpressure with compaction disabled is
rejected at open because it could wait forever.

Stats include blocked, rejected, timed-out, MemTable-stall, SST-stall,
total-wait, and max-wait counters.

## 7. Write Path

For a normal local mutation:

1. Check engine lifetime and previously stored background failures.
2. Apply configured engine backpressure.
3. Reserve a sequence or sequence range.
4. Externalize the value through Blob manager when enabled and threshold is met.
5. Append WAL and VersionLog records when enabled.
6. Apply value or tombstone to the MemTable.
7. Publish visibility according to the resolved visibility mode.
8. Ship the committed local mutation through cluster runtime when applicable.

`putHinted` and `removeHinted` trust caller-provided key fingerprint and mini-key
hints. A wrong hint is caller error. Standard `put` and `remove` compute hints
internally.

### 7.1 Standalone Native Transactions

The C++ `AkkEngine` public API removes `putBatch` and `BatchPutEntry`:

```cpp
auto tx = engine.begin(); // SERIALIZABLE by default
auto value = tx.get(key);
tx.put(otherKey, newValue);
tx.remove(deletedKey);
// Also: tx.exists, tx.getBatch, tx.scan(arena,start,end), tx.count(start,end)
tx.end(); // atomically commit, or throw
```

`begin({TransactionIsolation::SNAPSHOT_ISOLATION})` selects Snapshot Isolation.
Both modes read a fixed begin snapshot and their own staged changes. Repeated
staging for one key coalesces into its final value/tombstone. Regular Engine
writes commit independently and do not join a session. `rollback()` or session
destruction without `end()` discards staging.

Both modes validate write keys against changes since begin (first committer
wins), including ordinary writes, internal bulk writes and history rollbacks.
Serializable also validates point reads, including absent keys, and half-open
scan/count predicates `[start,end)`, including unconsumed scan cursors. It detects
range phantoms and put/remove ABA changes. Snapshot Isolation permits write skew
on disjoint keys. Conflicts throw `TransactionConflict` and abort the session;
retry with a fresh begin. Sessions require standalone mode, `GLOBAL_ATOMIC`
sequence allocation, and recovery enabled for any enabled WAL/SST component.

Cluster distributed transactions, typed-layer transactions and transaction
endpoints in Java/JNI, HTTP, TCP and gRPC are outside this API. Existing external
bulk protocols retain their non-transactional behavior through the internal
`detail::ProtocolBulkWriter`; no public C++ putBatch compatibility alias remains.

Sessions retain immutable MemTable generations, SST readers and Blob read leases.
Writers continue while a session is open; commit takes exclusive mutation
admission/publication. Ordinary point/batch reads cannot observe partial commit.
Ordinary scans keep `runtime.scanConsistency`; transaction scans always use the
fixed begin snapshot. A transaction scan captures staged writes at its call and
returns `ArenaGenerator<ScanRecordView>`. It may outlive end/rollback, retaining
that cut. Views follow the supplied Arena lifetime. Finish/destroy all sessions
and cursors before `engine.close()`, which waits for their operation leases.
Concurrent session/cursor use is unsupported; sequential thread handoff is
supported, including Blob snapshots.

`options.transactions` bounds resources:

| Option | Default | Contract |
|---|---:|---|
| `maxOpen` | 64 | Retained transaction snapshots, including surviving cursors |
| `maxLifetimeMs` | 300000 | Deadline checked at session operations/commit; idle sessions and independent cursors release pins on destruction |
| `maxWrites` | 65536 | Distinct staged keys |
| `maxWriteBytes` | 67108864 | Staged key/value bytes plus accounting overhead |
| `maxReadSetBytes` | 33554432 | Serializable read/range validation set |
| `maxPinnedBytes` | 536870912 | Conservative sum of retained MemTable bytes |
| `maxTrackedBytes` | 67108864 | Mutation tracking; overflow invalidates open sessions instead of stopping ordinary writers |
| `maxJournalBytes` | 268435456 | Retained redo; checkpoint before another commit exceeds the budget |

Capacity/staging errors throw before staging the offending change. Expired
sessions throw `TransactionConflict` and abort. Tracking invalidation is detected
on commit. A single oversized journal aborts before its decision. Retained journal
count is also bounded at 1024. Read-only commits validate without persistence I/O.

### 7.2 Transaction Persistence and Recovery

With any WAL, SST, VersionLog or Blob component enabled, transactions require
`paths.transactionDir`, defaulting to `dataDir/transactions`. Memory-only engines
need no journal. Commit durably saves sequence allocation before Blob preparation,
then durably publishes complete redo before applying any key. The decision is
always synchronized, even with weaker ordinary-write acknowledgements; persistent
transaction commits therefore have a minimum synchronization cost. Durable current
state still requires WAL or SST: Blob/VersionLog alone does not persist ordinary
MemTable contents after a clean close.

All transaction mutations share a commit sequence and timestamp in WAL/SST/history;
Blob IDs use distinct reserved slots. With VersionLog enabled, getAt before that
sequence sees prior revisions and getAt at it sees all new revisions. Interrupted
application is replayed before exposing an open Engine. Replay preserves higher
sequence overwrites and deduplicates history already persisted.

`<commit-seq>.aktxn`: `AKTX1`, little-endian u64 base/commit/source/time/count,
then key-sorted records of flags(u8), key length(u32), stored value length(u64),
key/value bytes, and CRC32C(u32). Blob payloads remain in durable Blob files.
`allocated.aktm`/`checkpoint.aktm`: `AKTM1`, u64 watermark, CRC32C.
Invalid sizes, sequence ranges, flags, ordering, checksums and trailing data reject
open; incomplete `.tmp` decisions are uncommitted.

Allocation watermarks prevent Blob ID reuse after pre-decision crashes. Retained
redo prevents orphan Blob collection from startup through recovery/checkpoint.
Checkpoint drains complete MemTables to SST where enabled, synchronizes WAL/history,
durably saves a checkpoint watermark, then removes redundant redo. Partial shard
flushes cannot discard an incompletely applied transaction. Existing WAL/SST/Blob/
history formats are unchanged. Keep the new journal directory with the database;
recovery requires a transaction-aware Engine. No older-binary migration is provided.

Before-decision errors discard staging. Errors after a decision or after memory
application starts throw `TransactionOutcomeUnknown` and block reads/writes on that
Engine. Close/reopen to recover; close retains incomplete redo even if it reports a
storage error. Memory-only engines cannot recover interrupted application after
close. Missing/corrupt committed Blob data rejects open rather than exposing a
partial transaction.

## 8. Read Path

### 8.1 Point Lookup

Point reads search newest visible MemTable state first, then SST state:

1. Capture visibility snapshot.
2. Search MemTable by key and snapshot.
3. If MemTable returns a visible tombstone, report missing.
4. If MemTable returns a normal or Blob record, materialize the public value.
5. If MemTable misses, search retained SST records with the same snapshot upper
   bound.
6. If SST returns a tombstone, report missing.
7. If SST returns a normal or Blob record, materialize the public value.
8. If `runtime.sstPromoteReads` is enabled, copy the SST hit back into the
   MemTable with its original sequence and flags.

`sstPromoteReads` is a cache/promotion optimization. It is not a new mutation
and must not alter snapshot semantics.

### 8.2 Public Point APIs

| API | Missing result | Allocation and lifetime |
|---|---|---|
| `get` | `std::nullopt` | Owning `std::vector<uint8_t>` |
| `getBatch` | `found == false` per result | Owning vectors; one snapshot for all keys |
| `exists` | `false` | No value materialization |
| `getInto` | `false` | Writes into caller vector |
| `getIntoArena` | `false` | Returns span into caller-owned `BufferArena` |

### 8.3 Range Reads

`count(startKey, endKey)` and `scan(arena, startKey, endKey)` use one captured
visibility snapshot. Empty start or end spans are unbounded. Scan merges
MemTable and SST ordered iterators, gives a MemTable version precedence over an
SST version of the same key, and suppresses tombstones.

With Cluster enabled these APIs return public keys across the configured data
placement. PARTITIONED queries each data owner, filters physical replicas by
ownership, and merges scans in unsigned byte-key order without duplicates.
Each owner captures its own read cut: this is not a globally atomic distributed
transaction snapshot. MIRROR/Raft queries the current Primary/leader in
OWNER_LINEARIZABLE mode, including when invoked on a follower or learner; the
leader completes ReadIndex before capturing its snapshot. LOCAL_STALE_OK permits
node-local MIRROR/Raft results. PARTITIONED range queries still gather all owners
in LOCAL_STALE_OK mode. PARTITIONED Raft range planning checks partition leaders
concurrently through one executor per initiating engine, with at most eight
active checks and 64 queued tasks shared across concurrent queries. All checks
retain the same placement view and share the original forwarding deadline,
including queueing and local ReadIndex. Results are grouped in partition order
only after every check succeeds; any failure joins submitted work and fails the
query without returning a partial plan. This does not add a common read cut
across partitions. STRIPE queries its metadata-Raft leader through a fresh
quorum barrier, translates metadata keys to public keys, suppresses logical
tombstones, and reconstructs public values from the captured shard generations.
STRIPE count counts live metadata keys and does not require reading their shards.

OWNER_ONLY never contacts a remote authority; a range requiring remote owners
fails instead of returning just the local portion. These read-only query RPCs
are independent of mutation `routingMode`, so REDIRECT/LOCAL_ONLY do not disable
internal range/history gathering. `scanLocalStorage` remains explicit physical
node-local inspection, including replicated records and internal STRIPE keys.

Cluster scan/history delivery is selected by `queryResultMode`, and the
initiating engine includes its choice in OPEN. `PREPARED` (default) writes the
complete result to a private disk spool before returning; consumption then
needs no storage snapshot pins. `STREAMING` captures fixed sources at OPEN and
generates up to 1 MiB per NEXT page without spooling the complete result. A
record may span pages. The reader retains one page and one current record per
owner, plus caller-owned output. Page replay retains only the latest page;
non-sequential offsets are rejected. A failed streaming cursor cannot resume
as if it had completed successfully.

Both modes seal active MemTables into immutable sources and switch writers to
new active tables. The short capture excludes publication, but copying and
consumer waits do not hold MemTable shard locks. Sources keep their exact
MemTable/SST ownership and Blob read pins until materialization ends. SST-less
engines use the existing immutable memory snapshot mechanism. Each partition
still captures its own read cut; this does not add a distributed transaction
snapshot. STRIPE metadata range capture preserves public byte order, seeks only
the requested range, and reconstructs one public value at a time. No separate
full metadata spool is required. COUNT streams live metadata without shard reads
or result spooling.

Snapshot admission is independent of result delivery: `querySnapshotAdmission`
is `REJECT` by default, or `WAIT` within the original OPEN deadline. Capacity
waits release write admission. `queryMaxPinnedBytes` defaults to 512 MiB
(1 MiB–1 TiB) and bounds aggregate reservations for captured MemTable sources;
shared generations can be conservatively counted more than once. It is not a
limit on SST files, history index locations, individual decoded values or total
process RSS. `queryMaxPinnedSnapshots` defaults to 64 (1–1,024). The existing
SST-less snapshot capacity also applies. Overlarge captures fail under either
admission policy. The MemTable/SST-less immutable sources are released on
completion, explicit close, abandonment, or cursor expiry.

Streaming STRIPE scans/history acquire renewable GC pins on every active data
node before capture. All those nodes must be reachable for this optional mode.
Pins prevent deletion of captured shard generations, and are renewed for each
page. A missing/expired pin or placement change fails the query explicitly.
Closing the cursor requests release; an unreachable node releases its pin by
TTL. No thread-owned GC or iterator lock crosses RPC workers.

Runtime cursor limits are `queryMaxSpoolBytes` (1 GiB aggregate per engine;
1–INT64_MAX), `queryMaxOpenCursors` (64; 1–1,024),
`queryCursorIdleTimeoutMs` (60,000; 1–300,000), and
`queryCursorMaxLifetimeMs` (300,000; 1–3,600,000, including preparation).
Spools use `transfer.spoolDirectory` or the OS temporary directory, are private
anonymous/delete-on-close files, and are discarded on close, abandonment, TTL,
or process exit. An expired cursor throws instead of returning truncated
success. `forwardingTimeoutMs` bounds each OPEN/NEXT RPC; capacity and peer-pin
work share that RPC's remaining budget. Exceeded quotas, failed owners/quorum,
missing required shards/Blobs, and mid-query disconnects fail explicitly.
A scan or history generator may have yielded a prefix before failure; only
normal end-of-iteration means the complete result was received. COUNT/getAt
return no partial successful result. Failed unsent RPCs may wait for reconnection
within their original timeout; an ambiguous OPEN is not replayed automatically.
The pre-release READ_QUERY OPEN/page protocol changes with these policies;
all cluster binaries must be updated together. Persisted data formats do not
change for query delivery.

`scan` returns `ArenaGenerator<ScanRecordView>`. Each `ScanRecordView` contains
non-owning key/value spans into the supplied `BufferArena`. Resetting or
destroying the arena invalidates yielded views.

The scan generator also holds an engine operation lifetime; `close()` waits for
active scan generators to finish or be destroyed.

## 9. MemTable

The MemTable is a sharded ordered in-memory index. It also owns the primary
sequence allocator used by the engine.

### 9.1 Options

| Field | Default | Meaning |
|---|---:|---|
| `shardCount` | 0 | Auto-derive a power-of-two shard count |
| `expectedConcurrentWriters` | 0 | Sizing hint for auto-sharding |
| `autoShardCountCap` | 128 | Auto-shard upper bound before hard implementation ceiling |
| `flushMode` | `AUTO` | Normalize to byte-triggered or manual-only |
| `thresholdBytesPerShard` | 64 MiB | Active-shard rotation threshold |
| `flushInputMode` | `MATERIALIZE_VECTOR` | Immutable flush callback input shape |
| `backendOptions` | defaults | Backend-specific construction options |
| `backendFactory` | null | Use default ordered backend |
| `onFlush` | null | Optional immutable-table flush callback |
| `onFlushStream` | null | Optional streaming immutable-table flush callback |

The implementation hard ceiling is 256 MemTable shards. `AUTO` flush mode maps
to `BYTES_PER_SHARD` when `thresholdBytesPerShard > 0`, otherwise to
`MANUAL_ONLY`.

### 9.2 Backends and Version Retention

Built-in ordered backends include SkipList, B+Tree, and ART implementations.
The public contract is the `IMemTable` behavior, not a backend-specific memory
layout.

Built-in backends keep up to four in-memory versions per logical key by
default. `MemTableBackendOptions::maxVersionsPerKey` may raise or lower this
per mutable backend instance where supported. This is an in-memory optimization,
not the historical-read API. Persistent history requires VersionLog.

Backend construction options are:

| Field | Default | Meaning |
|---|---:|---|
| `mutableScanMode` | `RECONCILE` | B+Tree mutable scan strategy; other built-in backends ignore it |
| `bptreeConcurrencyMode` | `LOCKED` | B+Tree structural concurrency policy |
| `bptreeIteratorMode` | `MATERIALIZE_ALL` | B+Tree locked-iterator materialization policy |
| `bptreeIteratorBatchSize` | 1024 | Maximum visible records per read-lock section in batched B+Tree iteration |
| `maxVersionsPerKey` | 4 | In-memory same-key version retention for supported backends |

`BPTreeConcurrencyMode::LOCKED` serializes structural writes against mutable
B+Tree readers. In this mode, `BPTreeIteratorMode::MATERIALIZE_ALL` materializes
the full visible iterator result under one read lock and releases the lock
before yielding to the caller. `MATERIALIZE_BATCHED_UNPINNED` materializes
bounded batches under separate read locks, reducing long writer stalls during
large scans. It does not pin older same-key versions beyond
`maxVersionsPerKey`; long scans that overlap heavy same-key rewrites can lose
older mutable versions and should use `MATERIALIZE_ALL` when strict mutable
snapshot completeness is required.

Frozen B+Tree MemTables have no structural writers, so they stream directly
regardless of `bptreeIteratorMode`. This avoids an extra full-scan materialized
vector during immutable-table flush.

`BPTreeConcurrencyMode::OPTIMISTIC_UNSAFE` keeps the legacy optimistic reader
path for experiments. It is not the default because concurrent internal splits
can expose transient unreachable paths to readers.

### 9.3 Lookup Semantics

`MemTable::get` returns a `RecordView` when the key exists at or before the
snapshot sequence. Tombstones are returned as records so the engine can stop
the SST search.

`getInto` and `contains` return:

| Value | Meaning |
|---|---|
| `std::nullopt` | Not found in MemTable; caller should consult SST |
| `false` | Found tombstone |
| `true` | Found live value |

### 9.4 Flush Lifecycle

When a shard reaches its flush threshold, the active table is frozen and moved
to the immutable-table list. A fresh active backend is installed. Background
flush workers iterate immutable tables in key order and invoke either `onFlush`
or `onFlushStream`, depending on `flushInputMode`.

In the engine, `onFlush` writes records through `SSTManager::flush`, checkpoints
the Manifest, optionally prunes WAL through the returned checkpoint sequence,
and can run Blob GC when configured.

`MemTableFlushInputMode::MATERIALIZE_VECTOR` materializes each immutable table
into one sorted `std::vector<RecordView>` before invoking `onFlush`. This keeps
the legacy callback contract and gives consumers a contiguous span.

`MemTableFlushInputMode::STREAMING` passes an estimated record count and an
`ArenaGenerator<RecordView>` to `onFlushStream`. `AkkEngine` uses this mode to
stream immutable records into `SSTManager::flush`, which forwards the generator
to `SSTWriter::write`. This removes the flush worker's full `RecordView`
materialization vector. SST writing still retains format metadata such as block
index, key arena, Bloom filter bits, and Manifest blob-reference entries until
the file is sealed.

`forceFlush()` schedules all shards and waits for flush workers to drain.
Asynchronous flush failure poisons later engine work and is rethrown by
`forceFlush()` / public operation boundaries.

## 10. WAL

The WAL stores mutation records before or alongside MemTable visibility,
depending on resolved durability mode.

### 10.1 Options

| Field | Default | Meaning |
|---|---:|---|
| `walDir` | derived from data dir | Segment directory |
| `syncMode` | `SYNC` | Compatibility shorthand |
| `execution` | `AUTO` | `INLINE` or `ASYNC` physical append path |
| `syncPolicy` | `AUTO` | `NEVER`, `ON_SYNC_ACK`, or `ALWAYS` |
| `backpressure` | `BLOCK` | Async queue overflow policy |
| `shardCount` | 0 | Auto, one per hardware thread capped at 16 |
| `groupN` | 128 | Async batch entry threshold |
| `groupMicros` | 100 | Async batch delay threshold |
| `groupBytes` | 4 MiB | Async batch byte threshold |
| `asyncMaxPendingBytes` | 64 MiB | Queued plus in-flight byte limit |

When `execution == AUTO`, `syncMode == ASYNC` selects async execution and other
values select inline execution. When `syncPolicy == AUTO`, compatibility
`syncMode` values expand to the implemented sync policy. A `SYNCED` durability
acknowledgement with `NEVER` sync policy is invalid.

The low-level WAL writer accepts at most 16 shards. Engine-side `writerThreads`
derivation can otherwise choose a larger logical value, so operators using high
writer counts should set `wal.shardCount` explicitly within `1..16`.

### 10.2 Acknowledgement Boundaries

`WalAppendAck::ENQUEUED`, `WRITTEN`, and `SYNCED` are acknowledgement
boundaries, not alternate file formats.

`WRITTEN` means the WAL batch has been written/flushed to the operating-system
file cache. `SYNCED` requires a sync policy capable of issuing storage sync.

### 10.3 Async Failure Semantics

An async WAL failure:

- records the first failure in the writer,
- completes waiting calls exceptionally,
- fails queued work,
- increments failure stats,
- marks the writer unhealthy,
- is rethrown by later engine data operations,
- remains diagnosable through `stats()`,
- does not prevent best-effort `close()`.

### 10.4 Recovery

WAL recovery validates segment headers and entry CRCs. It replays only a valid
contiguous prefix into a fresh MemTable. A truncated or corrupt tail stops replay
at the recovery boundary; later valid-looking bytes are not accepted as a
replacement for a valid log sequence.

When `runtime.truncateCorruptWalOnRecovery` is enabled, WAL recovery may rewrite
physical WAL files while applying the same valid-prefix rule. The corrupt segment
is truncated to its valid prefix, and later segments in the same shard are
removed because they are beyond the accepted recovery boundary. The default is
non-destructive recovery; truncation is opt-in.

When VersionLog is enabled and WAL recovery stops at a corrupt boundary, engine
startup uses VersionLog records newer than the checkpoint sequence to supplement
the recovered MemTable. This is not WAL byte repair; it reconstructs missing
mutations from an independent durable history source when that source contains
the corresponding records.

If `runtime.ignoreVersionLogSupplementErrors` is enabled, a VersionLog startup
or supplement scan failure during corrupt-WAL recovery is ignored and startup
continues with the WAL valid prefix only. The opened engine does not use
VersionLog for that process. The default is to fail startup when VersionLog
supplementation fails.

## 11. Blob Manager

The Blob manager externalizes large values so MemTable, WAL, SST, and VersionLog
store compact references.

### 11.1 Options

| Field | Default | Meaning |
|---|---:|---|
| `blobDir` | derived from data dir | Blob directory |
| `thresholdBytes` | 16 KiB | Externalize values with size >= threshold |
| `codec` | `NONE` | `NONE` or `ZSTD` payload codec |
| `gcOnFlush` | `false` | Run orphan collection after flush lifecycle |
| `gcOnClose` | `false` | Run orphan collection during close |

Blob id currently equals the creating sequence. Blob files carry a 48-byte v1
header and CRCs for both header and original content. Reads validate the header,
codec, stored size, and expected content CRC before returning a public value.

`runBlobGc()` is a no-op when Blob is disabled. It throws when VersionLog is
enabled because historical records may still reference older Blob ids.

Blob externalization disables parallel write admission.

## 12. SST and Manifest

### 12.1 SST Manager

The SST manager stores immutable sorted files under `sstDir`.

| Option | Default | Meaning |
|---|---:|---|
| `maxLevels` | 7 | Number of LSM levels |
| `maxL0Files` | 4 | L0 compaction trigger |
| `l1MaxBytes` | 64 MiB | Level-1 size budget |
| `levelSizeMultiplier` | 10.0 | Budget multiplier for later levels |
| `compactionMode` | `AUTO` | Normalize to background or disabled |
| `targetFileSize` | 64 MiB | Flush/compaction output target |
| `blockSize` | 32 KiB | SST data block target |
| `bloomBitsPerKey` | 10 | Bloom filter density |
| `blockCacheBytes` | 64 MiB | Block cache budget |
| `compactThreads` | 2 | Background compaction worker count |
| `codec` | `ZSTD` | SST block codec |
| `zstdCompressionLevel` | 1 | Zstd level for newly written SST blocks; must be within the linked Zstd range |

In `AUTO`, zero `compactThreads` disables background compaction; otherwise it
selects background compaction.

Reads search L0 newest first because L0 files may overlap. Higher levels are
kept as ordered ranges by compaction policy. SST lookup and scan apply the
caller-provided snapshot sequence as a visibility upper bound over records that
remain in the current SST set.

### 12.2 Flush and Compaction

MemTable flush writes sorted records to an L0 SST. Compaction replaces one or
more input files with output files and may collapse multiple records for the
same key to the newest retained record. SST compaction is not required to
preserve compacted-away historical versions for arbitrary older snapshot
sequences; `getAt`, `history`, and rollback use VersionLog for persistent
history. The Manifest's atomic `COMPACTION_COMMIT` record names all outputs and
all inputs, so recovery either installs the whole replacement or preserves the
old inputs and discards orphan outputs.

Blocking L0 backpressure with compaction disabled is rejected at open.

### 12.3 Manifest

The Manifest is a durable append-only log of SST and cluster lifecycle state.
It records:

- stripe counter advances,
- SST seal events,
- checkpoints,
- legacy compaction start/end records,
- SST delete records,
- atomic compaction commits,
- truncation markers,
- node join/leave events,
- primary lease records.

`fastMode == false` writes and syncs each manifest record. `fastMode == true`
uses a background flusher and provides lower-latency but weaker immediate
manifest durability.

Manifest replay rebuilds the live/deleted SST sets and last checkpoint state.

## 13. Version Log

VersionLog is opt-in. It backs:

- `getAt(key, seq)`,
- `history(key)`,
- `rollbackTo(seq)`,
- `rollbackKey(key, seq)`.

`getAt` and `history` support Cluster through the same bounded read-query cursor
protocol as range reads. PARTITIONED always reads the key's actual owner history,
even with LOCAL_STALE_OK, because a replica's physical apply sequence is not an
owner-stream revision. MIRROR/Raft honors readMode and performs ReadIndex on the
leader for OWNER_LINEARIZABLE. STRIPE reads metadata-leader history through a
quorum barrier and reconstructs historical public shard generations; placement
repairs with an unchanged logicalSeq are coalesced into one public version.
Both `AkkEngine::history(key)` and `VersionLog::history(key)` return
`core::ArenaGenerator<VersionEntry>`; the owning-vector history APIs were removed.
A generator owns its coroutine Arena, copies the input key, and captures its
read cut during the call. Entry values are materialized one at a time; a yielded
entry reference remains valid only until advancement or destruction, and callers
copy entries they intend to retain. Iteration can move between threads but must
be serialized. An engine history generator holds operation admission, so
`close()` waits until it is consumed/destroyed. Runtime read failures can occur
during iteration. VersionLog-disabled history yields no entries.
VersionLog captures per-key index locations plus its bounded pending-value
overlay, pins retention, and reads disk payloads in sequence order. Index metadata
memory grows with the number of matching revisions, but full historical values
are never collected into a vector. STRIPE coalescing retains revision metadata,
then reconstructs values one at a time. Existing TCP/HTTP/gRPC streaming paths
iterate directly; whole-response protocols still allocate their encoded response,
and JNI still returns the Java array required by its existing Java contract.
Returned history is ordered by logical seq, materializes values rather than
Blob references or metadata bytes, and preserves tombstone/rollback/retention
base flags. Missing required historical bytes throw rather than masquerading
as an absent version.

`getAt(key, uint64_t)` interprets the scalar in that key's logical stream:
PARTITIONED owner stream; shared MIRROR/Raft stream; STRIPE metadata stream.
`getAt(key, Revision)` additionally validates streamId (owner node id for
PARTITIONED, zero otherwise) and is available only with Cluster enabled.
There is no cross-owner global scalar sequence. VersionLog-disabled behavior is
determined at the read authority: missing for getAt, empty for history. Targets
earlier than the retained base remain unavailable under ordinary retention
semantics, not reconstructible from current values. STRIPE public timestamps
come from the metadata leader's local VersionLog append and are not a globally
synchronized event clock; source identity comes from metadata authority (or
ROLLBACK_NODE).

### 13.1 Options

| Field | Default | Meaning |
|---|---:|---|
| `logPath` | derived from data dir | Version log file |
| `syncMode` | `ASYNC` | `SYNC`, `ASYNC`, or `BATCHED_SYNC` |
| `writeAdmission` | `SERIAL` | `SERIAL`, `PREPARE_PARALLEL`, or `PARALLEL` (described below) |
| `serialAppendMode` | `PIPELINED` | `SERIAL` admission only: `PIPELINED` or `WAIT_PREVIOUS_APPEND` |
| `readVisibility` | `COMMIT_ORDER` | `COMMIT_ORDER` exposes only entries whose matching MemTable mutation committed; `APPLIED` exposes an appended entry immediately |
| `recoveryMode` | `EAGER` | `EAGER` validates and rebuilds indexes during open; `BACKGROUND` returns after opening the active file, then VersionLog and AkkEngine data operations wait for validation/index reconstruction |
| `codec` | `NONE` | `NONE` or opt-in `ZSTD` value compression |
| `zstdCompressionLevel` | 1 | Zstd level for newly written VLog records; validated against the linked Zstd range when `codec = ZSTD` |
| `groupN` | 128 | Async/batched entry grouping |
| `groupMicros` | 500 | Async/batched delay threshold |
| `groupBytes` | 1 MiB | Async/batched byte threshold |
| `asyncMaxPendingBytes` | 64 MiB | Pending byte limit |
| `parallelPendingLimitScope` | `PER_LANE` | `PARALLEL` only: apply `asyncMaxPendingBytes` to every lane independently, or once across all lane queues with `GLOBAL` |
| `parallelWriteLanes` | 0 | `PARALLEL` lane-worker count; `0` selects a hardware-derived value clamped to 2 through 8, explicit values support 1 through 64 |
| `segmentBytes` | 64 MiB | Rotate an active stream/lane into numbered sibling segment files at this size; `0` disables rotation |
| `retentionDays` | 0 | Delete closed segments whose last modification is older than this many days; `0` disables the age boundary |
| `retentionMinCommitSeq` | 0 | Delete closed segments whose largest sequence is lower than this sequence; `0` disables the sequence boundary |
| `initialCommittedSeq` | 0 | Initial `COMMIT_ORDER` frontier; `AkkEngine` seeds this from WAL/SST recovery |

### 13.2 Version Entries

Each public `VersionEntry` contains:

- `seq`,
- `sourceNodeId`,
- `timestampNs`,
- `flags`,
- value bytes (always decompressed when VLog compression is enabled).

`ROLLBACK_NODE` is `UINT64_MAX`. `VLOG_FLAG_ROLLBACK` is `0x04`.
`VLOG_FLAG_RETENTION_BASE` is `0x08` and identifies a synthetic state entry
emitted by retention compaction. The internal `VLOG_FLAG_ZSTD` bit (`0x01`)
prefixes the stored compressed payload with its four-byte uncompressed size; it
is stripped before a `VersionEntry` is exposed.

VLog compression is disabled by default. With `codec = ZSTD`, a record is only
stored compressed when the result including its original-size prefix is smaller
than the raw value; reads always return decompressed value bytes. Configure it
directly through `AkkEngineOptions::vlog.codec` and
`AkkEngineOptions::vlog.zstdCompressionLevel`, or at the high-level startup
surface through `AkkaraDB::Options::overrides.versionLogCodec` and
`versionLogZstdCompressionLevel`.

Retention is disabled by default. Set `retentionDays`,
`retentionMinCommitSeq`, or both. The enabled conditions are ORed: a closed
segment is deleted when its file age reaches `retentionDays` or its highest
sequence is lower than `retentionMinCommitSeq`. Retention runs after recovery,
after segment rotation, and on close; it never deletes the active segment.
It also leaves any segment that reaches beyond the current commit frontier in
place until those writes commit.
It is segment-granular, so history can remain slightly longer than the boundary
but is never partially rewritten. Before deleting an expired segment, VersionLog
scans the retained set and writes one `VLOG_FLAG_RETENTION_BASE` record for each
key whose most recent value at the boundary would otherwise disappear. The base
record is durable before deletion, so `getAt`, `history`, and rollback preserve
the state from the effective retained boundary onward without retaining all
older versions. The boundary is the later of `retentionMinCommitSeq` and the
sequence immediately after the highest removed version. VersionLog keeps
expired segments until that boundary is committed, so a base is never invisible
under `COMMIT_ORDER` and never stands before a removed version.

Queries before the base boundary remain unavailable, and `history` reports the
synthetic base as its first retained version with the retention-base flag. This
is an intentional history rebase, not a preservation of the original mutation
sequence.

Retention compaction snapshots the closed-segment set, commit frontier, and a
persisted-generation counter, then reconstructs its base state under a shared
scan lock. Regular reads and writes continue during that scan. It commits only
when the persisted-generation counter is unchanged, taking the exclusive scan
lock only to append durable bases and unlink expired files. A concurrent
persisted write causes a retry; after two unstable attempts, pruning remains
pending for a later append or close rather than deleting from an unsafe snapshot.

The same settings are exposed at the high-level startup surface as
`AkkaraDB::Options::overrides.versionLogRetentionDays` and
`versionLogRetentionMinCommitSeq`.

### 13.3 Disabled Behavior

When VersionLog is disabled:

- `getAt` returns `std::nullopt`,
- `history` returns an empty generator,
- `rollbackTo` and `rollbackKey` throw.

VersionLog is incompatible with `THREAD_LOCAL_RANGES`. Its write admission is
independent from MemTable admission: a parallel MemTable may funnel VersionLog
records through its serial entry, or both components may use parallel entry.

`SERIAL` uses one admission mutex. `PREPARE_PARALLEL` retains the former
parallel behavior: entry serialization and compression occur concurrently, but
one append stream persists records and updates its active index in order.

`serialAppendMode` applies only to `SERIAL`. `PIPELINED` submits VLog work and
lets the same put continue to its MemTable and commit steps. With
`WAIT_PREVIOUS_APPEND`, that put still continues immediately, but the next
serial put waits until the preceding VLog append reaches the log file before it
submits its own VLog record. This is an admission gate between puts; it does not
make the preceding put wait before its MemTable step.

`PARALLEL` is physical write parallelism and requires `syncMode = ASYNC`. Put
calls enqueue a record to a lane worker rather than issuing VLog I/O themselves.
Each worker owns an active segment, selected from the stable key fingerprint,
and serializes that lane's append, active-index update, and rotation. Different
lanes therefore persist and index concurrently, while writes for the same key
remain ordered. `segmentBytes` bounds each lane independently, so the aggregate
active VLog/index budget is approximately the lane count times that threshold.
`parallelPendingLimitScope = PER_LANE` permits up to `asyncMaxPendingBytes` in
each lane. `GLOBAL` applies that value once across all lane queues. Queues reject
a write when their applicable pending-byte limit is reached; they do not turn
ordinary puts into synchronous disk I/O. The durable tail
advances at `forceSync`, lane rotation, or close. Serial ASYNC recovery also
accepts only the valid prefix of the last active segment and truncates any
interrupted trailing entry before reopening it for append. Entries that remain
queued or unsynced at a crash are discarded rather than extending recovery
beyond a durable or validated prefix.

VersionLog does not retain persisted historical values in RAM. Each segment has
a derived `*.akvidx` sidecar containing a Bloom filter and a sorted mapping
from key fingerprint to `(commitSeq, VLog byte offset)` records. `history` uses
the Bloom filter to reject segments that cannot contain its key, then reads only
that key's offsets; `getAt` binary-searches those per-key sequences and reads the
selected record directly. With segmentation enabled, the active segment keeps
only key-to-`(commitSeq, VLog byte offset)` metadata in RAM, bounded by
`segmentBytes`; historical values remain on disk. Setting `segmentBytes = 0` also disables that active
metadata, so reads of a newly appended single-file log safely fall back to a
file scan rather than accumulating unbounded history in RAM. Reads run
concurrently and retain only the async records that have not reached the file as
a small value overlay. Rollback-target collection intentionally builds a
temporary full-history view for the duration of that rollback only.

The VLog segment is authoritative. Index-sidecar regeneration is attempted at
segment rotation, clean close, and recovery. A missing, stale, malformed, or
unwritable sidecar causes a safe scan of just that segment and never makes
valid VLog data unreadable.

Each sidecar has independent header and payload CRC32C validation. A corrupted
Bloom filter or offset directory is therefore rejected before it can produce a
false negative; the VLog segment remains the fallback source of truth.

The base `logPath` is segment `0`; later segments insert `-seg-N` before the
extension (for example, `vlog.akvlog`, `vlog-seg-1.akvlog`). Their derived
indexes replace the VLog extension with `.akvidx` (for example, `vlog.akvidx`
and `vlog-seg-1.akvidx`).
VersionLog writes an `.akvtail` sibling at explicit sync boundaries, segment
rotation, and close. `PARALLEL` keeps one tail for every active lane. It records
the byte length of the durable contiguous VLog prefix. Recovery reads no bytes
beyond that prefix, so a crash cannot turn an interrupted append into a corrupt
VLog tail. When a serial ASYNC active segment has no tail file, recovery scans
only the valid prefix of that final segment and truncates the file to the last
verified offset. The tail is a recovery boundary, not a historical index.
A compact in-memory segment directory records each file's id, minimum sequence,
maximum sequence, and byte size. `getAt` uses those ranges to skip segments
that begin after its requested sequence. The directory is rebuilt during
VersionLog recovery, so no separate manifest can become a source of truth.

With `recoveryMode = BACKGROUND`, `open` returns after the active log file is
opened and validation/index reconstruction continues in a worker. VersionLog
append, visibility, history/rollback, sync, and close operations—and AkkEngine
read or mutation paths—wait for that worker before continuing, so an application
cannot observe or mutate engine state before the recovery boundary completes. A
recovery error (including VLog corruption) is rethrown by that waiting operation;
`EAGER` instead reports it from `open`.

`AkkEngine` appends VersionLog records before applying their MemTable mutation,
then publishes the VersionLog commit after that mutation. Consequently,
`COMMIT_ORDER` history and `getAt` never expose a record ahead of the engine's
VersionLog commit frontier; `APPLIED` is the opt-in lower-latency alternative.

### 13.4 Rollback

Rollback is a compensating write, not a sequence rewind. It resolves the value
or tombstone visible at the target revision, submits that state through the
normal write coordinator, and appends a new mutation at the current head.
Rollback mutations use `ROLLBACK_NODE` and `VLOG_FLAG_ROLLBACK`; cluster
rollback therefore follows the same ownership, acknowledgement, replication,
Raft, fencing, and durability rules as an ordinary write.

The scalar `rollbackTo(uint64_t)` and `rollbackKey(key, uint64_t)` overloads are
first-class non-cluster APIs and throw when the cluster runtime is enabled.
Cluster callers use `Revision{streamId, seq}` and `ClusterCheckpoint` so a
partition-owner sequence cannot be mistaken for a global sequence:

The existing JNI, HTTP, TCP, and gRPC rollback operations carry only a scalar
sequence and therefore remain non-cluster endpoints. Distributed checkpoint
and rollback are exposed by the Native C++ API until those wire contracts gain
an explicit revision/checkpoint representation.

| Mode | Revision stream |
|---|---|
| non-cluster | scalar local storage sequence |
| `MIRROR` and data `RAFT_QUORUM` | stream `0`, the leader's replicated mutation sequence |
| `PARTITIONED` with `PRIMARY_ACK`/`ASYNC` | one stream per owner; `streamId` is the owner node id |
| `PARTITIONED` with `RAFT_QUORUM` | stable partition stream; `streamId` is the zero-based partition index plus one |
| `STRIPE` | stream `0`, the metadata-Raft state-machine sequence |

`createClusterCheckpoint()` returns the cluster id, configuration epoch,
timeline id, and one watermark per required stream. A checkpoint is accepted
only by the same cluster identity, epoch, and timeline, must contain exactly the
expected streams, and cannot name a future watermark. PARTITIONED checkpoint
collection is a vector cut rather than a cross-partition transaction.
PARTITIONED Raft checkpoints remain valid across holder-count changes because
the logical partition streams do not change; their placement epoch is informational.
`revisionStreamId(key)` returns the current public stream identity. STRIPE
watermarks pass a linearizable metadata-Raft read barrier before the applied
state-machine sequence is captured.

VersionLog append is asynchronous by default, so checkpoint and rollback
control take the mutation epoch exclusively and force the VersionLog queue to a
durable visibility boundary before capturing or scanning a watermark. This
prevents a committed owner mutation from being visible in storage sequence
state but missing from rollback history.

`RollbackOptions::execution` selects:

| Value | Behavior |
|---|---|
| `IMMEDIATE` | Execute now and do not create a retry journal entry |
| `IMMEDIATE_AND_DEFER_FAILED` | Durably journal before attempting; remove the entry after a completed request and retain transport/availability failures for replay after restart |
| `NEXT_STARTUP` | Durably journal without executing in the current process; replay in a background startup worker after the next open |

The startup worker begins after local recovery and cluster startup but before
the optional API server is started. Cluster peers may still be unavailable, so
it retries pending transport failures once per second. `NEXT_STARTUP` is thus
eventually applied after the next open rather than blocking `open` until every
peer is reachable. The CRC32C-protected `AKRJ1` journal is replaced atomically
and records task/operation ids, the scheduling startup id, cluster identity and
epoch, action, target stream/sequence, planning watermark, conflict policy, and
optional key. Reopening with a journal from another cluster mode, id, or epoch
is rejected rather than applying it to a different topology.

`RollbackOptions::conflict` defaults to `FAIL_IF_CHANGED`. Deferred work records
the planning watermark; a key whose head advanced beyond it reports `CONFLICT`
and is not overwritten. `OVERWRITE_LATEST` deliberately applies the historical
state over a newer head. History removed by configured retention or a STRIPE
metadata snapshot base can produce `PERMANENT_FAILURE`; rollback does not infer
missing historical state. Operators must retain VersionLog history and STRIPE
generations through the oldest checkpoint or deferred target they intend to
use. Every owner/authority that may execute a distributed rollback must have
VersionLog enabled; a mixed cluster fails the control request instead of silently
rolling back only the nodes with history.

`RollbackResult` contains a random operation id and per-key `APPLIED`,
`DEFERRED`, `CONFLICT`, or `PERMANENT_FAILURE` results. `complete()` is true only
when every returned item is applied. Distributed stream rollback processes keys
in deterministic lexical pages of at most 256 keys, carrying a continuation
cursor in the cluster control response so no result frame grows without
bound. Distributed rollback is not an atomic
multi-stream transaction: all current stream watermarks are planned before
execution, but a later transport failure can follow successful mutations on an
earlier stream. Use `IMMEDIATE_AND_DEFER_FAILED` when that retry behavior is
required. Exact physical database restore is a separate offline generation
replacement operation, not online rollback.

VLog entry corruption is never accepted. `EAGER` recovery fails `open`; in
`BACKGROUND` mode the first operation that crosses the recovery barrier throws
the stored recovery error.

### 13.5 Operational Runbook

VersionLog health is primarily observed through `AkkEngine::stats().vlog`.
Operators should treat the following counters as signals rather than as exact
transactional measurements:

| Signal | Meaning | Operational response |
|---|---|---|
| `recoveryDurationMicros` | Time spent in the latest VersionLog recovery pass | Track startup regression and compare with segment count and entry count |
| `recoveredSegmentCount` / `recoveredEntryCount` | Number of segments and entries accepted by recovery | Unexpected drops indicate retention, truncation by durable tail, or failed recovery |
| `segmentCount` / `activeSegmentBytes` | Current segment fan-out and active segment size | Tune `segmentBytes` and retention when history storage grows too quickly |
| `sidecarFallbackCount` | Queries that could not use a derived `.akvidx` and scanned the authoritative segment | Investigate repeated growth; missing or corrupt sidecars are safe but slower |
| `sidecarRebuildFailures` | Best-effort sidecar writes that failed at rotation, recovery, close, or retention-base creation | Check filesystem errors, permissions, full disks, and antivirus/file-lock interference |
| `retentionPrunedSegments` | Closed segments successfully deleted by retention | Confirms retention is making progress |
| `retentionBaseEntriesWritten` | Synthetic base records written before segment deletion | Should move with pruning; very high values mean many live keys cross the retention boundary |
| `parallelLaneCount` | Active `PARALLEL` lane workers | Confirms the resolved lane count after hardware/default normalization |
| `parallelPendingWrites` / `parallelPendingBytes` | Queued lane work not yet persisted | Sustained growth means storage cannot keep up or `asyncMaxPendingBytes` is too small |
| `parallelQueueRejects` | Writes rejected because the parallel pending-byte limit was reached | Alert on any sustained non-zero increase in production workloads |

For recovery failures, the thrown `VersionLog:` error includes path context and,
when available, the byte offset and sequence number. The segment file remains
the source of truth; deleting or rewriting `.akvidx` sidecars is safe because
they are derived. Do not delete `.akvlog` or `.akvtail` files unless the data
set is intentionally being discarded or restored from a backup.

Recommended operating checks:

- Record the VersionLog stats snapshot at process start after recovery
  completes.
- Alert when `sidecarFallbackCount` or `sidecarRebuildFailures` increases
  repeatedly, because reads remain correct but may degrade to scans.
- Alert when `parallelQueueRejects` increases; this means the configured
  admission limit is actively shedding writes.
- Watch `parallelPendingBytes` against `asyncMaxPendingBytes` for backpressure
  headroom.
- Watch `segmentCount`, `durableBytes`, and `retentionPrunedSegments` together
  to verify that history retention is reducing closed-segment storage.
- Preserve `.akvlog`, `.akvtail`, and engine WAL/SST/manifest files together
  when taking filesystem-level backups.

The `akkaradb_vlog_tool` maintenance executable can validate a VersionLog path
and rebuild derived sidecar indexes:

```text
akkaradb_vlog_tool validate --log <path> [--json]
akkaradb_vlog_tool rebuild-indexes --log <path> [--json]
```

Both commands open the log through normal eager recovery, so they validate
authoritative segments and may rewrite `.akvidx` sidecars. The tool refuses to
create a new empty log when neither the base path nor a sibling segment exists.
Exit code `0` means recovery completed; exit code `1` means validation/open
failed; `rebuild-indexes` returns `2` when recovery completed but sidecar
regeneration reported failures.

## 14. Generation Layout

Generation layout isolates mutable engine state under an active generation
directory. It is disabled by default for existing directory compatibility.

The layout manager uses:

- root pointer: `current.akgen`,
- active generations under `generations/`,
- uncommitted generation work under `staging/`.

The first empty-root generation is `generations/gen-1`. Activation is explicit;
staging state is not treated as active by directory presence alone.

This feature is a storage-layout contract. It does not itself imply online
migration from legacy flat directories.

## 15. Cluster and Replication

Cluster configuration is persisted separately from runtime transport options.
The config file uses `AKC1` magic and version 1.

### 15.1 Persistent Cluster Config

`ClusterConfig` stores:

- node ids and advertised host/data/repl ports plus the dedicated STRIPE
  metadata-Raft port,
- node capability flags,
- replication/placement mode,
- acknowledgement policy,
- write consistency controls,
- replica lag behavior,
- Raft membership options,
- stripe data/parity shard counts,
- PARTITIONED total copy count (`PartitionOptions::replicationFactor`, default 3),
- the single fixed Primary node id for non-Raft `MIRROR`,
- a nonzero random 128-bit cluster id.

Constructing a new config generates a new cluster id. Every node in one cluster
must therefore load or receive the same persisted config rather than constructing
independent equivalent configs. The id remains stable across save/load and is not
a node id or a Raft term.

For non-Raft `MIRROR`, `ClusterConfig::primaryNodeId()` must identify a
configured node with both `COORDINATOR_ELIGIBLE` and `DATA_BEARING`. If the
constructor's `primaryNodeId` argument is zero or omitted, it selects the first
node in configuration order with both capabilities. All other placements,
including data-consensus groups, require this field to be zero; it is not a list of
per-key owners or a persisted Raft election result.

Runtime-only values such as bind host, replica-side primary endpoint overrides,
identity seed path, and peer key pins are not serialized into `ClusterConfig`.

Configuration validation rejects zero replication ports in clustered placement
or Raft mode and duplicate advertised replication endpoints. Every data-bearing
STRIPE node additionally requires a nonzero `stripeMetadataPort`; metadata-Raft
endpoints must be unique and cannot alias a normal replication endpoint.
Endpoint comparison folds ASCII host-name case and a trailing DNS dot; it does
not resolve DNS aliases. Hosts cannot contain whitespace or control characters.
Non-Raft STANDALONE may leave its unused replication port zero.

`ClusterConfig::validateRuntime(selfNodeId, options)` additionally validates
transport selection, membership of the local node, and SECURE peer pins before
runtime creation. Pins must have unique nonzero node ids. Non-Raft MIRROR replicas
need their fixed Primary's pin; the Primary needs every data replica's pin.
PARTITIONED, STRIPE, and Raft require pins for all other data peers. Additional pins
for future Raft members are permitted. Raft also validates recovered membership
before opening its listener and validates proposed voters before adding them.
AkkEngine performs the initial-config check before
storage recovery and cluster identity/membership initialization. Low-level
replication endpoint factories also reject invalid ports and missing expected
peer pins before creating an identity or starting connection retries.

### 15.2 Placement Modes

| Mode | Meaning |
|---|---|
| `STANDALONE` | No replication runtime |
| `MIRROR` | Ship writes to every data-bearing node |
| `PARTITIONED` | Store each key on the highest-ranked N holders, first holder owns mutations |
| `STRIPE` | Split data and optional parity shards across distinct data-bearing nodes |

`RaidOptions{.preset = RaidPreset::RAID0, .dataShards = k}` is the public
`RAID.0` preset. Construction immediately normalizes it to `STRIPE` with `k`
data shards and zero parity shards; only those canonical fields are persisted.
`k` must be at least two. Loading the canonical fields derives `RAID.0` again
through `ClusterConfig::raidPreset()` without storing parallel preset state.

`RaidOptions{.preset = RaidPreset::RAID1, .primaryNodeId = id}` is the public
`RAID.1` preset. It normalizes to fixed-Primary `MIRROR`, durable
`ALL_TARGETS` acknowledgements, `AVAILABLE_REPLICAS` write consistency,
`FAIL_WRITE`, and asynchronous replica resynchronization. Every configured node
must be data-bearing and at least two nodes are required. A zero Primary id
selects the first coordinator-eligible data node. RAID.1 requires WAL storage.
While every replica is connected, a write completes only after every replica
has applied and forced the record durable. With one or more replicas offline,
the Primary records and syncs the write locally and remains writable in
`DEGRADED`; the write is not reported as redundantly stored. A returning replica
is excluded from live acknowledgements until WAL catch-up or snapshot rebuild
finishes. A live replica that misses the acknowledgement deadline is stopped and
removed from that live set, allowing the already durable Primary-local write to
complete. The Primary reports `REBUILDING` during catch-up and returns to
`HEALTHY` only after every configured replica is live. Primary replacement is
never automatic and uses the same explicit offline MIRROR promotion contract.
Loading these canonical fields derives `RAID.1` without a parallel preset flag.

`RaidOptions{.preset = RaidPreset::RAID5, .dataShards = k}` is the public
`RAID.5` preset. It normalizes to `STRIPE` with `k` data shards and one parity
shard. `k` must be at least two, so at least three data-bearing nodes are
required. One missing node may be tolerated by reads and `DATA_SHARDS` writes.
Loading the canonical fields derives `RAID.5` without persisted preset state.

`RaidOptions{.preset = RaidPreset::RAID6, .dataShards = k}` is the public
`RAID.6` preset. It normalizes to `STRIPE` with `k` data shards and two parity
shards. `k` must be at least two. At least five data-bearing nodes are required
even for a `2 + 2` shard layout so the metadata-Raft group retains a three-node
majority after any two voters fail. Two missing nodes may be tolerated by reads
and `DATA_SHARDS` writes. Loading the canonical fields derives `RAID.6` without
persisted preset state.

`RaidOptions{.preset = RaidPreset::RAID10, .dataShards = k}` is the public
`RAID.10` preset. It normalizes to `STRIPE` with `k` logical data shards, zero
parity shards, and two copies per shard. `k` must be at least two and exactly
`2 * k` data-bearing nodes are required. Adjacent nodes in configured membership
order form fixed mirror pairs; rendezvous ranking assigns those pairs to logical
shards per key and ranks the two members within each pair. Loading the canonical
fields derives `RAID.10` without persisted preset state.
`RaidOptions::hotSpareNodeId` may designate one additional coordinator-eligible,
data-bearing node as a shared hot spare. Zero disables this option. The preset
normalizes the selected node to `STRIPE_FAILOVER_ELIGIBLE`; the capability is
persisted with membership, while the convenience option itself is not stored.
The spare participates in metadata Raft but is excluded from normal shard pairs.

The active native cluster runtime accepts `MIRROR`, `PARTITIONED`, and
`STRIPE`. Non-Raft `MIRROR` has exactly one Primary selected by the persistent
config; only that node may accept writes or start with the `PRIMARY` role.
`RAFT_QUORUM` does not store a fixed Primary and instead elects one leader.
Every `PARTITIONED` and `STRIPE` node must be configured with the same explicit,
nonzero `ClusterRuntimeOptions::clusterGroupId`. Zero is rejected because these
multi-owner placements cannot independently generate one shared identity.
Non-Raft `MIRROR` retains its persisted Primary-generated default when the value
is zero.
Static `PARTITIONED` with `PRIMARY_ACK` or `ASYNC` and no data consensus runs every data-bearing node as a partition owner for
its rendezvous-hash key range. Mutations are applied only by the owner;
`routingMode` determines whether a non-owner rejects, redirects, or forwards. Owner
mutations are replicated only to the remaining selected holders, and remote entries are
assigned local storage sequence numbers on receipt so independent owners do not
collide in the local MemTable/WAL sequence space. Consequently, one owner's
replication sequence can contain gaps where that owner applied another owner's
mutation; receivers accept forward gaps while rejecting replayed entries.
Placement scores use the first eight bytes (little-endian) of BLAKE2b-256 over
length-delimited `AKHRW1`, the binary key, and little-endian node identities.
PARTITIONED and STRIPE use the same node scores; RAID.10 hashes both sorted
members of each fixed mirror pair rather than collapsing the pair to a synthetic
node id. Scores sort descending, with smaller node ids breaking ties.
`PartitionOptions::replicationFactor` is the total number of copies including
the owner, defaults to 3, and must be nonzero. The effective count is capped by
the configured data-node count. Thus a two-node cluster defaults to two copies.
`ClusterRouter::writeTargets()` and `readCandidates()` return all selected holders,
owner first. Only the owner accepts mutations in static replication; consensus groups use their current leader.
Live replication, catch-up, snapshots, receiver validation, and ACK accounting
use the same holder set. `ALL_CONFIGURED` requires every selected replica,
including offline holders; offline non-holders do not delay a write. Quorum
counts refer to replicas excluding the owner and cannot exceed that key's replicas.
With one copy, `ALL_TARGETS`/`ALL_CONFIGURED` complete locally; a requested replica
quorum or `ONE_REPLICA` is invalid.

Completed owner snapshots are staged within the shared transfer budget, then installed as
a WAL snapshot transaction using a local storage sequence. Only that owner's
selected keys are replaced; keys of other owners remain intact. Interrupted
receives do not advance persisted peer progress. Deletes absent from the snapshot
are applied only within that owner's scope.

The BLAKE2b placement contract changes owner/shard locations from earlier development
builds. Upgrade all nodes together, recreate and redistribute the config, and
recreate PARTITIONED/STRIPE data. No migration, legacy placement, or dedicated
old-placement error is provided. Existing data is never deleted automatically.

PARTITIONED requires WAL storage. A receiver forces an applied mutation durable
before atomically advancing its persisted per-peer replication progress, so
recovery never skips data whose progress alone reached stable storage.

`ClusterRuntimeOptions::readMode` controls point-read routing for native
placement. Owner reads use the routing policy below; Raft placement executes
`OWNER_LINEARIZABLE` on the current leader after a fresh quorum ReadIndex
confirmation and local application through the captured commit index:

| Value | Meaning |
|---|---|
| `LOCAL_STALE_OK` | Serve reads from local storage without freshness coordination |
| `OWNER_ONLY` | Serve only keys owned by the local node; remote-owner reads fail |
| `OWNER_LINEARIZABLE` | Require the current owner, redirect/forward according to `routingMode`; Raft leaders confirm a fresh quorum ReadIndex before reading locally |

#### Native client routing

`ClusterRuntimeOptions::routingMode` is runtime-only (not serialized in AKC1).

| Value | Public Native behavior on a non-owner data node |
|---|---|
| `LOCAL_ONLY` | Throw `ClusterRoutingError` with `LOCAL_ONLY`; no network forwarding or redirect hint |
| `REDIRECT` (default) | Throw `NOT_OWNER` with a structured `ClusterRouteTarget` |
| `FORWARD` | Send one peer RPC using the configured transport to the owner and return its result |

The target contains `nodeId`, `host`, `tcpPort`, `httpPort`, `grpcPort`,
`replPort`, `configurationEpoch` (non-Raft group epoch; zero for data Raft), and
`raftTerm` (zero outside data Raft).
`NodeInfo::dataPort` supplies the default TCP port. Configure runtime
`apiEndpoints` on the entry nodes to advertise protocol-specific public ports;
zero means unspecified, not a guessed HTTP/gRPC endpoint. Unknown/duplicate
endpoint node ids and invalid modes/timeouts are rejected at startup.

MIRROR routes to the **data Primary**, never its separate authority leader.
Data Raft routes to the current data leader, including when the entry node is a
learner. PARTITIONED routes by key. STRIPE uses its owner/failover eligibility,
then executes the ordinary fenced metadata/shard write coordinator at that
destination. Authority-only MIRROR witnesses advertise redirects but do not
host data forwarding connections. No forwarding path bypasses WriteCoordinator,
ownership checks, ReadIndex, or MIRROR/STRIPE fencing.

Forwarding covers put/remove (including hinted forms), same-owner protocol bulk writes,
owner-coordinated point reads (get/getInto/getIntoArena/getBatch/exists), and
Raft putWithRequest/removeWithRequest/queryRequest. Hints are recomputed at the
destination. `LOCAL_STALE_OK` stays node-local for non-STRIPE reads; `OWNER_ONLY`
never forwards. STRIPE `LOCAL_COORDINATOR` retains its explicit local gather and
authoritative metadata validation. Read batches may route keys independently
and are not a cluster-wide snapshot. A write batch spanning destinations throws
`CROSS_OWNER_BATCH` **before any application**; it is never silently split.
Co-located batches retain their existing completion/atomicity contract.

`forwardingTimeoutMs` defaults to 5000 (valid range 1-300000). One monotonic
budget bounds admission, send, and response wait; the destination subtracts its
queue delay before execution. Platform DNS resolution is synchronous and may
delay the reported timeout; the remaining budget is rechecked before submission,
so a resolver delay never permits a new mutation after its deadline.
A destination never forwards again: stale routing
returns another structured error to the caller, so internal loops are impossible.
Timeout is not cancellation: an admitted mutation may commit after the caller's
deadline. No forwarded mutation is automatically replayed. `NO_TARGET` means no
writable leader/owner was known, `FORWARD_UNAVAILABLE` means no operation was
admitted for application at the destination (or the RPC was never sent), and
`OUTCOME_UNKNOWN` means application/commit may have occurred.
Resolve unknown outcomes with the original Raft request identity and query/retry
APIs when enabled; ordinary writes have no implicit exactly-once guarantee.
SECURE forwarding inherits peer identity pins and cluster membership checks;
explicit PLAIN transport retains its trusted-network-only contract.

Forward request/response payloads are bounded to 4 MiB (including framing fields)
on both data transports. Larger remote operations return `PAYLOAD_TOO_LARGE`
before submission (or after a read whose value cannot fit); connect directly to
the advertised owner for larger values. Up to 64 outstanding forward requests
per non-Raft peer connection are identified independently. Streaming large
forwarded values and transparent client-side redirect following are future work.
Raft forwarding uses independent RPC channels, with `raftMaxForwardRequests`
limiting active channels per runtime (default 64, range 1–256). Waiting for a
channel consumes the caller's original deadline. Consensus traffic uses separate
connections so forwarded administration cannot block its own replication.
One additional channel is reserved for leaf placement-state reads needed by
forwarding callbacks, allowing reconfiguration even when the ordinary limit is 1.
Forward frames use AKR1 types 0x24/0x25; mixed development builds are unsupported.
Native callers and optional runtime/API backends must be rebuilt together for
the changed C++ ABI. Online placement changes persist the committed AKC1 configuration.

API backends preserve the same JSON error object: TCP returns
`ApiStatus::ROUTING_ERROR` (0x02) without closing the connection or treating it as
a protocol violation; HTTP returns 409 (routing/local-only/cross-owner), 503
(no target/unsubmitted forwarding failure), 413 (payload limit), or 500
(unknown outcome), with `application/json`. HTTP does not issue an automatic
307 replay. gRPC returns FAILED_PRECONDITION, or UNKNOWN for ambiguous outcomes,
with the JSON object in `Status::error_details()`. Clients must inspect the
outcome field before considering retries.

Administrative operations remain explicit: maintenance runs on the connected
node. Native placement reconfiguration follows `routingMode` and may forward to
the placement authority; other membership/leadership calls require their local
authority. Distributed rollback uses its checkpoint/control protocol.

Distributed aggregate/range/history reads use READ_QUERY over the authenticated
node-to-node forwarding transport, with bounded ephemeral cursor pages. See 8.3
and 13 for authority selection, read-mode behavior, logical history streams,
resource limits and distributed-cut semantics. They do not proxy administrative
operations or replay mutations. Existing API/JNI scalar historical callers keep
working with key-stream seq semantics; native callers may use Revision for
explicit stream validation. All native core/backend binaries must be rebuilt
together (runtime interface and query operation added). Current pre-release format markers remain version 1. All consumers must use
the new layout; no backward readers or migration layer are provided.

`STRIPE` stores public values as internal metadata plus Reed-Solomon data shards
and optional parity shard records. The first shard target is the key owner.
Non-owner public writes follow `routingMode`; a configured STRIPE failover
candidate may accept them while that key's owner is unreachable, still subject
to the existing metadata authority fencing. Reads reconstruct
the value from the latest authoritative metadata and at least `dataShards`
available shards.
For `RAID.0`, every data shard is required: one missing or corrupt shard makes
the value unavailable, and read repair is disabled because no reconstruction
information exists. Writes still use the normal all-shard durable publication
protocol even when `DATA_SHARDS` is selected, so a generation is never
published after placing only a subset.

`ClusterRuntimeOptions::stripeWriteCommitMode` controls STRIPE write completion:

| Value | Meaning |
|---|---|
| `ALL_SHARDS` | Durably place every data/parity shard, then atomically publish durable owner metadata |
| `DATA_SHARDS` | With parity, publish after any `dataShards` shards are durable; with RAID.10, require one durable copy of every logical shard; record missing positions for rebuild |

`DATA_SHARDS` is the default. It permits degraded writes through as many node
failures as `parityShards`: RAID5-style layouts continue with one missing node
and RAID6-style layouts continue with up to two. Publication is rejected when
fewer than `dataShards` new-generation shards are durable. A successful degraded
write sets engine health to `REBUILDING`; disconnected peers remain
`DEGRADED` at the transport layer. `ALL_SHARDS` remains available for
deployments which prefer rejecting every incomplete generation.
For `RAID.10`, `DATA_SHARDS` permits a write while at least one member of every
mirror pair is reachable. Losing both members of any one pair makes the value
unavailable. A returning member is rebuilt from its surviving mirror without
Reed-Solomon decoding.

`ClusterRuntimeOptions::stripeReadCoordinatorMode` controls where STRIPE reads
are assembled:

| Value | Meaning |
|---|---|
| `OWNER` | Route public reads to the current authority; it gathers shards |
| `LOCAL_COORDINATOR` | Fetch authoritative metadata, gather shards, and revalidate the generation before returning |

STRIPE requires WAL storage. Each mutation first persists an authority-local
`AKST1` intent payload under the internal `0x00 || "AKST1"` key prefix, using a
reserved storage sequence which is separate from its generation/fencing token.
Only after that intent is durable may immutable generation-qualified shards be
sent. Targeted shard operations always require `ALL_TARGETS`/`DURABLE`
acknowledgement with `FAIL_WRITE` on timeout, including when the configured
general replication policy is `ASYNC` or `NONE`.
The authority then asks the current STRIPE metadata-Raft leader to publish one
WAL-backed `AKSM1` value under the internal `0x00 || "AKSM1" || publicKey` key,
without a key-length prefix. Publication succeeds only after the metadata entry is
durably committed by a majority of the configured data-bearing voters and
applied locally. This quorum commit is the sole metadata commit point; an
uncommitted local record is never a source of authority. Metadata reads use a
Raft read barrier and therefore also require a live majority.
Deletes use the same intent/publication protocol with tombstone metadata.
An ambiguous owner-local persistence failure stops STRIPE reads/writes until
the engine is reopened.

Within an engine, STRIPE read/write/repair/garbage-collection operations serialize
only for the same binary public key. Shard reads and mutation sends use eight
workers with at most 64 queued tasks per engine. Completion waits for all submitted
tasks, including on failure, before releasing the generation's key guard.
Range-query materialization pins captured generations against garbage collection;
other keys remain free to publish newer generations. The metadata authority keeps
only short shared lease-map locks. During quorum publication, a per-key reservation
continues to fence competing operations and owner/failover handoff across term and
connection changes. Control RPCs correlate responses by request id with at most
64 pending requests per connection and four authority workers/64 queued requests
per server. Targeted shard ACKs must match the individual request id; a higher id
is not evidence that an earlier request has completed.
Rollback control RPCs retain separate per-connection serialization for their
shared rollback worker without blocking metadata control RPCs.

STRIPE metadata Raft is a second logical Raft group which reuses the native Raft
implementation but has its own listener (`NodeInfo::stripeMetadataPort`), hard
state, segmented log, snapshot stream, and `stripe-metadata-raft` directory.
Only public-key/`AKSM1` metadata enters this group; value shards remain on the
normal STRIPE transport. Every data-bearing STRIPE node is a static voter. The
majority rule is therefore the same as ordinary Raft: a three-node group
continues after one voter fails and a five-node group after two; a two-node
group cannot continue after either voter fails. The current leader is elected
automatically and may run on any data-bearing node. A metadata request waits
through the election window independently of the shorter shard-acknowledgement
timeout. `EngineStats::ClusterStats` exposes the metadata term, leader,
commit/applied indexes, and snapshot index.

Multi-failure STRIPE layouts with two or more parity shards require at least
`2 * parityShards + 1` data-bearing metadata voters. This keeps metadata quorum
available through the same number of node failures as the encoded value. In
particular, a RAID6-style two-parity layout requires at least five data-bearing
nodes; a four-node `2 + 2` layout is rejected even though its value shards can be
encoded, because two failures would leave metadata Raft without a majority.
RAID.10 has no equivalent multi-failure metadata-quorum guarantee: its minimum
four-voter group continues through one voter failure, but two pair-distinct node
failures can leave every value shard available while metadata reads and writes
are blocked by loss of majority.
A five-voter RAID.10 layout with one hot spare retains metadata quorum through
two node failures, although one spare can replace only one missing placement per
value generation.

Targeted STRIPE operations use per-target request ids allocated before sending;
a failed attempt does not allow its id to be reused for a different operation
within that runtime. Receivers apply these idempotent shard-key operations
without ordered-log gap or duplicate suppression. Reconnect does not use a
persisted targeted-stream watermark or ordered catch-up replay. Storage sequence
numbers, generation ids, and transport request ids are separate concerns.

The current authority serializes publication, authority-side reads/repair, and garbage collection.
Durable intents are recovered at startup and retired in the background: an
unpublished generation or superseded generation is deleted on every shard node
before its intent is removed. Unreachable shard nodes leave cleanup pending for
retry. A local coordinator racing generation retirement revalidates against the
owner and retries rather than returning a partially reconstructed generation.
The current published generation is never collected.

Published STRIPE metadata values use `AKSM1` and durably record tombstone and
rollback flags, generation and fencing fields, the metadata logical sequence,
and the placement and observed presence of every generation-qualified shard.
For RAID.10,
the placement count is twice the logical shard count and adjacent placement
entries are the two copies of one logical shard. The copy count is derived from
those counts rather than stored as a second metadata field. The current metadata-Raft
leader runs a bounded background scrub/rebuild loop. It may write a repaired
shard on behalf of the logical owner, but the receiver still validates its shard
index against the normal or owner-excluded placement. Presence updates are
compare-and-set metadata mutations committed through the same Raft group. The
loop scans at most
`stripeRebuildBatchKeys` metadata records every `stripeRebuildIntervalMs`,
verifies actual shard records, reconstructs missing shards once their target
nodes are reachable, and durably updates the presence record. This requires no
foreground read and resumes from published metadata after process restart.
`stripeAutoRebuild` may disable the worker without disabling read repair.
`EngineStats::stripeRebuild` exposes cycles, scanned keys, attempts, successes,
failures, last failed node, and current rebuild activity. RAID.0 never enters
this reconstruction path; RAID.10 copies the surviving mirror shard.

`STRIPE_FAILOVER_ELIGIBLE` enables owner failover. At most one configured node
may have this capability; it must also be `COORDINATOR_ELIGIBLE` and
`DATA_BEARING`. Enabling it requires at least one data-bearing node more than the
configured shard-placement count, because a failover generation excludes the
failed owner and places its shard on a spare. In RAID.10 this capability denotes
the sole shared hot spare. A disconnected placement node is replaced during the
background rebuild by copying its surviving mirror to the spare, then committing
the changed effective placement in `AKSM1`. If the unavailable node is the key
owner, the same spare obtains the fenced failover lease and writes the new
generation directly into that replacement placement.

RAID.10 does not automatically fail back. Once `AKSM1` names the spare for a
placement, subsequent generations preserve that placement even if the original
node reconnects. Reusing the original node requires an explicit
reconfigureCluster placement change. Only one hot spare may be configured; a second
simultaneous placement failure remains degraded and is not remapped onto the
already active spare.

Every owner/failover read or write first acquires a per-key authority lease from
the current metadata-Raft leader. The lease carries an unpredictable nonzero
fencing token, which is also the immutable generation id. Only the holder may
publish metadata with that token. Leases are leader-term-local: a term change
clears them, the old leader cannot commit without a majority, and the new leader
rejects every token issued in an earlier term. Shards written with a rejected
token remain unreferenced and cannot become visible.

Remote leases also belong to the specific accepted replication connection that
acquired them. Before processing authority requests, the metadata leader revokes
all leases from disconnected connections, including leases on other keys. A
reconnection or process restart cannot revive the old token. A fresh acquisition
by the same owner or failover holder fences its previous grant, including a grant
whose response was lost on a live connection. Native engines serialize their
STRIPE operations; callers of the low-level API must respect that contract.
Commit and reacquisition are serialized by the authority lock, so a superseded
token cannot publish after a new grant.

`STRIPE_FAILOVER_ELIGIBLE` does not select the metadata leader. It selects only
the one optional data-operation authority that may replace an unreachable key
owner. If an owner connection disappears, its outstanding lease is revoked
before that candidate may acquire the key. Loss of the failover candidate blocks
owner replacement, but does not block ordinary owner operations while the
metadata-Raft majority remains available. Loss of the metadata majority blocks
authoritative metadata reads and writes. The normal peer connection carries
`STRIPE_CONTROL_REQUEST` (`0x22`) and `STRIPE_CONTROL_RESPONSE` (`0x23`);
Raft replication itself uses the dedicated metadata endpoints. Mixed-version
rolling operation is not supported.

On owner return, the metadata leader stops granting new failover leases. The returning
owner receives `BUSY` while a live failover read/write still holds any lease for
that owner's keys and waits until those operations commit or release their leases;
the next lease is then granted
to the original owner with a new fencing token. The candidate behaves as an
ordinary data-bearing node when it does not hold a failover lease.
`AKSM1` and `AKST1` are the current pre-release metadata and intent payload
formats. The metadata key namespace is `0x00 || "AKSM1" || publicKey`, ordered
by the public key's raw bytes. Intent keys use `AKST1` and shard keys use `AKSS1`.
Readers validate the current format and report ordinary metadata read errors
for malformed input. There is no historical-format detection, backward reader,
migration, or automatic data deletion. All STRIPE nodes must use the same build.

`ClusterRuntimeOptions::stripeReadRepair` enables owner-side best-effort repair
of missing shards when an owner read can reconstruct the value.
`EngineStats::stripeReadRepair` reports engine-instance-lifetime `attempts`, `succeeded`,
`failed`, and `lastFailureNodeId` (zero before any failure). Each missing shard
is one attempt; failure includes reconstruction or shard-write failure and never
turns a reconstructable read into a failed read. Success means the repaired shard
write completed with the normal durability acknowledgement. Statistics are
thread-safe, not persisted, and expose no keys or values. Disabled repair and
non-owner reads do not increment them.

`reconfigureCluster(config)` supports MIRROR data-consensus membership
replacement, PARTITIONED data-consensus node addition/removal and copy-count
changes, and STRIPE node/placement/hot-spare replacement. Replication mode,
cluster identity, partition count, existing node endpoints, consensus policy,
and stripe geometry stay fixed. Geometry changes require a separate rebuild.
PARTITIONED holder changes require `JOINT_CONSENSUS`,
`allowOnlineVoterChanges`, and `allowLearners`.

PARTITIONED consensus hashes the length-delimited `AKPT1` domain and key with
BLAKE2b-256, takes the first four digest bytes as a little-endian integer modulo
`partitionCount`, and rendezvous-ranks holders with the partition's two-byte
little-endian index. Each partition owns storage and a consensus journal under
`partitions/<index>`. Its stable revision stream is index + 1. A surviving
voting majority elects its replacement leader when automatic failover is enabled,
without changing retained logical revisions. One copy has no failure redundancy.
`partitionCount` defaults to 16 and is bounded by 256. Each data node supplies
`partitionReplBasePort`, reserving `partitionCount + 1` consecutive ports for
data groups and placement authority. All endpoints use the chosen PLAIN/SECURE
transport.

The placement authority durably commits the complete target configuration
before transfer. New PARTITIONED holders receive a retained-history snapshot and its journal suffix before
promotion and joint consensus; only after every group finishes membership does
the authority activate placement. New nodes start with the active configuration
plus themselves as `RAFT_LEARNER` (PARTITIONED) or `PLACEMENT_STANDBY` (STRIPE).
The target configuration removes that bootstrap capability to activate them.
SECURE deployments must provision their identity pins on every participant
before admission; extra pins for future members are allowed. Removed nodes'
storage is retained, and they may be stopped after activation.
An already unavailable STRIPE node can be removed when surviving shards allow
reconstruction and the old/new metadata memberships can commit the transition;
every node retained in the target placement must be reachable for transfer.

STRIPE's default `runtime.reconfiguration.stripeMigrationMode = FREEZE`
waits for existing fenced operations to finish, freezes new operations
which need leases, and copies every retained metadata revision and its shards
before atomically activating placement aliases. Reads through a local coordinator
can continue against the active metadata. Owner reads, writes, and rollback need
leases and wait or fail according to write admission policy. Thus STRIPE migration
can pause these operations for the duration of transfer. Original logical
sequence, generation token, writer id, timestamp and tombstone stay unchanged.
`LIVE_COPY` keeps the old placement serving owner reads, writes and rollback
during bulk copying. It then commits a lease freeze, drains admitted operations,
copies newly published revisions, and activates the target. Already copied
revisions have durable plan-scoped checkpoints and are not transferred again.
Aliases use `0x00 || "AKSH2" || placementGeneration:u64le || logicalSeq:u64le || publicKey`, with a placement
generation followed by current `AKSM1` metadata. Shard payloads use `AKSS1`.
Garbage collection and automatic rebuild are suspended until activation, so
retained revisions remain readable and rollback checkpoints survive placement
changes.

An unfinished intent blocks a different plan. Repeating the same configuration
is idempotent. `runtime.reconfiguration.resumePendingChanges` defaults to true:
a surviving authority leader retries after interruption, restart or leadership
change. Set it to false to require an explicit `reconfigureCluster` retry.
`stripeInterruptionPolicy = RETAIN` (default) retains interrupted intents and
source data. `CANCEL` asks the authority recovery worker to cancel an interrupted
STRIPE plan before authority membership activation starts, even when
`resumePendingChanges` is false. Cleanup is a separate bounded attempt: the
caller returns on its deadline without waiting for another timeout. It requires
a surviving authority quorum and retries after failure or restart. The policy
and freeze/activation phases are stored in the committed intent, so a successor
uses the same contract. While quorum is unavailable the pending plan remains.
After authority activation starts, the plan must be resumed to completion;
automatic recovery follows `resumePendingChanges`, and explicit retry is always
available. Clear a cancellation hook before resuming such a plan.
`cancelClusterReconfiguration()` explicitly cancels an idle pending STRIPE plan
under either policy, forwards according to `routingMode`, returns false when
none remains, and rejects cancellation after authority activation starts.
Cancelling keeps the old active configuration and never reuses the cancelled
generation or exposes its aliases. Preparatory non-voting learners remain
reusable; the next successful authority membership transition removes unused
ones. Staged shard data is retained; cancellation
does not delete real storage. Both new policy options are STRIPE-only.
`clusterReconfigurationStatus()` reports authority, active/pending generations,
completed partitions and the latest local error. STRIPE also reports transferred
bytes, completed records, `stripeWritesBlocked`, and `activationStarted` for the authority's current transfer attempt; these
counters reset on retry or restart. `clusterPartitionStats()` exposes local
streams, leaders, sequences, commit indices, configuration generations and
each local child's complete storage statistics. PARTITIONED root storage
statistics describe its control engine; data sequences and storage metrics
belong to these independent partition engines.

`runtime.reconfiguration.writePolicy` selects `WAIT` (default) or `REJECT`
during group administration or a STRIPE lease freeze. `timeoutMs` defaults to
60000 (maximum 300000) and is one overall budget for serialization admission,
authority reads/intent commit, preparation, draining, shard transfer, membership,
and activation. Nested and forwarded requests inherit the remaining budget;
parallel partition and shard work share its deadline. Existing per-RPC limits
can expire earlier. A deadline cannot preempt a local filesystem flush or an
embedding's blocking callback; checks surround cooperative work and network
waits are bounded. A submitted consensus command may still commit after the
caller times out: inspect status and retry/cancel the exact pending plan rather
than assuming rollback. `maxConcurrentPartitions` defaults to 1 (maximum 16).
`maxTransferBytesPerSecond` limits new-holder catch-up per partition or the
STRIPE transfer; zero is unlimited. Existing transport resource limits also apply,
and embeddings may supply a thread-safe cancellation hook. PARTITIONED bulk copy admits
writes until the final journal tail and joint membership transition; unrelated
groups continue accepting writes. Batches do not provide cross-group atomicity.
Range queries require complete coverage and reject leadership changes during
capture.

MIRROR, partition and placement Raft groups apply the configured snapshot policy
on leaders and followers. Snapshots preserve retained history, original mutation
timestamps, request results and membership configuration generations, allowing
the committed journal prefix to be reclaimed without losing these state-machine
records. Placement snapshots include active and unfinished target configurations;
STRIPE also includes retained metadata revisions and placement aliases. Shard
payloads continue to use the placement transfer path.
Placement authority state now uses `AKPC2`; STRIPE placement aliases use `AKSH2`.
These pre-release internal formats replace `AKPC1`/`AKSH1` without migration.
Existing PARTITIONED/STRIPE cluster storage using those old formats must be
recreated. Public revision and shard payload formats remain unchanged.
VersionLog retention still controls public history availability. Retry identities
are partition-scoped: use `queryRequest(id, originalKey)` for PARTITIONED
consensus. Keyless request queries remain available for a shared MIRROR stream.
Retry-safe requests require quorum completion.

`ClusterConfig::failover()` separates owner election from acknowledgement:

| Policy | Contract |
|---|---|
| `NONE` | No automatic data-owner election. Consensus groups require an explicit `campaignClusterLeadership(streamId)` or leadership transfer. Static replication keeps its fixed owner. |
| `PRESERVE_ACKNOWLEDGED` | Automatic quorum election with quorum-completed writes; requires `RAFT_QUORUM`. |
| `ALLOW_ACKNOWLEDGED_LOSS` | Automatic fenced election with local-journal acknowledgement, permitted for `ASYNC` or `PRIMARY_ACK` with local completion. Accepted writes may be lost after failover. |

Constructor `DEFAULT` resolves to PRESERVE for RAFT_QUORUM and NONE otherwise;
only the three resolved policies are serialized. Data consensus is selected by
RAFT_QUORUM, ALLOW_ACKNOWLEDGED_LOSS, or explicit JOINT_CONSENSUS membership.
This permits manual election and online placement with local acknowledgement.
A locally acknowledged mutation becomes visible only after background quorum
commit. Acceptance confirms a current-term writer with a fresh quorum fence;
a network-isolated old leader cannot acknowledge new writes. Local completion
does not promise read-after-write visibility or survival after failover.
For PARTITIONED campaigns, stream id is partition index + 1; MIRROR uses 0.
Existing fixed-primary replication and external fencing remain separate choices.

### 15.3 Node Roles and Capabilities

Runtime role is `STANDALONE`, `PRIMARY`, or `REPLICA`. Startup role may be
`AUTO`, `PRIMARY`, or `REPLICA`.

Capabilities are bit flags:

| Capability | Meaning |
|---|---|
| `COORDINATOR_ELIGIBLE` | Node may be selected as primary |
| `RAFT_LEARNER` | Receives a data-consensus journal without voting or owning a new partition |
| `PLACEMENT_STANDBY` | STRIPE joining node, excluded from active shard placement and initial metadata voting |
| `DATA_BEARING` | Node can store KV data and receive routed writes |
| `STRIPE_FAILOVER_ELIGIBLE` | Sole optional owner-failover candidate; for RAID.10, the shared hot spare excluded from normal pairs; unrelated to metadata-Raft leadership |

Node id zero is reserved and invalid for configured nodes.

### 15.4 Acknowledgement and Consistency

Replica acknowledgement policy:

| Field | Values |
|---|---|
| `AckPolicyMode` | `NONE`, `ALL_TARGETS`, `QUORUM` |
| `AckStage` | `RECEIVED`, `APPLIED`, `DURABLE` |

Write consistency:

| Value | Meaning |
|---|---|
| `LEGACY_ACK_POLICY` | Use the `AckPolicy` behavior |
| `LOCAL` | Complete locally |
| `ONE_REPLICA` | Require one replica acknowledgement |
| `QUORUM` | Require quorum |
| `ALL_CONFIGURED` | Require all configured targets |
| `AVAILABLE_REPLICAS` | RAID.1 only: require every replica currently in the live acknowledgement set; an empty set permits a durable Primary-local write |

Consistency mode:

| Value | Meaning |
|---|---|
| `PRIMARY_ACK` | Primary-to-replica protocol governed by write consistency/ack policy |
| `ASYNC` | Fire-and-forget replication with local completion |
| `RAFT_QUORUM` | Raft-style quorum commit over native replication transport |

`RAFT_QUORUM` derives quorum from data-bearing voters and forces failed writes
on acknowledgement timeout.

Non-Raft `PRIMARY_ACK` and `ASYNC` are owner-based replication, not consensus.
`MIRROR` has one group-wide Primary; `PARTITIONED` and `STRIPE` may have multiple
writer nodes in one group, but each key still has exactly one owner.
For `MIRROR`, the persistent config names exactly one
Primary; a conflicting startup role, replica target, security pin, or manifest
lease is rejected. `AUTO` follows a valid unexpired manifest primary lease only
when it matches that configured Primary; expired or missing leases do not
self-promote a primary and require explicit startup-role selection. A replica
uses the configured Primary id unless a matching runtime assertion is supplied.
The Primary renews its local manifest lease every 10 seconds for a 30-second
window. This lease is not distributed fencing. Primary changes require an
explicit authorized transition under the MIRROR fencing policy below and a
shared replacement config. Merely changing `ClusterConfig::primaryNodeId()` is
not sufficient: an existing `AKCG1` membership naming another Primary rejects
startup.

An offline promotion must set `mirrorPromotion.enabled` on every node for the
one authorized transition. The authorization names the previous Primary, its
exact persisted group epoch, and the candidate's expected durable storage
sequence. The configured new Primary forces local storage durable, requires its
sequence to equal that authorization, atomically preserves the group id, changes
the Primary id, and increments the group epoch by exactly one. A candidate with
no prior membership, a stale sequence, a different previous Primary/epoch, or a
skipped epoch is rejected. Replicas accept the successor only when their existing
membership has the same group id, the same authorized previous Primary/epoch,
the incoming Primary matches their configured endpoint, and the incoming epoch
is exactly the next epoch. The operation is retryable after membership was
published but endpoint startup failed. Promotion is rejected under `STATIC`;
the other policies must fence the previous authority before publishing the
successor. This remains an explicit offline configuration transition, not
automatic data-Primary election. Promotion requires
explicit `PRIMARY`/`REPLICA` startup roles; `AUTO`, membership reset, Raft, and
non-MIRROR placement are rejected.

Explicit `PRIMARY` startup is rejected on every other node under the same config.
An unexpected lease-renewal failure is contained by the background worker: it
stores the failure, demotes the manager to `REPLICA`, and causes the owning
runtime to stop replication endpoints. Subsequent runtime operations rethrow the
stored failure through the manager health check instead of terminating the process.

#### MIRROR fencing policy

`ClusterRuntimeOptions::mirrorFencing` separates Primary authority from data
replication. It applies only to non-Raft `MIRROR` (`PRIMARY_ACK` or `ASYNC`).

| `MirrorFencingMode` | Contract |
|---|---|
| `STATIC` (default) | Fixed configured Primary; reject `mirrorPromotion`. No dynamic election or distributed lease. |
| `QUORUM_FENCED` | Separate Raft group orders write grants and authority changes; value payloads stay on the selected data replication path. |
| `EXTERNAL_FENCED` | An embedding-provided `IMirrorFencingProvider` must fence the old writer before promotion and validate current authority before writes and authoritative reads. |

For `QUORUM_FENCED`, supply `mirrorFencing.authorityNodes` with exactly one
voter per configured node, including authority-only witnesses. Use the same
node ids and advertised hosts, both coordinator/data-bearing control
capabilities, and dedicated replication ports that do not overlap data or
STRIPE metadata endpoints. Control `dataPort` is unused. At least three and at
most 512 voters and an explicit shared nonzero `clusterGroupId` are required.
Secure transport must pin every authority voter, including witnesses. A main
node without `DATA_BEARING` votes on authority but receives no mirrored data.
Authority topology is fixed; these voters are not exposed through the data
Raft membership APIs. `transferMirrorAuthorityLeadership()` transfers control
leadership only, not data Primary authority. Control leadership automatically
converges to the configured/current data Primary (or authorized successor).
Native engines require WAL and WAL recovery to be enabled for this policy.
An embedded runtime must supply real durable apply/forceDurable/sequence
callbacks; a no-op durability callback cannot satisfy the recovery contract.

Each Native public mutation runs through `executePrimaryWrite`: commit a BEGIN
grant by authority quorum, perform the local mutation and selected data
replication, force local storage durable, persist a durable completion receipt,
then commit END by authority quorum. The control log contains only Primary id,
epoch and operation token; never public keys or values. One grant may be active
at a time. BEGIN orders the mutation before any subsequent Primary transition;
a transition cannot commit while that grant is active. END follows durable
local application. A put batch uses one grant; each rollback mutation is also
guarded (a multi-key rollback need not be one atomic grant).
Authority quorum loss fails closed before local application when no grant was
committed, regardless of `ACCEPT_LOCAL`. Loss after BEGIN may report failure
after local application; callers must not infer that a failed operation had no
effect. Fencing does not add atomic rollback of a partially applied batch.

Data may remain `ASYNC`: writes wait for authority BEGIN/END and local durability,
not replica data arrival. A stopped data replica does not block writes when the
authority majority is reachable. Async promotion can still lose acknowledged
data not present on the selected successor; fencing is not a zero-data-loss
guarantee and does not establish a global MVCC timeline across failover.

**Recovery contract:** outstanding grants never expire by
timeout or clock. A process pause, authority leader change, or restart cannot
revoke a possibly executing local mutation and simultaneously permit a new
Primary. An exception or crash before a durable completion receipt leaves the
grant unresolved and blocks further writes and promotion. When the original
Primary has a matching durable receipt, it automatically retries END after
startup or authority quorum recovery, without waiting for another public write.
Recovery is serialized with public writes and matches the receipt's Primary id,
epoch and operation token against the current authority grant. An old or foreign
receipt cannot release a different grant, and recovery never replays the mutation
or fabricates a receipt. A public write also retries this recovery before BEGIN.
Receipts identify the grant obtained before local application, even if external
fencing changes authority while the operation is running. An obsolete Primary
cannot report successful release merely because the successor has no active grant.
Otherwise `ClusterRuntimeOptions::mirrorRecovery` selects how to resolve the
unconfirmed grant. This policy is independent of `ASYNC`/`PRIMARY_ACK` and the
data acknowledgement policy; it applies to `QUORUM_FENCED` authority grants.

| Recovery mode | Behavior without a matching durable completion receipt |
|---|---|
| `BLOCK` (default) | Keep the grant unresolved; never invoke an external recovery provider. |
| `MANUAL` | `AkkEngine::recoverMirrorWrite()` (or the runtime method) explicitly asks the provider to resume the original Primary. An explicitly authorized promotion asks it to revoke the old Primary instead. |
| `AUTOMATIC` | The original Primary retries provider recovery in the background at `retryIntervalMs`; explicit recovery and promotion remain available. Primary promotion itself still requires `mirrorPromotion`. |

`MANUAL`/`AUTOMATIC` require `mirrorRecovery.provider`, an
`IMirrorRecoveryProvider`; `BLOCK` rejects a configured provider to avoid silently
ignoring it. The retry interval defaults to 1000 ms and must be in [50, 3600000].
These are local runtime choices and may be changed on restart without rewriting
the authority state. Matching completion receipts are always recovered
automatically, including with `BLOCK` or `MANUAL`.

A `MirrorRecoveryRequest` names the cluster/group, old Primary/epoch, exact
operation id, candidate, expected local durable sequence and requested action.
The provider returns `nullopt` or throws if safety remains uncertain. A
`MirrorRecoveryProof` must contain the exact request; a proof for another
operation, generation, candidate, sequence or action is rejected. Providers are
trusted integrations: matching fields alone cannot prove that an external
process has stopped. Never return a proof merely because a heartbeat timed out.

For `RESUME_PRIMARY`, the provider must establish that the old operation and all
older execution instances can never resume writing, while allowing this runtime
to continue. Partial effects must be recoverable by the engine's durability
callback. The engine serializes recovery with writes, rechecks authority through
a quorum barrier, forces local durability, confirms the expected sequence,
persists a matching receipt and commits END for that operation. This does not
report the original failed operation as successful or roll back partial effects.

For `PROMOTE_PRIMARY`, the provider must additionally prevent the old Primary
from writing or restarting under its old authority, including paused/in-flight
operations. After checking proof, authority and candidate durability, the engine
commits END for the exact operation and then the ordinary guarded Primary
transition. A changed/intervening grant is never forcibly discarded. If the
transition fails after END, retrying the same promotion can complete it.
Previously persisted forced transitions remain replayable; new recovery does
not emit them. Data and authority persisted formats are unchanged.

The provider must be idempotent and thread-safe, honor the supplied cancellation
token, and must not re-enter the engine/runtime. Native close cancels recovery
before waiting for active public operations. Cancellation, failed durability,
stale proof or lost quorum never supplies permission to release a grant. A valid
durable receipt can still complete recovery after a failed END attempt. `recoverMirrorWrite()`
returns true when it resolves a pending grant, false when none is pending or the
provider cannot yet prove recovery, and throws on invalid proof or failed checks.
Automatic attempts preserve the pending grant on failure and retry; the existing
`mirrorWritePending` observation exposes that unresolved state.

Existing external fencing implementations can be used for quorum promotion:

```cpp
runtime.mirrorRecovery.mode = MirrorRecoveryMode::MANUAL;
runtime.mirrorRecovery.provider =
    std::make_shared<MirrorFencingRecoveryProvider>(fencingProvider);
```

This adapter calls the existing fencer for `PROMOTE_PRIMARY` and declines
`RESUME_PRIMARY`, since fencing this runtime would not let it resume.
The adapter checks cancellation before and after `fence()`; the wrapped callback
must return in bounded time because its existing API has no cancellation token.
Independent providers may integrate process/session recovery, storage access revocation or
other mechanisms meeting the requested guarantee. Engine-managed expiring leases
and universal generation fencing are not supplied by this policy interface.
The old optional `mirrorFencing.external` setting for quorum recovery is replaced
by `mirrorRecovery`; `mirrorFencing.external` now belongs only to
`EXTERNAL_FENCED` authority.

Without a recovery proof, an unresolved grant deliberately sacrifices availability;
do not delete the authority state or fabricate a completion receipt to recover.

`EXTERNAL_FENCED` requires `mirrorFencing.external`. The provider's `fence()`
must prevent the previous Primary from writing or restarting as a writer,
covering already-running operations and long process pauses. It must be
idempotent for the same transition and throw on uncertainty. `validatePrimary()`
must reject revoked authority. These calls are integration hooks, not a bundled
STONITH/lease service: a heartbeat, an unchecked success callback, or a local
flag is not sufficient external fencing. Fencing failure does not advance the
persisted group epoch. No new production service dependency is imposed.

Quorum data frames, retained history and snapshot keys carry the authority
epoch (`AKMD1` envelope). Receivers reject unapproved/obsolete generations
before application. An authorized successor forces a full replica snapshot,
including equal-sequence replicas, to avoid merging divergent old data.
`cluster.membership.mirror-resync` records the intent before successor
membership is published and is cleared only after snapshot application is
durable. Reopening without the promotion options still retries an unfinished
snapshot; a candidate with an outstanding resync intent cannot be promoted.
Rejoining with storage ahead of the authorized candidate sequence is rejected;
restore an authoritative copy first. Promotion still requires exact previous
group identity/epoch and exact candidate durable sequence on every retry.
The promotion options authorize one transition, not subsequent ordinary boots:
clear them after that node has completed the transition. Keeping the old
expected sequence after later writes intentionally fails the startup assertion.
An unfinished replica snapshot still resumes from its durable intent even when
these options have been cleared after successor membership was accepted.

`mirror.fencing` durably records the chosen policy and authority topology;
changing or downgrading it in an existing database directory is rejected.
`mirror-authority/` stores independently checksummed control state and recovery
receipts. Do not remove these files to change mode. Choose a policy for a new
directory, or explicitly rebuild from an authoritative copy with all old
writers stopped. Enabling fencing does not retroactively constrain old binaries.
The pre-release replication wire magic is reset to `AKR1`; this denotes the
current protocol, not compatibility with earlier development builds that used
that name. All peers must upgrade together and
mixed fencing modes are rejected in the handshake. Native clients must be
rebuilt for the changed runtime-options/stats ABI. The pre-release `AKC1`
ClusterConfig format remains unchanged; fencing options are runtime options.

`EngineStats::cluster` exposes `mirrorAuthorityLeaderNodeId`,
`mirrorAuthorityPrimaryNodeId`, `mirrorAuthorityEpoch`,
`mirrorAuthorityCommitIndex` and `mirrorWritePending`.

**Future improvement:** ReadIndex plus BEGIN/END consensus and receipt sync
add latency, control-log traffic and serialization even with async data.
Investigate safe batching/pipelining and bounded authority leases. Improving
unfinished-write recovery and reducing promotion downtime are also open work.
These are optimization/availability opportunities, not permission to weaken
fencing. A lease design needs a correctness argument for clock behavior,
expiry, in-flight writes and long process/VM pauses, with explicit watchdog or
external fencing requirements. Do not replace per-operation consensus with a
heartbeat-only check or the local manifest lease.

Timeout action is `ACCEPT_LOCAL`, `FAIL_ACK`, or `FAIL_WRITE`. `FAIL_ACK`
reports acknowledgement failure after local commit in local-first primary-ack
paths. `FAIL_WRITE` also reports acknowledgement failure, but engine-level
non-Raft `MIRROR`/`PARTITIONED` writes first commit and sync the owner-local
WAL/MemTable state before waiting for replica acknowledgements. This ensures
the owner records a write locally before waiting for its replication; neither timeout
action rolls back that local commit. STRIPE instead uses the intent, durable
shards, then owner-metadata publication contract in section 15.2. Replica lag
action is `ASYNC_RESYNC`, `REJECT_REPLICA`, or `BLOCK_WRITES`.

At the engine layer, non-Raft native cluster configurations require Blob storage
to be disabled. Low-level non-Raft `ReplicationServer::shipBlob` remains a
live-only frame path for tests and embedding, but Blob frames are not retained
for catch-up and are not acknowledgement-gated. Engine Blob replication uses
`RAFT_QUORUM` with the configured `raftBlobPolicy`.

### 15.5 Transport and Trust Boundary

Replication transport is `SECURE` by default. Secure mode uses native
raw-public-key identity:

- persistent identity seed can be loaded or created,
- peers can be pinned by node id and public key,
- clients can require an expected primary node id.

Plain replication is intentionally restricted to validated LAN or loopback
advertised addresses. PLAIN deliberately provides neither confidentiality nor
peer authentication: a process able to reach the replication port can impersonate
a configured node and forge acknowledgements. Node-id filtering and LAN address
validation do not establish an authentication or quorum trust boundary. PLAIN is
for explicitly trusted networks/processes only; use SECURE with peer pins when
that assumption does not hold.

Both non-Raft hello payloads are exactly 82 bytes: the existing 34-byte identity,
role, fencing and group prefix, followed by the persistent 16-byte cluster ID and
a 32-byte configuration fingerprint. Both endpoints compare these fields before
admitting data, catch-up, membership persistence or acknowledgement targets.
PLAIN and SECURE enforce the same compatibility contract. This comparison does
not authenticate a PLAIN peer.

`ClusterConfig::replicationFingerprint()` hashes the `AKNP1` canonical encoding
of the cluster ID, flags, replication mode, primary, acknowledgement and
consistency policies, membership policy, stripe geometry, stable partition count
and placement algorithm. Non-STRIPE contracts also include membership sorted
by node ID (including capabilities, advertised hosts and all ports). STRIPE
topology and RAID.10 pair order are instead validated by committed placement
intents, permitting online replacement. PARTITIONED data consensus excludes its
mutable copy count from this fingerprint and validates it through placement.
Save timestamps and local runtime choices such as paths and peer pins are
excluded. `ClusterRuntime` supplies the contract from its config; direct
`ReplicationClient`/`ReplicationServer` users supply matching
`replicationClusterId` and `replicationConfigFingerprint` runtime options.

The earlier 34-byte hello is rejected; upgrade all non-Raft nodes together.
The runtime-options layout and STRIPE control callback signature also changed,
so native embedding callers must rebuild. The callback now receives the
transport-owned connection lifetime via `PeerSession`. The hello and callback
changes retain the stored config and membership formats.

### 15.6 Erasure Codec

The native erasure codec API supports Reed-Solomon-style and external codec
selection contracts. Stripe configuration records data and parity shard counts,
the number of copies per logical shard, and defaults to 4 data, 2 parity, and one
copy. Active native STRIPE uses the RS codec for data/parity placement, degraded
reads, and best-effort read repair. Its systematic data-splitting path also
accepts zero parity for canonical `RAID.0` and `RAID.10`; RAID.0 requires every
data shard, while RAID.10 satisfies each logical shard from either mirror copy.

### 15.7 Replication Connection Lifecycle

Point-read and STRIPE-control `timeoutMs` is one budget starting at API entry.
It includes admission, pending-response drainage, state/socket/message/send-lock
waits, transmission (including large-transfer READY), and response waiting.
An unsent admission timeout leaves the connection usable. A stalled or partial
send is interrupted by shutting down the connection and cancelling transfer
waits; it cannot be resumed as a different request on that socket. A point-read
response timeout leaves the connection alive for replication ACKs, and the late
read response is drained before another point read is sent.
Physical writes share that deadline: Windows uses cancellable overlapped I/O
and retains send buffers until cancellation completes; POSIX uses nonblocking
per-call sends and deadline-bounded writable waits.

Non-Raft listeners dispatch handshakes to four fixed workers with at most 64
queued sockets; overflow is disconnected. The existing five-second socket I/O
timeout applies during handshaking, including SECURE negotiation. A stalled
handshake does not occupy the accept worker. Server close interrupts active and
queued handshakes and joins all handshake workers before draining replicas.
Socket ownership transfers once, including duplicate-node rejection and failed
handshakes, so no rejection path closes a reused descriptor twice.

For non-Raft replication, each accepted replica connection owns a stable socket
handle and send, receive, read, and bootstrap workers. After `SERVER_HELLO`, a
connection starts in `BOOTSTRAPPING`, not `LIVE`. The bootstrap worker obtains a
fixed `(lastSeq, throughSeq]` history range and feeds it into the normal outbound
queue while waiting for frame/byte capacity as the send worker drains that queue.
Queue capacity therefore bounds resident catch-up frames; the total catch-up
range may be larger than the queue. A single logical outbound message larger than the byte
limit cannot make progress and requires resynchronization.

After each range is queued, bootstrap reads the current sequence again and
queues another contiguous range when writes committed during catch-up. It changes
the connection to `LIVE` while holding the same replica-set mutex used by live
fan-out. Consequently, a write is either included in a catch-up range or appended
after all queued catch-up frames; it cannot fall between bootstrap and live
delivery. Snapshot fallback uses the same bounded producer path and resumes
history catch-up from the snapshot sequence. `BOOTSTRAPPING` connections reject
duplicate node connections but are excluded from live replica counts, targeted
sends, live fan-out, and acknowledgement counts until promotion.

Any connection worker may request termination, but an atomic guard issues socket
shutdown only once. Actual socket close and handle invalidation occur only after
all connection workers, including bootstrap, have joined. Catch-up cancellation
wakes a producer waiting for queue capacity, and a slow bootstrap does not retain
one of the four handshake workers.

A background reaper removes disconnected replicas and joins their workers during
normal operation, rather than retaining all historical connections until server
close. Reaping waits for worker initialization to finish, including failed
initialization, so it cannot race thread creation. Server close interrupts
handshaking sockets, joins the accept worker, and drains connection cleanup.

Read callbacks run on a separate per-connection worker so owner coordination
cannot block receipt of replication ACKs. Each connection allows at most one
active read and one pending read; another request while the pending slot is
occupied terminates the connection. Outbound frames and read responses share
serialized socket writes. Configured outbound frame/byte queue limits remain
independent of this read-work bound.

The non-Raft runtime copies shared endpoint ownership while holding its lifecycle
mutex, then releases that mutex before peer reads, replication sends, targeted
writes, statistics collection, and endpoint shutdown. A slow network operation
therefore cannot head-of-line block unrelated runtime operations. Non-Raft start
is transactional from the caller's perspective: a failed bind or endpoint start
performs best-effort cleanup and leaves the same runtime eligible for a later
`start()` retry.

### 15.8 Raft Replication, Proposal Batching, and ReadIndex

Raft uses a long-lived worker and reusable outbound connection per peer. Vote,
append, snapshot, and leadership-transfer RPCs reuse the transport session,
including the SECURE handshake. Request/response exchanges on a connection are
serialized; a failed exchange discards the connection so a delayed response
cannot satisfy a later request. Subsequent work reconnects. Replication requests
coalesce their target index and heartbeat/read-confirmation work. Peer workers
are retired after membership removal. Inbound connections serve multiple RPCs;
the existing 128-handler limit bounds accepted connections. Close interrupts
sockets and drains workers and handlers.

`raftMaxReceiveMemoryBytes` bounds aggregate Raft frame receive/decode
reservations across inbound and outbound peer connections. Raft protocol v1
limits each Raft payload to 4 MiB and the aggregate budget defaults to 64 MiB.
The general replication framing and non-Raft chunk-transfer limit remains 128
MiB. AppendEntries batching and snapshot/Blob chunking split work before the
Raft limit. The configured budget must accommodate at least one maximum-size
SECURE Raft frame and its receive/decode copies. Budget exhaustion rejects the
affected connection; it does not weaken frame validation or commit rules.
Current use is reported as
`transferMemoryBytes` in the native cluster statistics snapshot.

The Raft leader heartbeat interval is a runtime-only startup option,
`raftHeartbeatIntervalMs`, defaulting to 100 ms with a valid range of 10-1000
ms. It is intentionally excluded from the compatibility fingerprint because
each term's leader alone controls its send interval. Increasing it reduces idle
traffic but delays commit-index propagation and leaves less margin before the
3-second minimum election timeout.

Before serving Raft RPCs, protocol-v1 peer hello exchanges the persistent
128-bit cluster id, a BLAKE2b-256 compatibility fingerprint, the Blob policy,
and retry-request policy. A different cluster id is rejected as a foreign
cluster. A different protocol-affecting config, Blob policy, or request policy
is rejected as a policy mismatch before voting or log replication. The
fingerprint covers Raft/consistency behavior but deliberately excludes the
current node list so joint-consensus membership changes can introduce a node
that already carries the same cluster id and policy. A Raft configuration may
contain at most 512 data-bearing members (voters plus learners); together with the 32768-entry retry
request-state limit, this keeps final snapshot metadata within one 4 MiB Raft
frame even for maximum-length configured host names.

The engine admits writes through `IClusterRuntime::submitMutation()`. The Raft
runtime serializes admission, assigns the next state-machine sequence from its
snapshot, committed state machine, and retained consensus log, then invokes the
mutation factory synchronously. Success requires current-term quorum commit and
durable local application, not merely queue admission or log append. The older
`submitEntry()`/`shipEntry()` entry points remain low-level embedding interfaces;
their caller-supplied sequences must preserve log order.

`cluster-raft.log` and its segments are the only engine Raft proposal ledger;
the engine does not create or append a separate `raft.log`. A pre-release data
directory may still contain an obsolete `raft.log`; it is ignored and is not
deleted automatically. With `raftBlobPolicy=RAFT_LOG`, all Blob chunks and the
final Blob-reference mutation share one state-machine sequence and one proposal,
and only the proposal-final entry is a commit boundary. Admission, replication,
quorum commit, state-machine application, and retry-safe request identity
therefore cover the complete logical write atomically.

Admission is bounded to 4096 outstanding proposals and 256 MiB of queued or
not-yet-applied entry data/overhead; timed-out retained entries still count toward
the byte limit. Exceeding either bound fails admission. A flush worker
coalesces arrivals for up to 1 ms, draining at most 256 proposals or approximately
4 MiB per batch (a larger individual proposal is not split at this boundary).
Each batch is durably appended before replication. Later batches can be admitted
and appended while earlier batches await quorum. AppendEntries carries multiple
entries within the frame limit; commit advances over a quorum-replicated
current-term prefix, and application shares its durability barrier across the
committed batch. Blob chunks for one payload remain contiguous in the log.
Internal protocol bulk writes submit bounded groups without promising transactionality.

Raft log compaction exports a full state-machine snapshot only after at least
`raftSnapshot.minLogEntries` committed entries, approximately
`raftSnapshot.minLogBytes` committed log bytes, or
`raftSnapshot.maxIntervalMs` with uncompacted commits. The trigger check is
rate-limited to once per second. This keeps ordinary proposal batches from
scanning the full database while still bounding log growth.
Every holder compacts its applied prefix, including followers. Reclaimed log
entries release their in-memory payloads and unreferenced segment files. A segment
with a retained suffix can still contain at most its original rotation-sized
unused prefix. VersionLog retention is independent: keeping all public history
intentionally retains that history, but no longer requires keeping the Raft journal.

Raft engine snapshot keys use an internal `AKES1` envelope distinguishing current
heads, historical records and placement state. Historical records retain their
sequence, writer, timestamp, logical flags and Blob identity. Values are expanded
for transport and restored with their original Blob identities; tombstones and
rollback revisions remain historical records. Heads restore current visibility
without synthesizing additional public revisions. Non-Raft snapshots keep their
existing key/value representation. Placement snapshots contain one state record
followed by STRIPE metadata history and heads when applicable.

Snapshot export captures one visibility sequence and immediately returns a
lazy entry stream. Its first consumer drives one scan that writes each record to
a CRC-protected local file under `cluster-snapshot-exports` and passes the same
record to transport before advancing. Transport therefore starts before the
database-sized export file is complete. The database is not materialized as an
in-memory vector. Snapshot entries use begin/chunk/finish delivery; external
Blob values are read and, when necessary, Zstd-decoded incrementally, so heap
retention is bounded by the current inline record or configured value chunk
rather than the largest Blob. The declared value size and CRC are verified
while the export file and transport consume the same chunks. A completed
export is re-readable in bounded chunks for retry and consumers of the
same sequence share it; a concurrent consumer follows flushed prefixes of the
active export and validates the completed file before accepting the snapshot.
For non-Raft bootstrap, entries above `transfer.thresholdBytes` are sent from
their completed `key + value` range in that export file. Transfer hashing and
resumed reads are bounded and seek directly into the range; they do not create
a second sender-side transfer spool. Small entries retain the in-memory path.
The wire format, content-addressed transfer id, whole-payload CRC, exact retry,
and concurrent-consumer behavior are unchanged. The file is deleted when its final handle is
released, and stale exports left by process failure are removed at engine
startup.

Snapshot capture holds write admission only for a short barrier: every non-empty
active MemTable shard is sealed and replaced. SST-backed engines pin those
tables together with the current SST readers. Memory-only engines publish the
sealed tables as immutable in-memory generations, so normal point reads and
scans merge the new active generation with the pinned generations by key and
sequence. The database-sized scan and local export-file write run after the
barrier, so normal writes continue while the snapshot is materialized. After
the final memory-only snapshot pin is released, a maintenance worker consolidates
generations one shard at a time while writes continue into a replacement active
table. A failed consolidation remains pending and is retried with bounded
exponential backoff; it never discards the published generations. `EngineStats`
reports completed consolidations, failures, the monotonic timestamp of the last
failure, and whether work remains pending. Tombstones remain in the consolidated
base. Blob-file deletion is pinned until the fixed view has resolved all
referenced values.
Retained-history capture pins VersionLog segment files and their captured byte
boundaries until the lazy history stream is released. Later appends remain outside
the fixed view, and retention pruning resumes after the pin is released.

Memory-only snapshot admission has two policies. `THROUGHPUT_FIRST` never waits
behind another snapshot-generation transition and rejects a snapshot whose
configured generation or byte budget is unavailable; cluster recovery retries
later. `COMPLETION_FIRST` waits for capacity and permits one individually
oversized generation, prioritizing snapshot completion. Waiting may apply write
backpressure because capture owns write admission, but the full export never
does. Both policies use the same fixed-sequence view and reject rather than
publish a partial or inconsistent snapshot.

Raft snapshot installation consumes that callback and sends each bounded RPC
as soon as it fills instead of first building all snapshot RPCs in memory.
Non-Raft snapshot fallback feeds the same callback into its bounded bootstrap
queue. Snapshot begin uses an unknown count; the producer's observed entry
count is finalized and verified only by the completion request. An interrupted
last consumer aborts generation and never publishes its partial export as
reusable. If another consumer is already following the active export, generation
continues for that follower.
The receiver closes and syncs its validated staging file before Raft publishes a
durable snapshot-install intent. Reopen completes that intent from staging. History
is synced before the KV snapshot WAL commit, and interrupted replay detects already
stored history records. Identical retried snapshot WAL records are idempotent;
conflicting records for the same snapshot sequence and key fail recovery.

Proposal deadlines use `ackTimeoutMs` from admission. Timeout, shutdown, or
leadership loss completes affected futures with an error; a timeout is not a
rollback. Ordinary `put`, `remove`, and external bulk writes do not deduplicate retries;
the opt-in request API below does. Membership changes
and leadership transfer serialize with each other and reject in-flight proposals;
new proposal admission is paused during these administrative operations.

With `raft.membership.allowLearners = true`, a `RAFT_LEARNER` node capability
marks an initial data-bearing non-voting member. All other initial data nodes
are voters. At least one voter is required. The capability is invalid outside
`RAFT_QUORUM` or with learners disabled. A newly joining process must include
itself with this capability in its bootstrap config before it starts; use the
same cluster id and Raft policy as the existing group. SECURE peers must have
the new node's public-key pin provisioned before addition.

The leader's `AkkEngine::addClusterLearner(NodeInfo)` adds a non-voting member
through a committed membership entry. Its replication/snapshot stream uses
the normal Raft peer workers. Learners cannot campaign, vote, count towards
write or ReadIndex quorum, or receive leadership transfer. Their unavailability
does not change voter quorum. `LOCAL_STALE_OK` reads may inspect their local
replica; this does not provide a linearizable learner read.

`promoteClusterLearner(nodeId)` requires `JOINT_CONSENSUS` and
`allowOnlineVoterChanges`, pauses new proposals, and rejects promotion unless
the learner's acknowledged match index covers the leader's complete current
log. Promotion preserves endpoint and coordinator eligibility, clears the
learner capability, and commits joint then final membership. No automatic
promotion occurs. If leadership or transport fails, the outcome may be partial;
inspect committed membership and retry promotion on the current leader. A
durably appended joint entry participates in election quorum and voter
eligibility even before commit notification, so a crash between joint
replication and final commit cannot strand the promoted member outside the
recovery election.

`removeClusterLearner(nodeId)` commits removal without changing voter quorum.
Learner-only changes are also available with STATIC voter membership; voter
changes still require joint consensus. Adding an existing member, promoting
an absent/voting node, or removing a voter through the learner API is rejected.
`ClusterRuntime` exposes the corresponding `addRaftLearner`,
`promoteRaftLearner`, and `removeRaftLearner` methods. These are Native C++ APIs;
the network-facing data protocols do not expose membership administration.

Member roles are encoded in the existing membership node sets, so Raft log
metadata, snapshot RPCs, durable snapshot-install transactions, compaction,
and restart all preserve them. Recovered membership overrides the initial
learner flag after promotion. `activeNodes()` returns these current roles;
`raftStats()` reports `voterCount`, `learnerCount`, `localLearner`, and each
replication peer's `learner` flag. During a joint transition, a node voting in
either side is reported as a voter. Upgrade all Raft peers together: the
compatibility-fingerprint domain is `AKRP2`. `AKC1` remains the initial pre-release config format.
A joining learner may accept leader replication while a pre-admission snapshot
still excludes it from authoritative membership; it cannot vote. This local
bootstrap allowance is durably cleared when committed membership admits it,
so removing and restarting the member cannot re-enable the allowance.

Ordinary elections always run a non-binding pre-vote round before advancing
the persisted term or voting for self. A sole-voter quorum needs no peer
exchange. `RAFT_PRE_VOTE` (`0x3A`) uses the same
32-byte payload as RequestVote (prospective term, candidate id, last log index,
last log term). `RAFT_PRE_VOTE_RESPONSE` (`0x3B`) uses its 9-byte response layout,
but reports the receiver's **actual** term, never the prospective term. Receiving
a probe does not change hard state, role, observed leader, or election/contact
deadlines. The candidate identity must match the negotiated peer hello and,
in SECURE mode, the configured public-key pin. PLAIN remains trusted-network-only.
Learners cannot
campaign or supply pre-votes; normal and pending/committed joint configurations
use the same voter eligibility and both-majorities rule as the real election.

A voter grants only for a future term and an up-to-date log, with no current
local leadership and no accepted same/newer-term AppendEntries or snapshot
contact during the baseline election timeout (3-6 seconds, derived from
`ackTimeoutMs`, before randomization). A heartbeat whose log prefix mismatches
still counts as leader contact. Timing uses the local monotonic clock; clock
synchronization is not required and this is not a data-read or fencing lease.
Isolation alone therefore does not inflate terms. A higher **actual** peer term
still updates hard state through the usual follower transition. If leader
contact, term, or voter configuration changes while probing, the result is
discarded before starting a real election. Late RPC replies cannot demote a
leader with an equal or newer term.

An accepted `TimeoutNow` from the observed leader bypasses pre-vote for an
explicit leadership transfer; otherwise healthy-leader suppression would block
the requested transfer. Normal retry/failover elections retain pre-vote. There
is no disabling option. The extra peer round trip applies to ordinary elections,
not to steady-state writes or reads. Pre-vote reduces avoidable election churn;
it does not guarantee progress through every asymmetric network partition.
Upgrade all Raft participants together, including STRIPE metadata and MIRROR
quorum-fencing authorities. Old `AKRP2` peers are rejected. This change does not
alter `AKR1`, `AKC1`, hard-state/log layouts, or require recreating persisted data.

A leader still appends a NOOP on election and must commit an entry in its current
term before serving linearizable reads. Subsequent reads do not append NOOPs:
each captures the term, commit index, voter configuration, and a fresh read round.
Only same-term successful AppendEntries replies to RPCs initiated for that round
or a newer round count. Previously received ACKs are insufficient. Joint
consensus requires a majority of both voter sets. The read fails on quorum
timeout or leadership/membership change, and waits for local application through
its captured index before returning. Concurrent reads may share a newer
confirmation round. This is not a clock-based lease read.

`ClusterRuntime::raftStats()` reports term, observed leader, commit/applied/log/
snapshot indexes, proposal and peer queues, per-peer match/next indexes and lag,
connection state and cumulative connection/batch counters. It also reports
retry-result capacity/use/rejections, request-journal activity, policy-handshake
rejections, and transport queue/budget use. Non-Raft runtimes leave Raft fields
zero while reporting their replication queue and shared transfer budgets.

The same snapshot exposes cluster health as `HEALTHY`, `DEGRADED`, `REBUILDING`,
or `FAILED`,
plus the most recently observed failure code and its Unix timestamp in
microseconds. Foreign-cluster and policy-mismatch rejections, lease-renewal
failures, endpoint-start failures, and peer-read timeouts are monotonically
increasing process-lifetime counters; they are not persisted. A successful
restart or recovered peer read may restore current health without erasing the
last-failure record or counters. `DEGRADED` means the runtime remains usable but
has observed a rejected peer, a peer-read timeout, or a required non-Raft peer
connection is currently unavailable. `FAILED` means a local lease, endpoint, or
Raft persistence failure prevents safe normal service. `REBUILDING` is emitted
by a RAID.1 Primary while a returning replica is catching up and is not yet an
acknowledgement target.

Every configured peer has a current connection flag and the Unix-microsecond
time of its last accepted handshake, valid replication frame, or successful
Raft RPC. Zero means no successful contact has been observed. Raft connection
state describes the current outbound peer session. PARTITIONED and STRIPE peers
are connected only when both inbound and outbound replication directions are
live; MIRROR uses the direction required by the local primary/replica role.

The Native snapshot is also the data contract for a future management
application. It includes the sample and runtime-start timestamps, persistent
cluster id, replication/consistency/transport modes, effective non-Raft group
identity, and every configured node's advertised host, ports, capabilities, and
stable id. Per-peer round-trip telemetry contains cumulative successful and
failed operation counts, consecutive failures, last completion/failure times,
and a successful-round-trip latency histogram. Histogram bucket bounds travel
with every value and bucket counts are cumulative, so a consumer can calculate
rates and approximate percentiles without sharing compile-time constants.
Counters and histograms cover the current runtime process and are not persisted;
the management application should retain timestamped samples and calculate
deltas. No metrics server, tracing backend, or external monitoring dependency is
required by the Native runtime.
`AkkEngine::stats().cluster` exposes the same operational snapshot. Read-only
traffic does not append log entries, although independent compaction or pending
writes may still update persistent state.

### 15.9 Retry-Safe Native Raft Requests

`cluster.runtime.requests.enabled = true` enables the native engine request API
for `RAFT_QUORUM` only. Call `newRequestId(retentionMs = 0)` once, save the entire
returned `ClusterRequestId`, and reuse it for every attempt of the same operation:
`putWithRequest(id, key, value)` or `removeWithRequest(id, key)`. Zero retention
selects the configured maximum. The random 128-bit nonce and immutable absolute
UTC expiry together identify the request; changing either creates a different
request, not an extension. Normal write APIs and network-facing data APIs are
unchanged. The high-level native facade exposes these methods via `engine()`.

An admitted request carries its identity and a length-delimited BLAKE2b-256
fingerprint of operation/key/raw value in the replicated mutation. Concurrent
retries share completion without allocating another storage sequence or Blob.
A retained request with different contents is rejected. `APPLIED` returns the
original storage sequence and Raft log index after quorum commit and durable
local application. Retrying a committed request never reapplies it, including
after another write has changed the key. Timeouts still throw and may subsequently
commit: reuse the same identity instead of issuing an ordinary write or new ID.

`queryRequest(id)` requires the leader and a fresh quorum ReadIndex. It returns
`APPLIED`, `PENDING` (queued or logged, not known durably applied), `NOT_FOUND`, or
`EXPIRED`; unavailable quorum/leadership is an error, not `NOT_FOUND`. `NOT_FOUND`
is only a point-in-time observation and does not cancel a concurrent submission.
Expired identities cannot admit new writes. `EXPIRED` does **not** prove a previous
attempt was never applied: an admitted request may finish after its expiry, and
the historical result may no longer be retained. Cluster clocks must be kept
synchronized; replicated expiry progress is monotonic and cannot be rolled back
to resurrect an expired identity. Do not automatically turn an expired retry into
a fresh request when duplicate application would be unsafe.

Results and expiry progress survive log compaction, restart, leader changes, and
snapshot installation. State-machine application and snapshot capture serialize
with result recording; crash recovery reconstructs receipts from retained committed
entries when storage became durable before receipt publication. Receipt changes
are fsynced as bounded append-only deltas in a generation-stamped request journal;
the atomic Raft metadata publishes the exact durable journal byte boundary. An
interrupted unpublished tail is ignored and truncated on recovery. Periodic atomic
checkpoints compact stale deltas without placing the complete receipt map back in
every Raft metadata rewrite. Receipt capacity is bounded; unexpired results are
never evicted to admit another request. Capacity exhaustion rejects admission
before preparing a mutation.

Every Raft connection performs a peer-contract Hello before its first RPC. The
peer node id and exact `requests.enabled`, `maxRetentionMs`, and
`maxTrackedRequests` values must match local policy; unknown nodes and mismatched
policies are rejected before RequestVote, AppendEntries, snapshot, or leadership
transfer processing. Thus a mismatched node cannot vote or contribute to quorum.
Disabling admission does not discard replicated receipts.

The pre-release Raft log/metadata and snapshot RPC layouts now include request
metadata and explicit proposal-final boundaries in place, without a version
bump, legacy reader, or migration. Existing Raft data directories from the
previous layout must be recreated; upgrade all participating nodes together. No
user data is automatically deleted.

### 15.10 Bounded Non-Raft Payload Transfer

Non-Raft `PLAIN` and `SECURE` replication use bounded physical frames (at most
64 KiB payload; the existing 128 MiB logical payload cap remains). Above
`transfer.thresholdBytes`, entries, Blob payloads, snapshot entries, owner-read
requests, and owner-read responses use `TRANSFER_BEGIN`, `TRANSFER_READY`, contiguous
offset-bearing `TRANSFER_CHUNK`, and `TRANSFER_END`. `TRANSFER_BEGIN` carries a
BLAKE2b-256 content identity and whole-payload CRC32C. The receiver durably syncs
each accepted chunk before advancing its restart offset; after reconnect it returns
the retained offset in `TRANSFER_READY`, and the sender continues from that byte.
Each frame retains its checksum/authentication. Interleaving,
invalid offsets/lengths, truncation, and a failed final checksum abort the connection
without applying the incomplete payload or acknowledging it complete.

Large outgoing payloads are staged once in a temporary file and shared by replica
queues instead of copied for each replica. Incomplete incoming payloads are retained
under a receiver/peer-scoped resume directory, so equal content sent concurrently to
different nodes cannot alias. The complete identity- and CRC-validated file is mapped
read-only for application and removed when that logical message is released. Invalid
partials are discarded; disconnected valid prefixes remain eligible for restart.
Startup and new-transfer admission remove expired partials and evict oldest unclaimed
partials above `maxResumeTransfers`. Retained bytes and capacity reserved for active
partials count against `maxSpoolBytes` across restart.

One runtime shares memory, spool-byte, and active-large-transfer budgets across
its server and clients. Exhaustion rejects admission or aborts the affected
connection instead of waiting while holding other transfer reservations. Existing
write timeout/ACK policy still determines the caller-visible write outcome.
Owner-read admission permits only one outstanding wire request per client,
including after its caller times out. A later read waits within its timeout for
the abandoned response to drain (or for disconnection), instead of flooding the
server's bounded read queue and disconnecting unrelated write ACK traffic.
Queue limits count logical message bytes per replica even though payload storage
is shared. Standalone server/client factories own budgets unless explicitly given
a shared budget. WAL-backed history is read from the history provider rather than
retained again in the server's live-only fallback history.

`maxMemoryBytes` bounds reserved transport payload/frame heap space, **not total
process RSS**: database data, application read-result buffers, allocator/container
overhead, and OS mapped-file/page cache are outside this limit. Spool storage is
bounded separately. Plain receive accounting distinguishes the encoded wire buffer
from the decoded payload lifetime; secure receive accounting additionally covers
ciphertext and plaintext overlap, and secure sends account for ciphertext allocation.
Operators must provision disk space and choose limits for
their maximum payload and concurrent replicas. Chunk size and staging threshold
may differ between peers within the fixed physical-frame limit. This pre-release
wire change requires upgrading all non-Raft peers together; no legacy large-frame
fallback is retained. STRIPE placement migration is unchanged.

## 16. API Servers and Wire Protocol

API serving is disabled unless `components.apiEnabled == true`. Low-level
`AkkEngineOptions::api.bindHost` defaults empty; enabling API serving without a
bind host fails. High-level `AkkaraDB::Options::ApiOptions` defaults bind host
to `127.0.0.1`.

### 16.1 Backends

`api.backends` may list `HTTP`, `TCP`, and/or `GRPC`. An empty backend list
requests all transports compiled into the loaded server backend. Availability
depends on build options and backend libraries.

Default ports:

| Backend | Default port |
|---|---:|
| HTTP | 7070 |
| TCP | 7071 |
| gRPC | 7072 |

The low-level engine API transport mode defaults to TLS. The high-level
`AkkaraDB::Options::ApiOptions` transport mode defaults to plain.

### 16.2 API TLS

API TLS options include:

- certificate path,
- private key path,
- CA path,
- pre-shared key bytes,
- PSK identity,
- peer verification toggle.

### 16.3 TCP Protocol

The TCP binary protocol is version 1.

Request header (`ApiRequestHeader`, 16 bytes):

| Field | Meaning |
|---|---|
| `magic[4]` | `AK1Q` |
| `version` | `1` |
| `opcode` | `ApiOp` |
| `requestId` | Caller request id |
| `keyLen` | Key byte length |
| `valLen` | Value/payload byte length |

Response header (`ApiResponseHeader`, 13 bytes):

| Field | Meaning |
|---|---|
| `magic[4]` | `AK1S` |
| `status` | `OK` (0), `NOT_FOUND` (1), `ROUTING_ERROR` (2, JSON payload), or `ERROR_STATUS` (255) |
| `requestId` | Echoed request id |
| `valLen` | Response payload byte length |

### 16.4 TCP Operations

| Opcode | Operation |
|---:|---|
| `0x01` | `GET` |
| `0x02` | `PUT` |
| `0x03` | `REMOVE` |
| `0x04` | `GET_AT` |
| `0x05` | `BATCH_PUT` |
| `0x06` | `BATCH_GET` |
| `0x07` | `PING` |
| `0x08` | `EXISTS` |
| `0x09` | `COUNT` |
| `0x0A` | `SCAN` |
| `0x0B` | `HISTORY` |
| `0x0C` | `ROLLBACK_TO` |
| `0x0D` | `ROLLBACK_KEY` |
| `0x0E` | `FORCE_SYNC` |
| `0x0F` | `FORCE_FLUSH` |
| `0x10` | `STATS` |
| `0x11` | `SCAN_STREAM` |
| `0x12` | `HISTORY_STREAM` |
| `0x13` | `RUN_BLOB_GC` |

TCP implementations validate framing, payload bounds, and CRC where applicable
before dispatch.

### 16.5 Server Limits

API options expose item/content/stream limits:

- HTTP: max batch items, scan items, history entries, max content length.
- TCP: accept queue limit/timeout, listen backlog, receive/send buffer sizes,
  pipeline batch limit, max batch items, max pending response bytes, read/write
  timeouts, `TCP_NODELAY`, keepalive, worker configuration.
- gRPC: worker threads, completion queues, min/max pollers, max concurrent
  streams, resource quota bytes, max batch items, scan items, history entries.

## 17. High-Level Typed API

`AkkaraDB` owns one `AkkEngine`. It provides typed table handles that encode
entities through BinPack and map operations onto raw KV records.

### 17.1 Opening

```cpp
auto db = akkaradb::AkkaraDB::open("data", akkaradb::StartupMode::FAST);
auto users = db->table<&User::id>("users");
```

`AkkaraDB::open(path, mode)` and `AkkaraDB::open(Options)` return an owning
database handle. `engine()` exposes the underlying raw engine. `close()`
delegates to engine shutdown.

### 17.2 Startup Modes

| Mode | Effective changes from low-level defaults |
|---|---|
| `ULTRA_FAST` | Disable WAL, Blob, Manifest, SST, VersionLog; disable forced flush/sync on close; use 512 MiB MemTable threshold per shard |
| `FAST` | WAL async; VersionLog disabled; enable SST read promotion; use 256 MiB threshold per shard |
| `NORMAL` | WAL async |
| `DURABLE` | WAL sync and VersionLog enabled |

High-level overrides can set MemTable threshold; VersionLog enablement, codec,
Zstd level, write admission, serial append mode, parallel lane count, parallel
pending-limit scope, and retention boundaries; SST/Blob codec; SST Zstd compression level; Blob threshold;
SST read promotion; Bloom density; and the L0 SST trigger.

### 17.3 Table Handles

`table<&Entity::primaryKey>(name)` returns a move-only `PackedTable`. It stores:

- engine pointer,
- table name,
- table prefix,
- primary-key-to-row-id prefix,
- row-id-to-primary-key prefix,
- next-row-id key,
- registered secondary indexes,
- prefix indexes,
- reference and foreign-key bindings,
- temporary arena-backed buffers.

`PackedTable` is not documented as safe for concurrent shared compound use.
Use separate handles or external synchronization when multiple threads compose
table-level operations.

### 17.4 Entity Declaration

`AKKARADB_QUERYABLE(Type, fields...)` defines query-column proxy metadata.
`AKKARADB_ENTITY(Type, PrimaryKey, fields...)` registers both `RefTraits` and
query metadata.

The primary key field cannot be `Immutable<T>`.

### 17.5 Table Operations

Packed-table functionality includes:

- primary-key put/get/remove,
- `exists`,
- `getInto`,
- table scan,
- secondary-index registration and exact lookup,
- prefix-index registration for string-like fields,
- query planning/evaluation helpers,
- joins,
- stable row-id lookup,
- primary-key updates,
- reference binding,
- foreign-key actions,
- field update hooks,
- immutable persisted fields.

Secondary index maintenance, row-id metadata writes, foreign-key actions, and
primary record updates are composed from ordinary engine mutations. They are not
transactions.

### 17.6 RowId and References

`RowId` is currently `uint64_t`. It is allocated on first insert and remains
stable across primary-key updates. `Ref<T>` may resolve by primary key or stored
row id depending on how it was created and bound.

`Schema` registers tables and foreign-key actions. It binds table-level
reference lookups after registration. Dereferencing a detached reference or a
missing target throws.

### 17.7 Foreign-Key Actions

Supported delete/update actions:

- `Cascade`,
- `Restrict`,
- `SetNull`.

Only one action may be selected for a single delete/update rule. Conflicting
action sets are rejected.

Foreign-key owner and target tables must be registered in the schema. Ref-field
foreign keys currently require the target primary key.

## 18. BinPack and Query Model

BinPack is the typed serialization layer used by `PackedTable` and JVM/schema
wire helpers.

### 18.1 BinPack Facade

`binpack::BinPack` exposes:

- `encode<T>(value) -> std::vector<uint8_t>`,
- `encodeInto<T>(value, out)`,
- `decode<T>(bytes) -> T`,
- `decodeInto<T>(bytes, out) -> bool`,
- `estimateSize<T>(value)`.

Concrete encoding behavior is defined by `TypeAdapter<T>`.

### 18.2 Struct Metadata

Member names and declaring classes are derived through the local type-traits and
macro infrastructure. Queryable fields create column proxies so typed query
expressions can be built without stringly typed field references.

### 18.3 Query Helpers

The query layer builds typed expression trees and plan objects. It can use
secondary indexes and prefix indexes when a supported predicate maps to an
ordered byte range. Otherwise it evaluates predicates over decoded rows from
the relevant scan.

Native query bytecode support exists for the JNI/query rewrite path. It is a
build/runtime feature on top of the same raw records and BinPack/schema
contracts, not a separate storage model.

## 19. JVM and JNI Integration

The JVM project uses Kotlin modules:

- `akkara/core`: Byte buffers, BinPack, query AST/DSL, serializer helpers.
- `akkara/engine`: engine facade, JNI/native handles, TCP/HTTP/gRPC clients,
  options/schema serializers, coroutines helpers.
- `akkara/plugin`: Gradle/Kotlin compiler plugin support.

The native JNI bridge under `akkara/jni` exports engine/table operations,
options payload decoding, schema and row-value codecs, query bytecode runtime,
stats objects, and scan history exports.

JVM integration shares these native contracts:

- raw engine key/value semantics,
- BinPack row encoding,
- schema/query payload conventions,
- native option resolution,
- engine statistics shape,
- native lifecycle/failure behavior.

The JNI ABI and compatibility line are tracked by version properties and encoded
in JNI artifact metadata.

## 20. Configuration Reference

This section lists public configuration surfaces. Defaults are from current
headers unless a high-level `StartupMode` modifies them.

### 20.1 `AkkEngineOptions::Components`

| Field | Default |
|---|---:|
| `walEnabled` | `true` |
| `blobEnabled` | `true` |
| `manifestEnabled` | `true` |
| `sstEnabled` | `true` |
| `versionLogEnabled` | `false` |
| `clusterEnabled` | `false` |
| `apiEnabled` | `false` |

### 20.2 Runtime Options

| Field | Default | Meaning |
|---|---:|---|
| `writerThreads` | 0 | Sizing hint for writer-related sharding |
| `recoverWal` | `true` | Replay WAL during open |
| `truncateCorruptWalOnRecovery` | `false` | Truncate corrupt WAL tails and remove later same-shard segments during WAL recovery |
| `ignoreVersionLogSupplementErrors` | `false` | Continue with WAL valid prefix if VersionLog supplement fails during corrupt-WAL recovery |
| `recoverSst` | `true` | Recover SST state during open |
| `pruneWalOnFlush` | `true` | Remove WAL segments made obsolete by flush checkpoint |
| `forceFlushOnClose` | `true` | Force MemTable flush during close |
| `forceSyncOnClose` | `true` | Force WAL/VersionLog sync during close |
| `sstPromoteReads` | `false` | Promote SST hits back into MemTable |
| `writePolicy` | `CUSTOM` | Preset resolver |
| `writeAdmission` | `AUTO` | Serialized/concurrent admission |
| `parallelWriteOrder` | `APPLY_ORDER` | Same-key ordering policy for parallel admission |
| `writeDurability` | `SYNCED` | WAL acknowledgement boundary |
| `writeVisibility` | `COMMIT_ORDER` | Legacy visibility field |
| `visibility.readVisibility` | `AUTO` | Explicit read visibility override |
| `sequence.allocation` | `GLOBAL_ATOMIC` | Sequence allocation strategy |
| `sequence.threadLocalRangeSize` | 1 | Per-thread range size |
| `sequence.commitWindowSize` | 0 | Commit window, 0 means default |
| `backpressure.maxMemtableImmutableTables` | 0 | Immutable backlog throttle |
| `backpressure.maxSstL0Files` | 0 | L0 backlog throttle |
| `backpressure.waitMicros` | 100 | Blocking poll interval |
| `backpressure.timeoutMs` | 30000 | Blocking timeout; 0 means unbounded |
| `relaxedConcurrentWrites` | `false` | Enable in-memory concurrent fast path when safe |
| `generationLayoutEnabled` | `false` | Use active-generation storage layout |

### 20.3 API Options

Low-level API defaults:

| Field | Default |
|---|---:|
| `httpPort` | 7070 |
| `tcpPort` | 7071 |
| `grpcPort` | 7072 |
| `httpMaxBatchItems` | 4096 |
| `httpMaxScanItems` | 4096 |
| `httpMaxHistoryEntries` | 4096 |
| `httpMaxContentLength` | 64 MiB |
| `tcpAcceptQueueLimit` | 4096 |
| `tcpAcceptQueueTimeoutMs` | 60000 |
| `tcpListenBacklog` | 1024 |
| `tcpPipelineBatchLimit` | 64 |
| `tcpMaxBatchItems` | 4096 |
| `tcpMaxPendingResponseBytes` | 8 MiB |
| `tcpReadTimeoutMs` | 60000 |
| `tcpWriteTimeoutMs` | 30000 |
| `tcpNoDelay` | `true` |
| `tcpKeepAlive` | `true` |
| `transportMode` | `TLS` |

High-level `AkkaraDB::Options::ApiOptions` changes the bind/transport defaults
to `bindHost = 127.0.0.1` and `transportMode = PLAIN`.

### 20.4 Cluster Runtime Options

| Field | Default | Meaning |
|---|---:|---|
| `transportMode` | `SECURE` | Replication transport mode |
| `requests.enabled` | `false` | Opt in to retry-safe native request APIs; only valid with `RAFT_QUORUM` |
| `requests.maxRetentionMs` | 86400000 | Maximum request lifetime in milliseconds; positive, at most 365 days |
| `requests.maxTrackedRequests` | 4096 | Unexpired applied/in-flight request capacity; 1–32768 so snapshot request state fits one protocol-v1 Raft frame; reject rather than evict |
| `raftSnapshot.minLogEntries` | 4096 | Committed entries since the prior snapshot that trigger Raft log compaction; 1–1000000000 |
| `raftSnapshot.minLogBytes` | 67108864 | Approximate committed Raft-log bytes that trigger compaction; positive, at most 1 TiB |
| `raftSnapshot.maxIntervalMs` | 300000 | Maximum interval before compacting nonempty committed log state; positive, at most 7 days |
| `raftHeartbeatIntervalMs` | 100 | Raft leader heartbeat interval in milliseconds; 10-1000. Larger values reduce idle traffic but approach the 3-second minimum election timeout |
| `raftMaxForwardRequests` | 64 | Concurrent independent Raft forwarding channels per runtime; 1–256. Admission waiting consumes the original forwarding deadline |
| `raftMaxReceiveMemoryBytes` | 67108864 | Aggregate inbound/outbound Raft frame receive/decode reservation; from three maximum 4 MiB frame buffers (`3 * (14 + 4194304)` bytes) through 64 GiB |
| `memoryOnlySnapshot.mode` | `COMPLETION_FIRST` | SST-disabled snapshot admission: `THROUGHPUT_FIRST` rejects instead of waiting for unavailable capacity; `COMPLETION_FIRST` waits and prioritizes completion |
| `memoryOnlySnapshot.maxPinnedBytes` | 536870912 | Conservative sum of memory-only generations admitted to concurrent snapshots; 1 MiB-1 TiB. `COMPLETION_FIRST` permits one snapshot larger than this limit |
| `memoryOnlySnapshot.maxPinnedGenerations` | 2 | Maximum concurrent memory-only snapshot pins; 1-64 |
| `transfer.thresholdBytes` | 32768 | Non-Raft logical payload staging/chunk threshold; 1–65536 bytes |
| `transfer.chunkBytes` | 32768 | Non-Raft chunk data size; 1–65528 bytes |
| `transfer.maxConcurrentTransfers` | 8 | Shared active large send/receive limit; 1–1024 |
| `transfer.maxMemoryBytes` | 8388608 | Shared transport heap reservations; at least 524288 bytes, not a process RSS limit |
| `transfer.maxSpoolBytes` | 1073741824 | Shared queued/incoming temporary payload bytes; positive |
| `transfer.spoolDirectory` | empty | Temporary staging directory; empty selects the OS temporary directory |
| `transfer.resumeEnabled` | true | Persist incomplete incoming logical payloads for reconnect resume |
| `transfer.resumeRetentionMs` | 900000 | Unclaimed partial retention; 1 ms–7 days |
| `transfer.maxResumeTransfers` | 64 | Retained partial count per spool directory; 1–65536 |
| `transfer.resumeHandshakeTimeoutMs` | 5000 | Wait for the receiver's durable offset; 1–300000 ms |
| `replBindHost` | `0.0.0.0` | Local replication listener bind host |
| `startupRole` | `AUTO` | Startup role selection |
| `primaryHost` | empty | Replica-side primary host override |
| `primaryReplPort` | 0 | Replica-side primary port override |
| `primaryNodeId` | 0 | Replica-side primary node id assertion/override; for non-Raft `MIRROR` it must match the persistent configured Primary |
| `clusterGroupId`, `clusterGroupEpoch` | 0 | Non-Raft group identity override. MIRROR creates persisted Primary defaults when zero; PARTITIONED/STRIPE require the same explicit nonzero group id on every node |
| `clusterMembershipPath` | empty | Non-Raft primary group state / replica membership state path |
| `resetClusterMembership` | `false` | Explicitly allow a valid replica membership switch |
| `mirrorPromotion.enabled` | `false` | Explicitly authorize one offline non-Raft MIRROR Primary transition; must be supplied to the candidate and replicas for that transition |
| `mirrorFencing.mode` | `STATIC` | `STATIC` rejects promotion; `QUORUM_FENCED` uses authority-only Raft; `EXTERNAL_FENCED` requires a provider (section 15.4) |
| `mirrorFencing.authorityNodes` | empty | QUORUM_FENCED only: same ids/hosts as all configured nodes, dedicated replication ports; includes witnesses |
| `mirrorFencing.external` | null | Required only for EXTERNAL_FENCED authority; embedding must implement real fencing |
| `mirrorRecovery.mode` | `BLOCK` | Unconfirmed quorum grants: BLOCK, MANUAL via recoverMirrorWrite/promotion, or AUTOMATIC background retry; matching receipts always recover automatically |
| `mirrorRecovery.provider` | null | Required for MANUAL/AUTOMATIC; must establish the requested guarantee for the exact generation/operation; MirrorFencingRecoveryProvider adapts an existing promotion fencer |
| `mirrorRecovery.retryIntervalMs` | 1000 | Automatic retry interval [50, 3600000] ms; independent of data replication acknowledgement policy |
| `mirrorPromotion.previousPrimaryNodeId`, `previousGroupEpoch`, `expectedDurableSeq` | 0 | Expected prior membership and exact candidate durable sequence. The new Primary advances the epoch by one; replicas verify the same transition. |
| `corruptStateAction` | `FAIL_STARTUP` | Corrupt small non-Raft cluster state files fail startup unless explicitly backed up/deleted and recreated. Raft hard state is always fail-fast and never resets persisted term/vote |
| `raftLogRecoveryAction` | `FAIL_STARTUP` | `TRUNCATE_UNCOMMITTED_TAIL` may discard corrupt segment entries only beyond an intact committed prefix; corrupt log metadata or committed entries always fail startup |
| `raftBlobPolicy` | `REJECT` | `RAFT_QUORUM` rejects Blob payload replication by default; `PRIMARY_SIDE_ONLY` explicitly allows primary-local Blob payloads outside Raft quorum; `RAFT_LOG` commits Blob chunks and the Blob-reference mutation as one Raft proposal and uses reserved Blob-id namespaces so Raft-log Blob ids encode a 16-bit node id plus 46-bit seq, while snapshot Blob ids encode a 40-bit seq plus 22-bit snapshot-entry ordinal |
| `raftBlobChunkSizeBytes` | 1048576 | Maximum payload bytes per automatic `RAFT_LOG` Blob and Raft snapshot chunk; must fit one Raft frame. Snapshot install streams entries into receiver staging instead of buffering the full snapshot in memory, records CRC32C per staged entry, externalizes large staged values to Blob refs before WAL finish, and finish recovery is WAL-transactional via snapshot record/commit markers |
| `maxReplicaQueueFrames` | 16384 | Per-replica outbound frame queue limit for non-Raft live and bootstrap replication; bootstrap waits for capacity; 0 disables the frame-count limit |
| `maxReplicaQueueBytes` | 268435456 | Per-replica outbound queued wire-byte limit for non-Raft live and bootstrap replication; one bootstrap message larger than this requires resynchronization; 0 disables the byte limit |
| `secure.identitySeedPath` | empty | Persistent identity seed path |
| `secure.pinnedPeers` | empty | Peer public-key pins. `transportMode=SECURE` requires a pin for every accepted or dialed peer; use `PLAIN` only for explicitly unauthenticated test/local transport |
| `secure.expectedPrimaryNodeId` | 0 | Expected primary id or unknown |

## 21. Statistics

`AkkEngine::stats()` is `noexcept` and returns a best-effort diagnostic snapshot.
It is not a synchronized transactionally consistent database observation.

Top-level stats include:

- current sequence snapshot,
- node id,
- put/remove/get/exists/scan counters,
- MemTable/SST hit/miss counters,
- Blob put counter,
- resolved configuration enum values,
- backpressure counters,
- memory-only snapshot-generation consolidation completions/failures, the
  monotonic-microsecond timestamp of the last failure, and whether consolidation
  remains pending,
- API server counters,
- cluster health/last failure, data-Raft term/index/leader, STRIPE metadata-Raft
  term/index/leader/snapshot, per-peer lag/connection/last successful contact,
  proposal/replication queues, retry-result/journal,
  foreign-cluster and policy rejection, lease/endpoint/read-timeout, and
  transfer-budget counters,
- MemTable snapshot,
- WAL snapshot,
- Blob snapshot,
- SST levels and compaction snapshot,
- VersionLog snapshot.

Stats remain intentionally available after certain background failures so
operators can inspect unhealthy WAL, pending queues, compaction failures, and
API counters.

The VersionLog portion exposes both configuration echoes and operational
counters:

| Field | Meaning |
|---|---|
| `enabled` | VersionLog component is active |
| `syncMode`, `groupN`, `groupMicros`, `groupBytes`, `asyncMaxPendingBytes` | Resolved write/sync configuration |
| `indexedKeys`, `indexedEntries`, `rollbackEntries` | Resident and persisted historical-entry index counters |
| `pendingWrites`, `pendingBytes` | Serial async/batched queue depth |
| `durableBytes` | Bytes accepted into recovered/known-written VersionLog segments after known pruning; in ASYNC live operation this is a diagnostic byte count and crash recovery is bounded by `.akvtail` or the validated active prefix |
| `segmentCount`, `activeSegmentBytes` | Current segment directory size and active segment byte count |
| `flushThreadRunning` | Serial async/batched flush worker is alive |
| `recoveryDurationMicros`, `recoveredSegmentCount`, `recoveredEntryCount` | Latest recovery pass duration and accepted segment/entry counts |
| `sidecarFallbackCount`, `sidecarRebuildFailures` | Derived sidecar read fallback and regeneration failure counters |
| `retentionPrunedSegments`, `retentionBaseEntriesWritten` | Retention progress counters |
| `parallelQueueRejects`, `parallelLaneCount`, `parallelPendingWrites`, `parallelPendingBytes` | `PARALLEL` admission and lane queue diagnostics |

HTTP and TCP stats encode new VersionLog fields after the existing
`flushThreadRunning` byte. gRPC exposes the same fields as appended protobuf
fields. Consumers should ignore fields they do not understand when they are on
an older compatibility line.

## 22. Threading and Lifecycle

### 22.1 Thread Safety

Public `AkkEngine` operations are thread-safe. Internal components use their
own synchronization appropriate to their role:

- MemTable shards protect ordered state and immutable queues.
- WAL append/sync/close paths are synchronized and may use async workers.
- Manifest public methods are thread-safe.
- SST background compaction reports stored failures.
- VersionLog async/batched writers serialize persistence. `PARALLEL` instead
  persists directly through independently locked lanes; `BACKGROUND` recovery
  uses a separate validation/index-rebuild worker.
- API servers own transport-specific accept/worker state.
- Cluster runtime serializes lifecycle state changes; blocking endpoint I/O uses
  retained endpoint handles after releasing the runtime lifecycle mutex.

`PackedTable` is move-only and contains mutable temporary buffers. It should not
be shared for unsynchronized compound table work.

### 22.2 Background Work

Depending on configuration, background work can include:

- WAL async flushers,
- MemTable immutable flush workers,
- memory-only snapshot-generation consolidation and bounded-backoff retry,
- SST compaction workers,
- Manifest fast-mode flusher,
- VersionLog async/batched flusher, `PARALLEL` lane writers, and optional
  `BACKGROUND` recovery worker,
- Blob GC,
- API accept/connection workers,
- cluster replication manager, endpoints, and disconnected-replica reaper,
- Raft peer workers and proposal-batch flusher,
- STRIPE owner intent recovery/garbage collection.

### 22.3 Close

`AkkEngine::close()` is idempotent. The first closer:

1. Marks the engine closing/closed and rejects new regular operations.
2. Requests WAL shutdown.
3. Waits for active public operations and scan generators.
4. Performs configured MemTable force flush.
5. Performs configured WAL/VersionLog force sync.
6. Runs configured Blob GC.
7. Stops API/cluster/background components.
8. Releases storage components.
9. Rethrows the first captured close failure after best-effort cleanup.

STRIPE garbage collection is stopped and joined before cluster transport and
storage are released. Cluster close detaches endpoint ownership under its
runtime lock, then shuts down and joins endpoints outside that lock. Outbound
clients are stopped before waiting for server read workers so callbacks waiting
on remote reads can unwind.

`stats()`, `forceSync()`, and `forceFlush()` safely return empty/no-op results
when they race a completed close as implemented.

## 23. Error Handling and Recovery

### 23.1 Public Error Policy

Expected absence is represented by `std::optional` or `bool`. Configuration,
I/O, corruption, closed-handle, invalid topology, and unsupported-backend cases
throw standard exceptions, typically `std::invalid_argument` or
`std::runtime_error`.

| Case | Public behavior |
|---|---|
| Missing key | `get`/typed `get` return `std::nullopt`; `getInto`/`exists` return `false` |
| Empty scan/query/history | Empty iterator/vector |
| Closed engine | Regular operations throw |
| Invalid configuration | Open or setter boundary throws |
| Corrupt required persisted data | Throws rather than silently accepting |
| VersionLog disabled | `getAt` returns missing, `history` empty, rollback throws |
| Cluster range/history read | Full public result under the selected authority/read-mode contract, or explicit owner/quorum/cursor/resource failure; no successful incomplete fallback |
| Unregistered index lookup | Typed API throws |
| Iterator `next()` after exhaustion | Throws `std::out_of_range` in typed iterators |
| Detached/missing `Ref<T>` target | Throws |

`core::Status` is an internal lightweight status value for hot subsystems. It
does not make the public native API zero-exception.

### 23.2 Open Ordering

Subject to enabled components, `open`:

1. Resolves write policy and visibility.
2. Activates generation layout when configured.
3. Derives paths from `dataDir`.
4. Applies runtime sizing hints.
5. Normalizes MemTable/WAL/SST options.
6. Validates component combinations.
7. Creates required directories.
8. Loads or generates the local node id.
9. Opens and starts Manifest.
10. Recovers SST state when enabled.
11. Creates MemTable.
12. Replays WAL when enabled and configured.
13. Advances recovered sequence state.
14. Starts WAL/Blob/VersionLog/SST managers.
15. Starts cluster runtime when enabled.
16. Starts API server when enabled.

If a requested server factory or transport backend cannot be loaded, open fails.
API serving enabled without a bind host fails. VersionLog enabled without a log
path after path derivation fails.

### 23.3 Recovery Semantics

After WAL replay, the engine compares recovered MemTable and SST sequence
state, advances the allocator as needed, and initializes commit-order visibility
from the recovered maximum. This prevents sequence reuse after recovery.

WAL recovery accepts only a contiguous valid prefix. SST and Manifest recovery
validate their own headers, records, CRCs, and lifecycle state. A compaction
output without a committed Manifest replacement remains orphaned instead of
replacing its inputs.

### 23.4 Failure Propagation

The engine does not hide:

- async WAL failure,
- async MemTable flush failure,
- SST background compaction failure,
- required component I/O failure,
- corrupt required persisted data.

Stored failures are checked at public operation boundaries and rethrown. Close
still attempts best-effort teardown.

## 24. Persistence and Wire Format Reference

All persisted integer fields are serialized little-endian unless a format
explicitly states otherwise. CRC32C uses the Castagnoli polynomial with hardware
dispatch where available.

### 24.1 Magic and Version Summary

| Artifact | Magic | Version | Integrity boundary |
|---|---|---:|---|
| WAL segment | `AKWA` | 1 | Header CRC and entry CRC |
| SST file | `AKS1` | 1 | Header, metadata, footer, and block CRCs |
| SST footer | `A1SF` | 1 | Footer CRC |
| Blob file | `AKB1` | 1 | Header CRC and original-content CRC |
| Manifest | `AMV1` | 1 | Header CRC and per-record payload CRC |
| VersionLog segment | `AKV1` | 1 | File-header and entry CRCs |
| VersionLog sidecar index | `AKVI` | 1 | Header CRC and payload CRC |
| VersionLog durable tail | `AKVT` | 1 | Tail-header CRC |
| Deferred rollback journal | `AKRJ1` | 1 | Whole-file CRC32C and atomic replacement |
| Cluster config | `AKC1` | 1 | Config CRC |
| Raft hard state | `AKRS1` | Encoded in magic | Whole-file CRC32C |
| Raft log metadata | `AKRL1` | No separate version field | Whole-metadata CRC32C |
| Raft log segment | No file magic | No separate version field | Per-entry payload CRC32C |
| Raft request journal | `AKRQ1` | No separate version field | Per-record payload CRC32C plus metadata-published byte boundary |
| TCP API request | `AK1Q` | protocol 1 | Transport framing and payload validation |
| TCP API response | `AK1S` | protocol 1 | Transport framing and payload validation |

The serializer/deserializer implementations are the byte-layout authority:

- `WalFraming.hpp/.cpp`
- `SSTFormat.hpp`, `SSTWriter.cpp`, `SSTReader.cpp`
- `BlobFraming.hpp/.cpp`
- `ManifestFraming.hpp/.cpp`
- `VersionLog.cpp`
- `AkkEngine.cpp` (deferred rollback journal and STRIPE metadata/intent payloads)
- `ClusterConfig.cpp`
- `RaftConsensusRuntime.cpp`
- `ApiFraming.hpp/.cpp`

### 24.2 WAL Segment Header

`WalSegmentHeader` is 48 bytes:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | `segmentId` |
| 8 | 8 | `createdUs` |
| 16 | 8 | `firstSeq` |
| 24 | 8 | `lastSeq` |
| 32 | 4 | `magic` |
| 36 | 4 | `crc32c` |
| 40 | 2 | `version` |
| 42 | 2 | `headerSize` |
| 44 | 2 | `shardId` |
| 46 | 2 | `flags` |

### 24.3 WAL Entry Header

`WalEntryHeader` is 32 bytes followed by key and value:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | `seq` |
| 8 | 8 | `keyFp64` |
| 16 | 4 | `entryLen` |
| 20 | 4 | `valueLen` |
| 24 | 2 | `keyLen` |
| 26 | 2 | `flags` |
| 28 | 4 | `crc32c` |

### 24.4 Blob Header

`AkBlobHeaderV1` is 48 bytes:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | `magic` |
| 4 | 2 | `version` |
| 6 | 2 | `headerSize` |
| 8 | 4 | `flags` |
| 12 | 4 | `codec` |
| 16 | 8 | `blobId` |
| 24 | 8 | `totalSize` |
| 32 | 8 | `storedSize` |
| 40 | 4 | `contentCrc32c` |
| 44 | 4 | `headerCrc32c` |

### 24.5 SST v1 File Shape

An SST v1 file contains:

```text
[SSTFileHeaderV1:256]
[data blocks...]
[block index entries...]
[key arena...]
[Bloom filter...]
[SSTFooterV1:48]
```

Fixed structures:

| Structure | Size |
|---|---:|
| `SSTFileHeaderV1` | 256 |
| `SSTBlockHeaderV1` | 64 |
| `SSTBlockIndexEntryV1` | 72 |
| `SSTBloomHeaderV1` | 16 |
| `SSTFooterV1` | 48 |

Per-file flags include block Zstd and metadata CRCs. All SST files must checksum
the block index, key arena, and Bloom filter; files without metadata checksums
are rejected. Per-block flags include compressed, raw, and prefix-compressed.
Record flags mirror tombstone and Blob status.

### 24.6 Manifest

Manifest files start with a 32-byte file header and then a sequence of records.
Each record has an 8-byte prefix:

```text
[type:u8][flags:u8][payloadLen:u16le][crc32c:u32le][payload]
```

Record payloads encode SST lifecycle, checkpoint, compaction, truncation, and
cluster lifecycle events.

### 24.7 VersionLog

Each `.akvlog` segment starts with this 32-byte `AKV1` v1 header:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | `magic` (`AKV1`) |
| 4 | 2 | `version` (`1`) |
| 6 | 1 | `syncModeHint` |
| 7 | 1 | reserved |
| 8 | 8 | `createdNs` |
| 16 | 8 | reserved |
| 24 | 4 | header `crc32c` |
| 28 | 4 | reserved |

It is followed by variable-sized entries:

```text
[AkvlogV1EntryHeader:43][key:keyLen][stored value:valueLen][entry CRC32C:u32]
```

The packed entry header contains `entryLen`, `seq`, `sourceNodeId`,
`timestampNs`, `flags`, `keyFp64`, `keyLen`, and `valueLen`. `entryLen` covers
the complete entry, including its trailing CRC. With `VLOG_FLAG_ZSTD`, the
stored value begins with a four-byte uncompressed size followed by the Zstd
payload. Public `VersionEntry` values are decompressed and do not expose that
internal flag.

Each VLog segment may have a derived sibling `.akvidx` file. It begins with a
44-byte `AKVI` v1 header containing the Bloom hash count, authoritative VLog
byte length, key/version counts, Bloom bit count, payload CRC32C, and header
CRC32C. Its payload is `[Bloom bytes][24-byte fingerprint directory
records][16-byte (seq, VLog-offset) records]`. The directory is ordered by key
fingerprint; a lookup verifies the referenced VLog key before trusting a
fingerprint match. A missing, stale, or invalid sidecar is ignored for that
query; recovery, segment rotation, and clean close attempt to regenerate it
from the authoritative segment.

`PARALLEL` mode additionally maintains a 24-byte `.akvtail` (`AKVT` v1)
record beside each active lane segment. It contains the committed byte length
and a CRC32C. Recovery validates that length against the physical file and
scans only the recorded prefix; bytes after it are interrupted, unpublished
tail data and are ignored.

VLog corruption is never accepted. `EAGER` recovery reports it from `open`;
with `BACKGROUND` recovery the first operation waiting on recovery receives the
stored error instead.

The deferred rollback journal begins with `AKRJ1`, a little-endian task count,
and variable-sized tasks. Each task stores 16-byte task, operation, scheduling
startup, and cluster ids; the configuration epoch; action, cluster, and conflict
bytes; target node, target sequence, and planning watermark; and a length-prefixed
key. A trailing CRC32C covers every preceding byte. Unknown actions, invalid
policies, truncation, trailing bytes, and checksum failures reject `open`.

### 24.8 Cluster Config

Cluster config is a CRC-protected `AKC1` v1 binary file. It stores persistent
membership, placement, acknowledgement, consistency, Raft, stripe, and the
fixed non-Raft `MIRROR` Primary settings. The fixed header is 72 bytes, with the
STRIPE data-shard, parity-shard, and copies-per-shard bytes at offsets 40, 41,
and 42, the nonzero little-endian `uint16_t partitionReplicationFactor` at
byte offset 44, failover policy at byte offset 43, the stable `uint16_t partitionCount` at byte offset 46,
the little-endian `uint64_t primaryNodeId` at byte offset 48, and the
16-byte cluster id at byte offset 56. Each node has a 22-byte fixed prefix containing node id,
capabilities, data port, normal replication port, STRIPE metadata-Raft port,
PARTITIONED replication base port, and host length, followed by the host bytes. Runtime transport settings remain
out-of-band. `AKC1` readers reject CRC mismatches, oversized host names,
truncation, and trailing bytes.

Non-Raft primary group state and replica membership state use CRC-protected
`AKCG1` little-endian binary files containing group id, Primary node id, and
group epoch. Ordinary startup cannot change the persisted Primary. Explicit
offline MIRROR promotion preserves the group id and advances the other two
fields atomically. Raft persisted term/vote hard state uses the
41-byte CRC-protected `AKRS1` layout: magic, 16-byte cluster id, term, voted-for
node id, and CRC32C. A valid hard-state file carrying another cluster id fails
startup. Raft's `cluster-raft.log` is an `AKRL1` CRC32C-protected
metadata file containing commit/applied/snapshot/membership state and the valid
byte extents in `cluster-raft.log.segments/segment-<id>.akrl`. Segment records
contain a little-endian `uint64_t` payload length, `uint32_t` payload CRC32C, and
encoded entry. Metadata references segment id/begin/end byte extents. Segments are
append-only, rotating at 16 MiB (one larger entry may occupy its own segment).
Index-only state changes rewrite metadata, not entry payloads.
Metadata also stores the latest membership configuration generation and the
local learner bootstrap allowance. Snapshot RPCs and durable install intents
carry the configuration generation together with the authoritative member sets.

Retry-safe results are stored separately in generation files named
`cluster-raft.requests.<generation>`, beginning with `AKRQ1`. The Raft metadata
publishes generation, exact byte length, and record count. Journal records are
length/CRC-framed full checkpoints or incremental expiry/upsert deltas. The
journal is synced before its new boundary is published; unreferenced generations
and unpublished tail bytes are collected only after a valid metadata boundary is
known. A referenced missing, truncated, or corrupt request journal fails startup.

Segments are synced before metadata is atomically replaced. Conflicting
uncommitted suffixes are written into a new segment; the old durable suffix
remains intact until replacement metadata is published. Compaction advances
the referenced extents and unreferenced segment files are collected afterward.
The in-memory location/extent index is updated by appended entries and discarded
prefix/suffix ranges. Normal metadata publication does not rebuild every retained
entry location. Replication and apply batches start at their contiguous log-index
offset. Entry shape and sequence validation is cached for the immutable prefix;
conflict replacement invalidates only the changed suffix, while startup and
snapshot compaction validate the retained log against their new base. Ordinary
AppendEntries rollback copies only the affected suffix, and apply-progress rollback
keeps the unchanged log prefix.

Partially referenced segments retain their unused prefix until the entire
segment is unreferenced; compaction does not rewrite retained entry payloads.
Bytes beyond the published extents are interrupted appends and ignored.
Metadata corruption always fails startup. Entry corruption also fails by default;
the explicit tail-recovery policy may discard only entries strictly beyond the
committed prefix. Uncertain persistence errors stop the runtime until reopen.
The current log layout is version 1 (`AKRL1`). No legacy or dual-format reader
is provided. Existing data is never automatically deleted.

Earlier pre-release cluster configs and `AKRS2` hard state are intentionally
incompatible with this initial format. `AKC1` includes the dedicated STRIPE
metadata-Raft port and has no legacy or dual-format reader. The current Raft peer hello is version 2 and
uses the `AKRP2` compatibility-fingerprint domain. Raft mutation records carry
the original timestamp; Forward QUERY_REQUEST carries one key entry. Earlier peer-hello/log
layouts, including development builds that used version 1 or 2, have no
compatibility bridge because request metadata and proposal-final boundaries were
changed in place. Upgrade every node and its core/cluster backend together;
keep one matching v1 config and recreate existing Raft data directories. No dual-format reader or
rolling-upgrade bridge is provided.

### 24.9 Endianness Exceptions

Most persisted integer fields are little-endian. Typed table integral key bytes
and sortable index field bytes deliberately use big-endian sortable transforms
so bytewise engine scans preserve numeric order.

## 25. Testing and Benchmarks

When `AKKARADB_BUILD_TESTS=ON`, the native build defines smoke and benchmark
targets including:

- API server smoke/test executables,
- TCP API throughput benchmark,
- MemTable throughput benchmark,
- AkkEngine B+Tree put benchmark,
- WAL throughput benchmark,
- SSTable throughput benchmark,
- SSTable Bloom negative lookup benchmark,
- cluster observability contract test,
- cluster smoke test,
- MemTable lifecycle smoke test,
- VersionLog admission/visibility smoke test,
- VersionLog recovery/concurrency smoke test,
- VersionLog maintenance-tool smoke test,
- engine recovery smoke test,
- WAL async failure smoke test,
- SST snapshot visibility smoke test.

Throughput benchmarks are measurement tools, not correctness specifications.
Smoke tests cover representative correctness contracts for recovery, async WAL
failure propagation, snapshot visibility, MemTable lifecycle, cluster behavior,
and API framing.

Native cluster smoke coverage includes:

- ordered STRIPE metadata ranges over binary keys and different key lengths,
  count independent of spool quotas, bounded metadata/result capture and cleanup,
  SST/restart, concurrent RPC admission in PLAIN/SECURE, socket-lock contention,
  and stalled transfer
  READY, physical send backpressure, expired zero budgets, and recovery after
  local encoding errors under the caller's total budget
  (`akkaradb_cluster_query_resource_test`),
- non-Raft cluster ID/configuration admission in MIRROR, PARTITIONED and STRIPE
  over PLAIN/SECURE, independent server-hello rejection before membership
  persistence, canonical policy fingerprints excluding mutable STRIPE topology
  and RAID.10 pair order, and STRIPE
  failover restart, orphan-lease handoff and stale-token rejection without a
  metadata-leader term change (`akkaradb_cluster_authority_contract_test`),
- repeated replica disconnect/reconnect cycles followed by successful replication,
- fixed MIRROR Primary config round trips, capability validation, and rejection
  of a second Primary under the same config; offline MIRROR promotion additionally
  covers rejection without authorization, stale-candidate rejection, atomic
  Primary/epoch transition, successor acceptance, and routed reads after demotion,
- MIRROR STATIC promotion rejection, external-fence failure without epoch advance,
  authority-only quorum with ASYNC data and a witness, quorum loss before local
  application, plain/secure transport, control snapshot/restart, unresolved-grant
  persistence and promotion rejection, external provider recovery, durable-receipt
  release recovery, obsolete-generation frame rejection, and stale-Primary
  restart without write permission (`akkaradb_mirror_fencing_test`),
- MIRROR recovery option validation, manual recovery with ASYNC and PRIMARY_ACK,
  unresolved/invalid/stale proofs, durability failure, automatic provider retry,
  restart, cancellation during runtime/Native close, external fencing adaptation
  and automatic durable-receipt recovery (`akkaradb_mirror_recovery_test`),
- STRIPE crash/reopen before and after metadata publication, recovery of pending
  intents, abandoned/superseded shard cleanup, durable-ACK enforcement under
  ASYNC/NONE, concurrent reads/overwrites, tombstone cleanup, and owner outage,
- `RAID.0` preset normalization and config round trip, data-only codec round
  trip, distributed engine storage, mandatory all-shard reads, failure on one
  unavailable node, and recovery after that node rejoins,
- `RAID.1` preset normalization and config round trip, WAL enforcement, durable
  full-copy replication, degraded Primary-local writes, reconnect rebuild and
  health transitions, plus rejection of replica writes without explicit Primary
  promotion,
- `RAID.5` preset normalization and config round trip, one-node degraded writes,
  parity reconstruction, and rejection of layouts with fewer than two data shards,
- `RAID.6` preset normalization and config round trip, two-node degraded writes
  and reads with a surviving metadata quorum, plus rejection of four-node layouts,
- `RAID.10` preset normalization and config round trip, fixed mirror-pair
  placement, degraded writes with one missing pair member, reads from the
  surviving copy, and automatic rebuild on return; hot-spare coverage additionally
  verifies option normalization, exclusion from normal placement, secondary and
  owner replacement, persisted effective placement, and no automatic failback,
- 4 MiB Raft frame enforcement, aggregate receive-memory and heartbeat bounds,
  retry-result capacity bounds, lazy/file-backed snapshot transfer, and
  memory-only snapshot admission, consolidation, failure reporting, and retry,
- endpoint role changes and shutdown without joining callback workers while the
  runtime state lock is held, plus retry after endpoint startup failure,
- scalar local rollback execution modes and journal recovery; MIRROR and Raft
  global-stream rollback; PARTITIONED owner-stream checkpoints, multi-page
  rollback, next-startup replay, and conflict preservation; and STRIPE logical
  metadata rollback with historical generation reconstruction,
- Raft crashes at segment-sync and metadata-publication boundaries, uncommitted
  versus committed corruption, metadata-corruption rejection, segment rotation,
  and preservation of existing segment contents during later appends.

`akkaradb_cluster_observability_test` is a separate, dynamically ported CTest
target for the management-data contract. It covers the empty disabled-cluster
snapshot, self-describing histogram invariants, runtime start/close/restart
timestamps, diagnosable endpoint startup failure, complete `EngineStats`
topology/configuration metadata, Raft peer disconnect/recovery counters, and
non-Raft owner-read success/failure/recovery. This focused target is labelled
`cluster` and `observability` and may be run without the monolithic cluster smoke
executable.

`akkaradb_cluster_routing_test` (`--routing`) isolates Native routing coverage:
PLAIN/SECURE PARTITIONED and Raft forwarding (including learners), structured
redirects and local-only rejection, concurrent request IDs, same-owner versus
cross-owner batches, STRIPE coordinator writes/reads, payload limits, late
responses without replay, and MIRROR
strict-ACK and quorum-authority/ASYNC-data forwarding. It also checks 1 MiB
PLAIN/SECURE batch forwarding, large transport responses, interleaved replication
ACKs, cancellation of transfer READY waits, and Raft admission consuming the
destination's remaining timeout budget. The API server smoke test
additionally verifies TCP connection reuse and HTTP/gRPC structured route errors.

`akkaradb_cluster_queries_test` (`--cluster-queries`) isolates public range and
history coverage across PLAIN/SECURE PARTITIONED, MIRROR, STRIPE, and data Raft
with a learner. It checks owner aggregation, public-key ordering and binary keys,
MemTable/SST snapshot cuts even with WEAK_ORDERED local scans, tombstones,
materialized Blob values larger than a frame, bounded paging, logical revisions,
rollback history, restart, strict OWNER_ONLY, missing owners/quorum, spool and
cursor limits, expiry, and prepared-cursor lifetime during shutdown. The default
cluster smoke includes these scenarios. The API server smoke additionally checks
TCP/HTTP/gRPC range/history queries through a MIRROR replica; MIRROR fencing
tests cover authority-quorum reads with asynchronous data replication.

`akkaradb_cluster_partition_query_parallelism_test`
(`--partition-query-parallelism`) gates real remote STATUS callbacks across 16
partitions to verify overlapping checks and the eight-check engine-wide bound
across concurrent queries. It checks complete counts, ordered unique scans,
bounded ranges, and explicit failure within the shared deadline when an owner
is unavailable, including shutdown while a query is pending. Saturated executor
checks verify deadline expiry during admission and after queueing, removal of
unstarted work, and subsequent executor reuse.

`akkaradb_cluster_partition_raft_test` (`--partition-raft`) covers PARTITIONED
leader failure and online holder/copy-count changes, MIRROR manual/automatic
election and acknowledged local-write loss, MIRROR admission with retained
history, and RAID.10 placement/hot-spare replacement. Its PLAIN/SECURE STRIPE
cases verify cancellation with a committed intent, write rejection while pending,
restart and automatic resumption, removal of an already unavailable member,
and forwarding with the ordinary channel limit set to one. Placement changes
preserve logical sequences, original writer ids and timestamps, historical
values and rollback checkpoints. `--stripe-online` isolates those STRIPE cases.
The focused target has a six-minute timeout and shares the cluster loopback lock.

`akkaradb_raft_prevote_test` (`--raft-prevote`) isolates election-probe coverage:
strict wire sizes and peer identity, rejection of `AKRP2` peers, prospective vs.
actual terms, preservation of durable votes, heartbeat/probe races, live-leader suppression without
deadline extension, stale-log rejection, repeated isolation without term growth,
and PLAIN/SECURE one-way partitions followed by catch-up without leader churn.
It also checks election and linearizable access after a real leader failure and
explicit leadership transfer. These scenarios are included in default cluster
smoke; the focused CTest target shares its loopback resource lock.

Raft pipeline tests additionally cover PLAIN and SECURE connection reuse,
128 outstanding proposals, fewer persistence batches than proposals, concurrent
ReadIndex requests without log metadata writes, one-peer outage/reconnect,
rejection of reads after quorum loss despite earlier successful ACKs, shutdown
of pending proposals, and concurrent engine inline/Blob/batch writes followed
by reopen. `--raft-pipeline` selects these tests; `--raft` includes them and the
existing Raft regression tests.

The cluster smoke executable's `--storage` mode runs the focused STRIPE and
segmented-Raft storage tests. Its default mode includes these and the other
cluster tests. PLAIN remains intentionally unauthenticated; these tests do not
establish a security guarantee for untrusted replication peers.

`--retry-transfer` selects retry-safe request and bounded-transfer tests, also
included in default mode: concurrent same-ID retries, conflicting contents,
expiry/capacity rejection, remove deduplication, compaction/reopen, process crash
after durable apply but before receipt persistence, late-node snapshot catch-up,
leadership transfer, quorum-loss timeout/retry and query failure, malformed or
truncated transfers, deterministic frame loss/duplication/reordering/partial-read
and latency injection, reconnect replay, whole-payload corruption, quota cleanup,
and large PLAIN/SECURE replication to two replicas plus owner-read responses. It
also verifies that a Raft node with mismatched retry policy is rejected before RPC
processing while the compatible majority remains available, and validates the
published cluster/peer/request-journal statistics. Snapshot catch-up
also verifies that read visibility advances over the prefix replaced by a snapshot.
Repeated owner-read timeouts must preserve the write ACK connection and admit
only one outstanding read. `--stripe-concurrency` isolates the existing STRIPE
failure/recovery and concurrent-overwrite regression.

`--soak <seed> <rounds>` runs every cluster stress category once per round in a
seed-shuffled order and prints the seed, round, and scenario before execution. Its
coverage includes malformed/latent and interrupted resumable frames, large PLAIN and
SECURE payloads, disconnect/reconnect reaping, subprocess apply crashes, snapshot
catch-up, leadership transfer, and large-Blob leader churn. The
`akkaradb_cluster_soak` target uses seed `0xA44A5EED`; failures are reproducible by
rerunning the printed command. On Linux, the `linux-sanitizers` configure/build
preset enables ASan and UBSan with non-recovering diagnostics, while
`linux-sanitizer-soak` builds and runs the deterministic soak target.

The JVM project contains integration tests for native engine access, HTTP/TCP
and gRPC engines, options wire serialization, sequence helpers, and high-level
database behavior.

## 26. Explicit Non-Guarantees

- `ENQUEUED` durability can lose a visible write on process or power failure
  before the async WAL writer persists it.
- `APPLIED` visibility can expose later sequences while earlier writers remain
  incomplete.
- Thread-local sequence ranges can create holes in the snapshot upper bound.
- External bulk protocols are not transactional; standalone C++ callers use `begin()`/`end()`.
- Typed table compound writes, secondary-index maintenance, and foreign-key
  actions are not transactional.
- Blob GC is disabled with VersionLog because historical versions may reference
  old blobs.
- L0 blocking backpressure requires a compaction path that can make progress.
- STRIPE degraded reads require at least `dataShards` readable shards and
  reachable owner metadata; they are not consensus reads.
- `RAID.0` has no shard redundancy: every data shard must remain readable, and
  authoritative metadata operations still require a metadata-Raft majority;
  losing one participating shard node can make affected values unavailable.
- `RAID.10` has fixed two-node mirror pairs and at most one shared hot spare. It
  can lose one member from every pair at the value layer, but only one missing
  placement is automatically remapped per generation. All authoritative
  operations still require a metadata-Raft majority; a four-node layout without
  a spare therefore stops metadata progress after two node failures even when
  they are from different pairs.
- Server backend availability depends on build flags and runtime library
  loading.
- Low-level C++ object layout is not an external ABI.
- Disk-format compatibility is limited to the current compatibility contract;
  unsupported old data must be migrated or recreated explicitly.

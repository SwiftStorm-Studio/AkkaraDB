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
- Multi-key transactions.
- Compare-and-swap or optimistic concurrency control.
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

Sequence numbers are not transactions. A batch reserves a range but does not
create all-or-nothing visibility or durability.

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

`putBatch(span<BatchPutEntry>)` reserves a sequence range and applies entries in
that range. It immediately returns for an empty span. It is not a transaction:
there is no all-or-nothing rollback, isolation, or independent durability
boundary for the group.

## 8. Read Path

### 8.1 Point Lookup

Point reads search newest visible MemTable state first, then SST state:

1. Capture visibility snapshot.
2. Search MemTable by key and snapshot.
3. If MemTable returns a visible tombstone, report missing.
4. If MemTable returns a normal or Blob record, materialize the public value.
5. If MemTable misses, search SST files with the same snapshot.
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
| `backendFactory` | null | Use default ordered backend |
| `onFlush` | null | Optional immutable-table flush callback |

The implementation hard ceiling is 256 MemTable shards. `AUTO` flush mode maps
to `BYTES_PER_SHARD` when `thresholdBytesPerShard > 0`, otherwise to
`MANUAL_ONLY`.

### 9.2 Backends and Version Retention

Built-in ordered backends include SkipList, B+Tree, and ART implementations.
The public contract is the `IMemTable` behavior, not a backend-specific memory
layout.

Built-in backends keep up to four in-memory versions per logical key. This is
an in-memory optimization, not the historical-read API. Persistent history
requires VersionLog.

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
flush workers iterate immutable tables in key order and invoke `onFlush` with
sorted `RecordView` spans.

In the engine, `onFlush` writes records through `SSTManager::flush`, checkpoints
the Manifest, optionally prunes WAL through the returned checkpoint sequence,
and can run Blob GC when configured.

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

Blob id currently equals the creating sequence. Blob files carry a 48-byte v5
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
kept as ordered ranges by compaction policy. SST lookup and scan honor the
caller-provided snapshot sequence.

### 12.2 Flush and Compaction

MemTable flush writes sorted records to an L0 SST. Compaction replaces one or
more input files with output files. The Manifest's atomic `COMPACTION_COMMIT`
record names all outputs and all inputs, so recovery either installs the whole
replacement or preserves the old inputs and discards orphan outputs.

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
- `history` returns an empty vector,
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
advances at `forceSync`, lane rotation, or close. Entries that remain queued or
unsynced at a crash are discarded rather than extending recovery beyond a
durable prefix.

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
`PARALLEL` also writes an `.akvtail` sibling for every active lane. It records
the byte length of the durable contiguous VLog prefix. Recovery reads no bytes
beyond that prefix, so a crash cannot turn an interrupted lane append into a
corrupt VLog tail. The tail is a recovery boundary, not a historical index.
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

Rollback does not rewrite old records. It appends a new normal or tombstone
mutation that represents the rolled-back state. Rollback mutations are marked
with the rollback source node id and rollback flag, then follow normal local and
cluster shipment paths.

VLog entry corruption is never accepted. `EAGER` recovery fails `open`; in
`BACKGROUND` mode the first operation that crosses the recovery barrier throws
the stored recovery error.

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
The config file uses `AKC5` magic and version 3.

### 15.1 Persistent Cluster Config

`ClusterConfig` stores:

- node ids and advertised host/data/repl ports,
- node capability flags,
- replication/placement mode,
- acknowledgement policy,
- write consistency controls,
- replica lag behavior,
- Raft membership options,
- stripe data/parity shard counts.

Runtime-only values such as bind host, primary override, identity seed path, and
peer key pins are not serialized into `ClusterConfig`.

### 15.2 Placement Modes

| Mode | Meaning |
|---|---|
| `STANDALONE` | No replication runtime |
| `MIRROR` | Ship writes to every data-bearing node |
| `PARTITIONED` | Assign each key to one owner using rendezvous-style placement |
| `STRIPE` | Split data and parity shards across distinct data-bearing nodes |

### 15.3 Node Roles and Capabilities

Runtime role is `STANDALONE`, `PRIMARY`, or `REPLICA`. Startup role may be
`AUTO`, `PRIMARY`, or `REPLICA`.

Capabilities are bit flags:

| Capability | Meaning |
|---|---|
| `COORDINATOR_ELIGIBLE` | Node may be selected as primary |
| `DATA_BEARING` | Node can store KV data and receive routed writes |

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

Consistency mode:

| Value | Meaning |
|---|---|
| `PRIMARY_ACK` | Primary-to-replica protocol governed by write consistency/ack policy |
| `ASYNC` | Fire-and-forget replication with local completion |
| `RAFT_QUORUM` | Raft-style quorum commit over native replication transport |

`RAFT_QUORUM` derives quorum from data-bearing voters and forces failed writes
on acknowledgement timeout.

Timeout action is `ACCEPT_LOCAL` or `FAIL_WRITE`. Replica lag action is
`ASYNC_RESYNC`, `REJECT_REPLICA`, or `BLOCK_WRITES`.

### 15.5 Secure Transport

Replication transport is `SECURE` by default. Secure mode uses native
raw-public-key identity:

- persistent identity seed can be loaded or created,
- peers can be pinned by node id and public key,
- clients can require an expected primary node id.

Plain replication is intentionally restricted to validated LAN or loopback
advertised addresses.

### 15.6 Erasure Codec

The native erasure codec API supports Reed-Solomon-style and external codec
selection contracts. Stripe configuration records data and parity shard counts,
defaulting to 4 data and 2 parity shards. Current stripe metadata and codec
support do not by themselves guarantee a complete degraded-read/repair system.

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

The TCP binary protocol is version 2.

Request header (`ApiRequestHeader`, 16 bytes):

| Field | Meaning |
|---|---|
| `magic[4]` | `AK5Q` |
| `version` | `2` |
| `opcode` | `ApiOp` |
| `requestId` | Caller request id |
| `keyLen` | Key byte length |
| `valLen` | Value/payload byte length |

Response header (`ApiResponseHeader`, 13 bytes):

| Field | Meaning |
|---|---|
| `magic[4]` | `AK5S` |
| `status` | `OK`, `NOT_FOUND`, or `ERROR_STATUS` |
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
| `replBindHost` | `0.0.0.0` | Local replication listener bind host |
| `startupRole` | `AUTO` | Startup role selection |
| `primaryHost` | empty | Replica-side primary host override |
| `primaryReplPort` | 0 | Replica-side primary port override |
| `primaryNodeId` | 0 | Replica-side primary node id override |
| `secure.identitySeedPath` | empty | Persistent identity seed path |
| `secure.pinnedPeers` | empty | Peer public-key pins |
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
- API server counters,
- MemTable snapshot,
- WAL snapshot,
- Blob snapshot,
- SST levels and compaction snapshot,
- VersionLog snapshot.

Stats remain intentionally available after certain background failures so
operators can inspect unhealthy WAL, pending queues, compaction failures, and
API counters.

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
- Cluster runtime serializes endpoint start/close/shipping as required.

`PackedTable` is move-only and contains mutable temporary buffers. It should not
be shared for unsynchronized compound table work.

### 22.2 Background Work

Depending on configuration, background work can include:

- WAL async flushers,
- MemTable immutable flush workers,
- SST compaction workers,
- Manifest fast-mode flusher,
- VersionLog async/batched flusher, `PARALLEL` lane writers, and optional
  `BACKGROUND` recovery worker,
- Blob GC,
- API accept/connection workers,
- cluster replication manager and endpoints.

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
| SST file | `AKS2` | 2 | Header, metadata, footer, and block CRCs |
| SST footer | `A2SF` | 2 | Footer CRC |
| Blob file | `AKB5` | 1 | Header CRC and original-content CRC |
| Manifest | `AMV5` | 1 | Header CRC and per-record payload CRC |
| VersionLog segment | `AKV5` | 1 | File-header and entry CRCs |
| VersionLog sidecar index | `AKVI` | 2 | Header CRC and payload CRC |
| VersionLog durable tail | `AKVT` | 1 | Tail-header CRC |
| Cluster config | `AKC5` | 3 | Config CRC |
| TCP API request | `AK5Q` | protocol 2 | Transport framing and payload validation |
| TCP API response | `AK5S` | protocol 2 | Transport framing and payload validation |

The serializer/deserializer implementations are the byte-layout authority:

- `WalFraming.hpp/.cpp`
- `SSTFormat.hpp`, `SSTWriter.cpp`, `SSTReader.cpp`
- `BlobFraming.hpp/.cpp`
- `ManifestFraming.hpp/.cpp`
- `VersionLog.cpp`
- `ClusterConfig.cpp`
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

`AkBlobHeaderV5` is 48 bytes:

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

### 24.5 SST v2 File Shape

An SST v2 file contains:

```text
[SSTFileHeaderV2:256]
[data blocks...]
[block index entries...]
[key arena...]
[Bloom filter...]
[SSTFooterV2:48]
```

Fixed structures:

| Structure | Size |
|---|---:|
| `SSTFileHeaderV2` | 256 |
| `SSTBlockHeaderV2` | 64 |
| `SSTBlockIndexEntryV2` | 72 |
| `SSTBloomHeaderV2` | 16 |
| `SSTFooterV2` | 48 |

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

Each `.akvlog` segment starts with this 32-byte `AKV5` v1 header:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | `magic` (`AKV5`) |
| 4 | 2 | `version` (`1`) |
| 6 | 1 | `syncModeHint` |
| 7 | 1 | reserved |
| 8 | 8 | `createdNs` |
| 16 | 8 | reserved |
| 24 | 4 | header `crc32c` |
| 28 | 4 | reserved |

It is followed by variable-sized entries:

```text
[AkvlogV5EntryHeader:43][key:keyLen][stored value:valueLen][entry CRC32C:u32]
```

The packed entry header contains `entryLen`, `seq`, `sourceNodeId`,
`timestampNs`, `flags`, `keyFp64`, `keyLen`, and `valueLen`. `entryLen` covers
the complete entry, including its trailing CRC. With `VLOG_FLAG_ZSTD`, the
stored value begins with a four-byte uncompressed size followed by the Zstd
payload. Public `VersionEntry` values are decompressed and do not expose that
internal flag.

Each VLog segment may have a derived sibling `.akvidx` file. It begins with a
44-byte `AKVI` v2 header containing the Bloom hash count, authoritative VLog
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

### 24.8 Cluster Config

Cluster config is a CRC-protected `AKC5` v3 binary file. It stores persistent
membership, placement, acknowledgement, consistency, Raft, and stripe settings.
Runtime transport settings remain out-of-band.

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
- cluster smoke test,
- MemTable lifecycle smoke test,
- engine recovery smoke test,
- WAL async failure smoke test,
- SST snapshot visibility smoke test.

Throughput benchmarks are measurement tools, not correctness specifications.
Smoke tests cover representative correctness contracts for recovery, async WAL
failure propagation, snapshot visibility, MemTable lifecycle, cluster behavior,
and API framing.

The JVM project contains integration tests for native engine access, HTTP/TCP
and gRPC engines, options wire serialization, sequence helpers, and high-level
database behavior.

## 26. Explicit Non-Guarantees

- `ENQUEUED` durability can lose a visible write on process or power failure
  before the async WAL writer persists it.
- `APPLIED` visibility can expose later sequences while earlier writers remain
  incomplete.
- Thread-local sequence ranges can create holes in the snapshot upper bound.
- `putBatch` is not transactional.
- Typed table compound writes, secondary-index maintenance, and foreign-key
  actions are not transactional.
- Blob GC is disabled with VersionLog because historical versions may reference
  old blobs.
- L0 blocking backpressure requires a compaction path that can make progress.
- Stripe metadata and erasure codec APIs do not guarantee completed degraded
  read/repair behavior by themselves.
- Server backend availability depends on build flags and runtime library
  loading.
- Low-level C++ object layout is not an external ABI.
- Disk-format compatibility is limited to the current compatibility contract;
  unsupported old data must be migrated or recreated explicitly.

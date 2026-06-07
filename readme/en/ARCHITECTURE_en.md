# AkkaraDB Native Architecture

This document explains the native C++23 AkkaraDB engine architecture. It is a companion to the v5 technical specification: the specification defines exact formats and compatibility rules, while this document explains how the major subsystems fit together and how data moves through the engine.

## Table of Contents

- [Overview](#overview)
- [Storage Layout](#storage-layout)
- [Core Components](#core-components)
  - [Public API Layer](#public-api-layer)
  - [AkkEngine](#akkengine)
  - [Record and Key Model](#record-and-key-model)
  - [Memory Management](#memory-management)
  - [MemTable](#memtable)
  - [Write-Ahead Log](#write-ahead-log)
  - [Blob Manager](#blob-manager)
  - [SST Manager](#sst-manager)
  - [Manifest](#manifest)
  - [Version Log](#version-log)
  - [API Servers](#api-servers)
  - [Cluster Runtime and TLS](#cluster-runtime-and-tls)
- [Data Flow](#data-flow)
- [Recovery and Shutdown](#recovery-and-shutdown)
- [Concurrency Model](#concurrency-model)
- [Startup Modes](#startup-modes)

---

## Overview

AkkaraDB native is the canonical C++23 storage engine for AkkaraDB. It stores binary-safe keys and values using an LSM-tree design: writes first enter an in-memory MemTable, durable configurations append them to WAL segments, and flushes create immutable SST files that are compacted across levels.

The native engine also includes systems that are outside a minimal LSM implementation:

- large-value externalization through the Blob Manager
- optional per-key version history through the Version Log
- typed C++ tables using BinPack serialization and secondary index keys
- JNI entry points used by the Kotlin/JVM layer
- embedded HTTP and binary TCP API servers
- cluster and replication primitives for standalone, mirror, and stripe deployments
- TLS transport support through mbedTLS

At a high level, the architecture looks like this:

```text
+---------------------------------------------------------------+
|                         Public API                            |
|                                                               |
|   AkkaraDB      PackedTable<&T::id>      engine::AkkEngine     |
+-------------------------------+-------------------------------+
                                |
+-------------------------------v-------------------------------+
|                           AkkEngine                           |
|                                                               |
|  +-------------------+      +------------------------------+  |
|  | MemTable          |      | WAL Writer                   |  |
|  | - N shards        |      | - sharded segments           |  |
|  | - BPTree/ART/etc. |      | - sync/async/off             |  |
|  +---------+---------+      +------------------------------+  |
|            |                                                  |
|            | flush                                            |
|            v                                                  |
|  +-------------------+      +------------------------------+  |
|  | SST Manager       |<---->| Manifest                     |  |
|  | - L0..L6 default  |      | - SST lifecycle log          |  |
|  | - bloom + index   |      | - compaction commits         |  |
|  +-------------------+      +------------------------------+  |
|                                                               |
|  +-------------------+      +------------------------------+  |
|  | Blob Manager      |      | VersionLog                   |  |
|  | - values >= limit |      | - point-in-time lookup       |  |
|  | - .blob files     |      | - rollback support           |  |
|  +-------------------+      +------------------------------+  |
|                                                               |
|  +-------------------+      +------------------------------+  |
|  | API Server        |      | Cluster Runtime              |  |
|  | - HTTP/TCP        |      | - primary/replica            |  |
|  | - AK5 protocol    |      | - mirror/stripe routing      |  |
|  +-------------------+      +------------------------------+  |
+---------------------------------------------------------------+
```

---

## Storage Layout

When `paths.data_dir` is set, missing component paths are derived from that directory.

```text
{data_dir}/
|-- wal/             WAL segment files
|-- sstable/         SST levels
|   |-- L0/
|   |-- L1/
|   |-- ...
|   `-- L6/
|-- blobs/           Externalized value payloads
|-- manifest.akmf    SST lifecycle and checkpoint metadata
|-- history.akvlog   Version history when enabled
|-- cluster.akcc     Cluster topology when enabled
`-- node.id          Persistent node identity
```

The exact binary formats are defined in `SPEC.md`. The important architectural point is that each persistent subsystem has a clear responsibility:

| Component | Role |
|---|---|
| WAL | Replays acknowledged or pending writes after a crash |
| SST | Stores immutable sorted key/value records |
| Manifest | Tracks which SST files and compaction transitions are live |
| Blob files | Store large values outside MemTable, WAL, and SST payloads |
| Version Log | Stores historical values used by point-in-time reads and rollback |
| Cluster config | Stores durable cluster topology, not runtime TLS paths |

---

## Core Components

### Public API Layer

The native repository exposes two main API levels.

`engine::AkkEngine` is the byte-oriented core API. It accepts raw byte spans for keys and values and exposes point reads, writes, removes, scans, history operations, rollback, flush, sync, and close.

`AkkaraDB` and `PackedTable<&T::id>` are the high-level typed API. They map C++ aggregate entities to raw key/value rows using BinPack serialization. Table keys are scoped by an 8-byte FNV-1a table prefix, and secondary indexes use a separate prefix derived from `table_name + ":idx:" + field_name`.

The JVM layer reaches the same native engine through JNI when `AKKARADB_BUILD_JNI=ON`. The JNI bridge exposes raw operations, scan cursors, query scan payload evaluation, option-based open, and rollback entry points used by the Kotlin module.

### AkkEngine

`AkkEngine` is the coordinator. It owns or connects the storage components and gives the rest of the system a single thread-safe mutation and read surface.

Main responsibilities:

- derive component paths from `data_dir`
- create or open WAL, SST, Blob, Manifest, VersionLog, API, and cluster components according to options
- serialize top-level write sequence assignment
- externalize large values before writing records
- append to WAL and VersionLog before mutating the MemTable when enabled
- flush MemTable records to SST
- route reads through MemTable, SST, and BlobManager
- ship writes and blobs to the cluster runtime when acting as primary
- close components in a controlled order

### Record and Key Model

The raw engine orders keys by bytewise lexicographic comparison. Higher layers must encode keys in a way that preserves the ordering they need. `PackedTable` uses little-endian encodings for integral primary keys to stay consistent with the native binary formats; numeric range semantics are enforced by typed scan/query logic rather than by assuming byte order matches numeric order.

In memory, records are represented by `OwnedRecord`. It is designed as a compact 64-byte metadata object:

```text
+-------------------+------------------------------------------+
| MemHdr16          | seq, key length, value length, flags     |
| key_fp64          | 64-bit key fingerprint                   |
| mini_key          | first up to 8 key bytes, little-endian   |
| SmallBuffer       | inline or arena-backed [key][value]      |
+-------------------+------------------------------------------+
```

On disk, SST records use `SSTHdr32`, a 32-byte header followed by key bytes and value bytes. The full key comparison is always authoritative. `key_fp64` and `mini_key` are acceleration hints for fast rejection and in-block search.

When a value is stored in the Blob Manager, the record value becomes a 20-byte `BlobRef` and the blob flag is set. Reads treat that as an indirection and return the original value after validating the blob header and content CRC.

### Memory Management

The native engine uses small, explicit buffer abstractions to keep hot paths predictable.

`BufferArena` owns temporary allocations for scans and API calls. `AkkEngine::scan` returns views whose key/value spans are valid for the arena lifetime.

`OwnedBuffer` is aligned owning storage for I/O-oriented buffers. `BufferView` is the corresponding non-owning view.

`SmallBuffer` is embedded in `OwnedRecord`. Short `[key][value]` payloads stay inline; larger payloads move to arena-backed storage. This keeps common records compact without forcing every payload into heap allocations.

### MemTable

The MemTable is the first read and write target. It is sharded to reduce contention and can derive its shard count automatically. When `RuntimeOptions::writer_threads` is set, both MemTable and WAL shard counts can be derived from writer count and capped by configuration.

Each write sequence is allocated through the MemTable sequence model:

```text
reserve_seq(1) -> globally monotonic seq
last_seq()     -> read snapshot for point reads and scans
replay(seq)    -> advances sequence state during recovery
```

Lookup returns records visible at the requested snapshot. Tombstones are returned to the caller rather than hidden inside the MemTable, because a tombstone must suppress older SST values.

Flush lifecycle:

```text
shard crosses threshold_bytes_per_shard
  |
  +-- shard can be sealed
  +-- engine on_flush callback receives sorted RecordView span
  +-- SSTManager::flush writes a new SST
  +-- Manifest checkpoint records the resulting state
  +-- WAL segments are pruned up to checkpoint when configured
```

### Write-Ahead Log

The WAL stores append-only mutation records for crash recovery. Native v5 uses a segmented WAL under `{data_dir}/wal`, and each segment starts with a CRC-protected `WalSegmentHeader`.

Entries are serialized as:

```text
[WalEntryHeader:32][key bytes][value bytes]
```

The entry header contains the record sequence, key fingerprint, total entry length, value length, key length, flags, and a CRC32C over the header-with-zero-crc plus key and value.

The WAL has three sync modes:

| Mode | Meaning |
|---|---|
| `Sync` | Synchronous durability path |
| `Async` | Grouped background flush path |
| `Off` | No explicit sync, intended for cache or test modes |

During startup recovery, segment headers and entry CRCs are validated before entries are applied to a fresh MemTable.

### Blob Manager

The Blob Manager externalizes values whose size is greater than or equal to `blob.threshold_bytes`, default 16 KiB. This keeps MemTable records, WAL entries, and SST blocks smaller while preserving the normal `get` API shape.

Write path:

```text
value size >= threshold
  |
  +-- BlobManager writes the payload to {data_dir}/blobs
  +-- optional Zstd compression is applied
  +-- Blob header and content CRC are stored
  +-- record value becomes BlobRef(blob_id, total_size, content_crc32c)
  +-- record flag includes blob
```

Read path:

```text
record flag includes blob
  |
  +-- parse BlobRef
  +-- read blob file
  +-- validate header CRC and content CRC
  +-- return original value bytes
```

### SST Manager

The SST Manager owns immutable sorted files and compaction. By default it manages 7 levels, `L0..L6`. L0 may contain overlapping files; higher levels are expected to become non-overlapping after compaction.

SST v2 file layout:

```text
[SSTFileHeaderV2:256]
[data blocks...]
[SSTBlockIndexEntryV2 array]
[key arena]
[SSTBloomHeaderV2][bloom bits]
[SSTFooterV2:48]
```

Lookup uses several filters before doing full record comparison:

```text
file min/max key check
  |
  +-- Bloom filter negative check
  +-- block index binary search
  +-- in-block binary search using SSTHdr32, mini_key, and full key compare
```

Data blocks contain either raw concatenated SST records or Zstd-compressed bytes. The block index stores block key boundaries, and the Bloom filter reduces disk and cache work for absent keys.

Flush creates new SSTs from sorted MemTable records. Compaction rewrites overlapping or over-budget SST sets into lower levels and records the transition in the Manifest. `CompactionCommit` is the preferred manifest event because replay can apply all input/output file changes atomically.

### Manifest

The Manifest is an append-only, CRC-protected storage lifecycle log. It tracks live SST files, compaction transitions, checkpoints, and cluster metadata events.

The file starts with `ManifestFileHeader`, then appends records:

```text
[ManifestFileHeader:32][ManifestRecordHeader:8][payload]...
```

Important record types include:

| Event | Purpose |
|---|---|
| `SSTSeal` | A new SST file was sealed |
| `Checkpoint` | Named or unnamed checkpoint |
| `CompactionStart` | Informational start marker |
| `CompactionCommit` | Atomic compaction input/output transition |
| `Truncate` | Informational truncation marker |
| `NodeJoin`, `NodeLeave`, `PrimaryLease` | Cluster metadata |

Manifest replay rebuilds in-memory SST lifecycle state. Malformed or CRC-invalid records are not applied.

### Version Log

The Version Log records per-key history when enabled. It powers:

- `get_at(key, seq)`
- `history(key)`
- `rollback_to(seq)`
- `rollback_key(key, seq)`

Each version entry stores sequence, source node id, timestamp, flags, and value bytes. Rollback-generated records use a reserved rollback node id and rollback flag so they can be distinguished from normal writes.

Version history is disabled by default in `FAST` and `NORMAL`, and enabled by the `DURABLE` startup preset unless overridden.

### API Servers

The engine can start embedded API backends when `components.api_enabled` is set.

Supported backends:

| Backend | Default port | Purpose |
|---|---:|---|
| HTTP | 7070 | REST-style key/value operations |
| TCP | 7071 | Binary AK5 request/response protocol |

The binary protocol uses `AK5Q` request frames and `AK5S` response frames. Current opcodes include `Get`, `Put`, `Remove`, and `GetAt`.

The HTTP API exposes basic endpoints such as `/v1/ping`, `/v1/put`, `/v1/get`, `/v1/remove`, and `/v1/get_at`.

### Cluster Runtime and TLS

The cluster layer supports three deployment modes:

| Mode | Meaning |
|---|---|
| `Standalone` | Local-only operation |
| `Mirror` | Writes are mirrored to all data-bearing nodes |
| `Stripe` | Keys are assigned to data nodes by router policy |

Stripe routing primitives exist, but the runtime currently rejects Stripe configs because distributed write forwarding and ownership migration are not implemented yet.

Node roles are `Standalone`, `Primary`, and `Replica`. The primary accepts writes and ships records or blobs to replicas. Replicas apply replicated records and blobs through callbacks supplied by the engine.

Acknowledgement policies are `Async`, `All`, and `Quorum`.

`NodeInfo.host` in the cluster config is the advertise address that peers dial. The primary replication listener binds to the runtime-only `repl_bind_host`, which defaults to `0.0.0.0`. Replication links use TCP. `TransportMode::TLS` wraps the TCP stream with mbedTLS, and `TransportMode::Plain` is allowed only when every node host is loopback or LAN/private address space. Hostnames other than `localhost` are treated as non-private during config validation. Primary selection is deterministic: the coordinator-eligible node with the lowest `node_id` becomes primary. This works across LAN/WAN nodes without shared filesystem state, but it is not quorum consensus and does not provide split-brain-safe automatic failover.

TLS support is compiled into the current native target through mbedTLS. API servers and replication links can use TLS or plain transport depending on their runtime options.

---

## Data Flow

### Write Path

```text
put(key, value)
  |
  +-- acquire engine write serialization
  +-- reserve seq from MemTable
  +-- if value >= blob threshold:
  |     |
  |     +-- BlobManager writes payload
  |     +-- stored value becomes 20-byte BlobRef
  |     +-- record flag includes blob
  |
  +-- append_all(seq, key, stored_value, flags)
        |
        +-- WAL append if enabled
        +-- VersionLog append if enabled
        +-- MemTable put/remove
        +-- Cluster ship_entry if enabled and this node is primary
```

The sequence assignment, WAL append, version-log append, MemTable mutation, blob externalization, and replication shipping are ordered under the engine write mutex. This keeps mutation order consistent even when public methods are called concurrently.

### Read Path

```text
get(key)
  |
  +-- snapshot_seq = MemTable::last_seq()
  +-- MemTable::get(key, snapshot_seq)
  |     |
  |     +-- tombstone -> not found
  |     +-- normal    -> return value
  |     +-- blob      -> BlobManager::read(...)
  |
  +-- SSTManager::get(key)
        |
        +-- tombstone -> not found
        +-- normal    -> return value
        +-- blob      -> BlobManager::read(...)
        +-- optional sst_promote_reads -> insert SST hit into MemTable
```

The MemTable is always the first authority for the current snapshot. SST is the fallback for data that has already been flushed. Blob dereference happens after the winning record has been selected.

### Range Scan Path

```text
scan(start, end)
  |
  +-- caller provides BufferArena
  +-- create MemTable iterator at snapshot
  +-- create SST iterators for candidate levels/files
  +-- merge by bytewise key order
  +-- for duplicate keys, newer visible record wins
  +-- tombstones suppress older values
  +-- return ArenaGenerator<ScanRecordView>
```

The returned views are tied to the arena lifetime. This avoids copying every scanned key/value into independent heap objects.

### Typed Table Path

```text
PackedTable<User, id>.put(user)
  |
  +-- encode primary row key:
  |     [table_prefix:8][encoded_pk]
  |
  +-- BinPack::encode(user)
  +-- engine.put(primary_key, encoded_user)
  |
  +-- for each secondary index:
        [index_prefix:8][field_len:u32le][encoded_field][encoded_pk] -> empty value
```

Secondary index entries are non-unique. The encoded primary key suffix keeps duplicate field values distinct and lets index scans recover the primary row.

### JNI Query Scan Path

```text
JVM scanQuery(start, end, queryBytes, schemaBytes)
  |
  +-- JNI opens native scan cursor over [start, end)
  +-- native side decodes schema payload
  +-- native side decodes query expression payload
  +-- each row value is decoded enough to evaluate predicates
  +-- matching rows are returned to JVM RowView(ByteBufferL key, ByteBufferL value)
```

Query and schema payload integers are little-endian. Unsupported operators must be treated as query-evaluation errors rather than silently matching rows.

---

## Recovery and Shutdown

### Startup Recovery

`AkkEngine::open` performs startup in a fixed order:

```text
1. Derive missing component paths from data_dir
2. Create required directories
3. Load or generate persistent node id
4. Open Manifest and prepare replay-capable state
5. Create SST Manager and recover SST state when enabled
6. Replay WAL into a fresh MemTable when enabled
7. Start WAL writer, BlobManager, VersionLog, ClusterRuntime, and API server as configured
```

The recovery policy is deliberately component-local:

- WAL recovery validates segment headers and entry CRCs before applying entries.
- SST readers validate headers, footers, block metadata, and block CRCs before trusting records.
- Blob reads validate both header CRC and original content CRC.
- Manifest replay ignores malformed or CRC-invalid records rather than applying partial state transitions.

### Crash Scenarios

```text
Power loss during WAL append
  -> recovery applies valid entries and stops or skips corrupted trailing data according to WAL logic

Power loss during MemTable flush
  -> WAL can rebuild records; partial SST is ignored if Manifest did not commit it

Power loss during compaction
  -> CompactionCommit is replayed atomically; without it, input files remain the live state

Power loss during blob write
  -> blob reads validate header and content CRC; unreferenced or corrupt blob payloads are not trusted
```

### Shutdown

`AkkEngine::close` is idempotent. It closes components in this order:

```text
1. API server
2. Cluster runtime
3. MemTable force flush when configured
4. SST manager shutdown
5. WAL force sync and close when configured
6. Manifest close
7. Blob manager close
8. VersionLog close
9. MemTable release
```

This order prevents new external traffic first, then drains local mutable state, then closes persistence components.

---

## Concurrency Model

The public storage components are designed to be thread-safe at their public method boundary:

| Component | Guarantee |
|---|---|
| `AkkEngine` | Public methods are thread-safe |
| `MemTable` | Public methods are thread-safe |
| `WalWriter` | Append, sync, and close paths are thread-safe |
| `BlobManager` | Read, write, and delete scheduling are thread-safe |
| `Manifest` | Public methods are thread-safe |
| `VersionLog` | Public methods are thread-safe |
| `ClusterRuntime` | Start, close, and ship paths serialize endpoint access |

Top-level writes are serialized by `AkkEngine::Impl::write_mu`. This is an intentional design choice: it keeps sequence assignment and the side effects around a write in one consistent order.

Background work can include:

- WAL async flusher threads
- MemTable shard flushing
- SST compaction workers
- Manifest fast-mode flusher
- VersionLog async flusher
- Blob cleanup
- API server accept and connection handling
- Cluster manager and replication endpoints

---

## Startup Modes

High-level `AkkaraDB::open` maps `StartupMode` presets into `AkkEngineOptions`.

| Mode | WAL | Blob | Manifest | SST | VersionLog | Close behavior | MemTable threshold |
|---|---|---|---|---|---|---|---|
| `ULTRA_FAST` | disabled | disabled | disabled | disabled | disabled | no force flush/sync | 512 MiB/shard |
| `FAST` | async | enabled | enabled | enabled | disabled | force flush/sync | 256 MiB/shard |
| `NORMAL` | async | enabled | enabled | enabled | disabled | force flush/sync | 64 MiB/shard |
| `DURABLE` | sync | enabled | enabled | enabled | enabled | force flush/sync | 64 MiB/shard |

`FAST` also enables SST read promotion. Fine-grained overrides can change MemTable thresholds, VersionLog, SST and blob codecs, blob thresholds, Bloom filter density, L0 compaction trigger, and SST read promotion.

---

## Relationship to SPEC.md

This architecture document intentionally avoids replacing `SPEC.md`. Use this document to understand component responsibilities and data flow. Use `SPEC.md` as the source of truth for exact binary layouts, magic numbers, CRC ranges, protocol frames, configuration fields, and compatibility rules.

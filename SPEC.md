# AkkaraDB - Technical Specification v5

> Version 0.5.0 - C++23 - Native engine specification - AGPL-3.0

---

## Table of Contents

1. [Overview & Design Goals](#1-overview--design-goals)
2. [Architecture](#2-architecture)
3. [Record & Key Formats](#3-record--key-formats)
4. [Memory Management](#4-memory-management)
5. [MemTable](#5-memtable)
6. [Write-Ahead Log (WAL)](#6-write-ahead-log-wal)
7. [Blob Manager](#7-blob-manager)
8. [Sorted String Tables (SST)](#8-sorted-string-tables-sst)
9. [Manifest](#9-manifest)
10. [Version Log](#10-version-log)
11. [API Servers](#11-api-servers)
12. [Cluster & Replication](#12-cluster--replication)
13. [TLS Support](#13-tls-support)
14. [Configuration Reference](#14-configuration-reference)
15. [Public API Reference](#15-public-api-reference)
16. [File Format Reference](#16-file-format-reference)
17. [Threading & Concurrency Model](#17-threading--concurrency-model)
18. [Error Handling & Recovery](#18-error-handling--recovery)
19. [Build & Integration](#19-build--integration)

---

## 1. Overview & Design Goals

AkkaraDB is a C++23 key-value storage engine designed to scale from an embedded local store to replicated deployments without changing the core key/value model.
It uses an LSM-tree architecture with an in-memory sharded MemTable, optional WAL durability, optional large-value blob externalization, SST flush and
compaction, optional per-key history, embedded HTTP/TCP API servers, and optional cluster replication.

The native C++ implementation is the canonical storage layout. The JVM layer reaches the native engine through JNI and intentionally reuses the same binary
key/value and BinPack layouts.

### Core Properties

| Property     | Guarantee                                                                           |
|--------------|-------------------------------------------------------------------------------------|
| Data model   | Binary-safe key/value records ordered lexicographically by raw key bytes            |
| Durability   | WAL, manifest, SST, blob, and version-log persistence are configurable              |
| Crash safety | CRC32C-protected disk structures and atomic file write patterns where applicable    |
| Concurrency  | `AkkEngine` public methods are thread-safe                                          |
| Reads        | MemTable first, SST fallback, blob dereference when needed                          |
| Writes       | Monotonic sequence assignment under the engine write mutex                          |
| Compression  | Zstd support for SST blocks and blob payloads                                       |
| Typed API    | `PackedTable<&T::field>` with BinPack serialization and secondary indexes           |
| Network      | Embedded HTTP and binary TCP API servers; replication links support TLS/plain modes |

### Non-Goals

- SQL compatibility.
- Cross-language object identity.
- Lock-free writes at the top-level engine API. The engine serializes mutation sequence assignment.
- Stable ABI for low-level AkkEngine headers under `akkara/akkengine/include/akk`.

---

## 2. Architecture

```
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

### Write Path

```
put(key, value)
  |
  +-- reserve seq from MemTable
  +-- maybe externalize large value through BlobManager
  |      |
  |      +-- blob.write(seq, value)
  |      +-- stored value becomes BlobRef(20B)
  |      +-- record flag includes FLAG_BLOB
  |
  +-- append_all(seq, key, stored_value, flags)
         |
         +-- WAL append if enabled
         +-- VersionLog append if enabled
         +-- MemTable put/remove
         +-- Cluster ship_entry if enabled and primary
```

### Read Path

```
get(key)
  |
  +-- snapshot_seq = MemTable::last_seq()
  +-- MemTable::get(key, snapshot_seq)
  |      |
  |      +-- tombstone -> not found
  |      +-- normal    -> value
  |      +-- blob      -> BlobManager::read(blob_id, crc)
  |
  +-- SSTManager::get(key)
         |
         +-- tombstone -> not found
         +-- normal    -> value
         +-- blob      -> BlobManager::read(blob_id, crc)
         +-- optional sstPromoteReads -> put SST record back into MemTable
```

### Range Scan Path

`AkkEngine::scan(arena, start, end)` merges MemTable and SST iterators. When the same key exists in both sources, the MemTable entry wins because it is newer.
Tombstones suppress older SST values.

---

## 3. Record & Key Formats

### 3.1 MemHdr16 - In-Memory Header

`MemHdr16` is the compact in-memory record header used by `OwnedRecord`.

All integer fields are little-endian in serialized memory on the native target. The struct is standard-layout, naturally aligned, and exactly 16 bytes.

```
Offset  Size  Field     Description
------  ----  --------  ---------------------------------------------
0       8     seq       Global monotonic sequence number
8       2     k_len     Key length, 0..65535
10      2     v_len     Value length, 0..65535
12      1     flags     0x00 normal, 0x01 tombstone, 0x02 blob
13      1     version   Header format version, currently 1
14      2     reserved  Reserved, written as zero
```

### 3.2 OwnedRecord - 64-Byte In-Memory Record

`OwnedRecord` owns one in-memory key/value entry.

```
Offset  Size  Field       Description
------  ----  ----------  ---------------------------------------------
0       16    MemHdr16    Sequence, lengths, flags
16      8     keyFp64    SipHash-2-4 fingerprint for fast rejection
24      8     miniKey    First up to 8 key bytes, little-endian packed
32      32    SmallBuffer Inline or arena-backed [key][value] payload
```

`OwnedRecord` is exactly 64 bytes and is optimized for one-cache-line metadata access. The full key comparison remains authoritative; `keyFp64` and `miniKey`
are only acceleration hints.

### 3.3 SSTHdr32 - On-Disk SST Record Header

SST records use a 32-byte header optimized for block-local search.

```
Offset  Size  Field       Description
------  ----  ----------  ---------------------------------------------
0       8     seq         Global sequence number
8       2     k_len       Key length, 0..65535
10      2     v_len       Value length, 0..65535
12      1     flags       0x00 normal, 0x01 tombstone, 0x02 blob
13      1     reserved0   Reserved
14      2     reserved1   Reserved
16      8     keyFp64    64-bit key fingerprint
24      8     miniKey    First up to 8 key bytes, little-endian packed
```

The SST record payload is:

```
[SSTHdr32][key bytes][value bytes]
```

### 3.4 BlobRef

When a value is externalized, the MemTable, WAL, SST, and VersionLog store a 20-byte `BlobRef` as the record value and set the blob flag.

```
Offset  Size  Field             Description
------  ----  ----------------  ---------------------------------------------
0       8     blob_id           Blob identifier, currently the creating seq
8       8     total_size        Original uncompressed value size
16      4     content_crc32c    CRC32C of the original value bytes
```

### 3.5 PackedTable Key Layout

`PackedTable<PrimaryKeyPtr>` maps typed entities onto the raw byte KV engine.

#### Primary Key Entry

```
[table_prefix:8][encoded_pk] -> BinPack::encode(entity)
```

`table_prefix` is `FNV-1a-64(table_name)` written little-endian.

For integral primary keys of size <= 8, `encoded_pk` is a fixed-width sortable big-endian integer using exactly `sizeof(PK)` bytes. Signed integral primary
keys flip the sign bit before big-endian encoding. For non-integral primary keys, `encoded_pk` is `BinPack::encode(pk)`.

#### Secondary Index Entry

```
[index_prefix:8][field_len:u32le][encoded_field][encoded_pk] -> empty value
```

`index_prefix` is `FNV-1a-64(table_name + ":idx:" + field_name)` written little-endian.
`field_len` is the byte length of `encoded_field` written little-endian.

Index entries are non-unique. The encoded primary key suffix makes duplicate field values distinct and allows exact-match index scans to recover the entity by
primary key.

#### RowId Metadata Entries

Each `PackedTable` also maintains a stable per-row identifier independent from the primary-key bytes:

```
[pk2row_prefix:8][encoded_pk]   -> BinPack::encode(RowId)
[row2pk_prefix:8][encoded_rowid] -> encoded_pk
[nextrow_prefix:8]              -> BinPack::encode(next_row_id)
```

`RowId` is currently `uint64_t`. It is allocated on first insert, remains stable across `updatePrimaryKey(...)`, and is removed when the row is deleted.
`Ref<T>` can resolve either by primary key or by remembered `RowId`, which lets references stay attached even when the target table cascades a primary-key
update.

### 3.6 Key Ordering

Raw engine keys are ordered lexicographically by bytes. PackedTable integral primary keys use the same sortable fixed-width big-endian encoding described
above, so typed primary-key scans over integral keys preserve numeric range order.

Secondary index field bytes are encoded separately from BinPack when ordering matters. Integral fields use a sortable big-endian unsigned representation, signed
integrals flip the sign bit before big-endian encoding, and `float`/`double` values use the standard sortable IEEE-754 bit transform before big-endian encoding.
Other field types use `BinPack::encodeInto`. This lets indexed equality and numeric range query plans use bytewise index ranges.

---

## 4. Memory Management

### 4.1 BufferArena

`BufferArena` is used to allocate temporary scan/read buffers whose lifetime is tied to an iterator or API call. `AkkEngine::scan` returns
`ArenaGenerator<ScanRecordView>` values whose key/value spans are valid for the arena lifetime.

### 4.2 OwnedBuffer and BufferView

`OwnedBuffer` provides aligned owning storage for I/O-oriented buffers. `BufferView` provides non-owning access to byte ranges. Together they avoid accidental
ownership transfers in hot storage paths.

### 4.3 SmallBuffer

`SmallBuffer` is embedded inside `OwnedRecord` and stores short `[key][value]` payloads inline. Larger payloads are allocated through the record's arena. This
keeps common small records at a single 64-byte metadata footprint.

---

## 5. MemTable

### 5.1 Sharding

The MemTable is sharded. `MemTable::Options::shard_count == 0` enables automatic derivation. When `AkkEngineOptions::RuntimeOptions::writer_threads > 0`, both
MemTable and WAL shard counts are derived from writer count using a birthday-paradox style estimate and capped by the configured limits.

| Field                         | Default | Description                          |
|-------------------------------|---------|--------------------------------------|
| `shard_count`                 | 0       | Auto when zero                       |
| `expected_concurrent_writers` | 0       | Used by MemTable auto-sharding       |
| `auto_shard_count_cap`        | 128     | MemTable shard cap                   |
| `thresholdBytesPerShard`      | 64 MiB  | Flush hint threshold                 |
| `backend_factory`             | null    | Uses the implementation default      |
| `on_flush`                    | null    | Called with sorted `RecordView` span |

### 5.2 Sequence Model

The MemTable owns the monotonic sequence allocator used by `AkkEngine` writes:

- `reserve_seq(1)` allocates a write sequence.
- `last_seq()` provides the read snapshot for point reads and scans.
- Recovery advances sequence state through replayed records.

### 5.3 Lookup Semantics

`MemTable::get` returns a `RecordView` when a key exists in memory at the requested snapshot. Tombstones are returned as records so the caller can suppress
older SST values.

`MemTable::getInto` and `contains` return `std::optional<bool>`:

| Value     | Meaning                                        |
|-----------|------------------------------------------------|
| `nullopt` | Not found in MemTable; caller should check SST |
| `false`   | Found tombstone                                |
| `true`    | Found live value                               |

### 5.4 Flush Lifecycle

When a shard crosses `thresholdBytesPerShard`, it can be sealed and flushed. The engine installs an `on_flush` callback that writes records to
`SSTManager::flush`, checkpoints the manifest, and prunes WAL segments up to the resulting checkpoint sequence when configured.

---

## 6. Write-Ahead Log (WAL)

### 6.1 Options

| Field                     | Default                                                  | Description                                                                             |
|---------------------------|----------------------------------------------------------|-----------------------------------------------------------------------------------------|
| `walDir`                 | `{dataDir}/wal`                                         | Segment directory                                                                       |
| `syncMode`                | `SYNC` at engine level, changed by `StartupMode` presets | `SYNC`, `ASYNC`, or `OFF`                                                               |
| `shardCount`              | 0                                                        | Auto, one shard per hardware thread capped by implementation; engine writer auto cap 64 |
| `groupN`                  | 128                                                      | Async batch entry trigger                                                               |
| `groupMicros`             | 100                                                      | Async batch time trigger                                                                |
| `groupBytes`              | 4 MiB                                                    | Async batch byte trigger                                                                |
| `asyncMaxPendingBytes`    | 64 MiB                                                   | Backpressure threshold                                                                  |

### 6.2 Segment Header

Every WAL segment begins with a 48-byte `WalSegmentHeader`.

```
Offset  Size  Field        Description
------  ----  -----------  ---------------------------------------------
0       8     segment_id   Per-shard segment id
8       8     created_us   Creation timestamp, microseconds
16      8     first_seq    First sequence in segment, 0 until finalized
24      8     last_seq     Last sequence in segment, 0 until finalized
32      4     magic        0x414B5741 ("AKWA")
36      4     crc32c       CRC32C of header with crc32c zeroed
40      2     version      0x0001
42      2     header_size  48
44      2     shard_id     WAL shard id
46      2     flags        Reserved
```

### 6.3 Entry Header

Each entry is serialized as:

```
[WalEntryHeader:32][key bytes][value bytes]
```

```
Offset  Size  Field       Description
------  ----  ----------  ---------------------------------------------
0       8     seq         Record sequence
8       8     keyFp64    Key fingerprint
16      4     entry_len   Header + key + value bytes
20      4     value_len   Value length
24      2     key_len     Key length
26      2     flags       Record flags
28      4     crc32c      CRC32C over header-with-zero-crc + key + value
```

Recovery validates segment headers and entry CRCs before applying entries to a fresh MemTable.

### 6.4 Sync Modes

| Mode    | Behavior                                           |
|---------|----------------------------------------------------|
| `Sync`  | Synchronous durability path                        |
| `Async` | Grouped background flush path                      |
| `Off`   | No explicit sync; useful only for cache/test modes |

---

## 7. Blob Manager

### 7.1 Purpose

The BlobManager externalizes values whose size is greater than or equal to `thresholdBytes`, default 16 KiB. Externalization keeps MemTable and WAL entries
small while preserving transparent reads at the engine API.

### 7.2 Options

| Field             | Default            | Description               |
|-------------------|--------------------|---------------------------|
| `blobDir`        | `{dataDir}/blobs` | Blob directory            |
| `thresholdBytes`  | 16 KiB             | Externalization threshold |
| `codec`           | `None`             | `None` or `Zstd`          |

### 7.3 Blob Header v5

Blob files use `AkBlobHeaderV5`, exactly 48 bytes.

```
Offset  Size  Field            Description
------  ----  ---------------  ---------------------------------------------
0       4     magic            0x35424B41 ("AKB5")
4       2     version          1
6       2     header_size      48
8       4     flags            Bit 0: Zstd
12      4     codec            0=None, 1=Zstd
16      8     blob_id          Blob id
24      8     total_size       Original content size
32      8     stored_size      Bytes after compression
40      4     content_crc32c   CRC32C of original content
44      4     header_crc32c    CRC32C of header with this field zeroed
```

Blob reads validate the header and content CRC before returning bytes.

---

## 8. Sorted String Tables (SST)

### 8.1 Manager Options

| Field                   | Default              | Description                 |
|-------------------------|----------------------|-----------------------------|
| `sstDir`               | `{dataDir}/sstable` | SST root directory          |
| `max_levels`            | 7                    | Levels L0..L6 by default    |
| `max_l0_files`          | 4                    | L0 compaction trigger       |
| `l1_max_bytes`          | 64 MiB               | L1 budget                   |
| `level_size_multiplier` | 10.0                 | Per-level size multiplier   |
| `target_file_size`      | 64 MiB               | Target output file size     |
| `block_size`            | 32 KiB               | SST block size              |
| `bloom_bits_per_key`    | 10                   | Bloom filter density        |
| `block_cache_bytes`     | 64 MiB               | Block cache budget          |
| `compact_threads`       | 2                    | Compaction worker count     |
| `codec`                 | `Zstd`               | SST block compression codec |

### 8.2 SST v2 File Layout

```
[SSTFileHeaderV2:256]
[data blocks...]
[SSTBlockIndexEntryV2 array]
[key arena]
[SSTBloomHeaderV2][bloom bits]
[SSTFooterV2:48]
```

Each data block stores records in key order. Within a block, keys may be prefix-delta encoded against the previous key before optional block compression. The reader
expands them back to full keys when loading the block into memory.

### 8.3 SSTFileHeaderV2

`SSTFileHeaderV2` is 256 bytes. Important fields:

| Field                                | Description                        |
|--------------------------------------|------------------------------------|
| `magic`                              | `0x32534B41` ("AKS2")              |
| `version`                            | 2                                  |
| `flags`                              | Bit 0 means Zstd-compressed blocks |
| `level`                              | SST level                          |
| `entry_count`                        | Number of records                  |
| `block_count`                        | Number of data blocks              |
| `data_offset`                        | First block offset, normally 256   |
| `index_offset`, `index_size`         | Block index location               |
| `key_arena_offset`, `key_arena_size` | Stored first/last block keys       |
| `bloom_offset`, `bloom_size`         | Bloom filter location              |
| `footer_offset`                      | Footer location                    |
| `min_seq`, `max_seq`                 | Sequence range                     |
| `block_size`                         | Configured target block size       |
| `crc32c`                             | Header CRC                         |

### 8.4 Block Format

Each block is:

```
[SSTBlockHeaderV2:64][payload][record_offsets]
```

The payload is either raw concatenated SST records or Zstd-compressed bytes. The offsets array contains little-endian `uint32_t` offsets into the uncompressed
block payload. Block CRC covers payload plus offsets.

### 8.5 Lookup

SST lookup uses:

1. File-level min/max key checks.
2. Bloom filter negative check.
3. Block index binary search by first/last key.
4. In-block binary search using `SSTHdr32`, `miniKey`, and full key comparison.

L0 may contain overlapping files; newer files are checked first by manager policy. Higher levels are expected to be non-overlapping after compaction.

---

## 9. Manifest

### 9.1 Purpose

The Manifest is an append-only, CRC-protected log of storage lifecycle events. The main engine manifest (`manifest.akmf`) tracks live SST files, compaction
transitions, and checkpoints. A separate node-local cluster manifest (`cluster.akmf`) records cluster runtime events.

### 9.2 File Format

```
[ManifestFileHeader:32][ManifestRecordHeader:8][payload]...
```

`ManifestFileHeader`:

| Field           | Description                |
|-----------------|----------------------------|
| `magic`         | `0x35564D41` ("AMV5")      |
| `version`       | 1                          |
| `flags`         | Reserved                   |
| `file_seq`      | Manifest rotation sequence |
| `created_at_us` | Creation timestamp         |
| `crc32c`        | Header CRC                 |

`ManifestRecordHeader`:

```
[type:u8][flags:u8][payload_len:u16][crc32c:u32]
```

The record CRC covers payload bytes only.

### 9.3 Record Types

| Value  | Name               | Purpose                             |
|--------|--------------------|-------------------------------------|
| `0x01` | `StripeCommit`     | Stripe counter advance              |
| `0x02` | `SSTSeal`          | New SST was sealed                  |
| `0x03` | `SSTDelete`        | Legacy SST deletion                 |
| `0x04` | `CompactionStart`  | Informational compaction start      |
| `0x05` | `CompactionEnd`    | Legacy single-output compaction end |
| `0x06` | `Checkpoint`       | Named or unnamed checkpoint         |
| `0x07` | `Truncate`         | Informational truncation marker     |
| `0x08` | `CompactionCommit` | Atomic multi-file compaction commit |
| `0x10` | `NodeJoin`         | Cluster node join                   |
| `0x11` | `NodeLeave`        | Cluster node leave                  |
| `0x12` | `PrimaryLease`     | Cluster primary lease               |

`CompactionCommit` is the preferred compaction record because replay either applies all output/input file changes or none.

The cluster manifest currently records:

- `NodeJoin` when a node starts and advertises its replication endpoint
- `NodeLeave` when a node shuts down cleanly
- `PrimaryLease` when a node starts as `PRIMARY`

Replay of `manifest.akmf` rebuilds SST lifecycle state. The cluster manifest is a durable local event log for cluster runtime breadcrumbs; it is not a distributed
leader-election source of truth.

---

## 10. Version Log

### 10.1 Purpose

The VersionLog records per-key history when enabled. It powers:

- `AkkEngine::getAt(key, seq)`
- `AkkEngine::history(key)`
- `AkkEngine::rollbackTo(seq)`
- `AkkEngine::rollbackKey(key, seq)`

### 10.2 Options

| Field       | Default                     | Description       |
|-------------|-----------------------------|-------------------|
| `logPath`  | `{dataDir}/history.akvlog` | Version log file  |
| `syncMode` | `ASYNC`                     | `SYNC`, `BATCHED_SYNC`, or `ASYNC` |

`SYNC` writes the entry, flushes the stdio buffer, and issues a durability sync before returning. `BATCHED_SYNC` updates the in-memory history immediately, writes
through a background flusher, and issues durability sync per batch. `ASYNC` also uses the background flusher but does not guarantee per-batch durability at method
return.

### 10.3 VersionEntry

```
seq            uint64
source_node_id uint64
timestamp_ns   uint64
flags          uint8
value          bytes
```

Rollback-generated records use `ROLLBACK_NODE = UINT64_MAX` and `VLOG_FLAG_ROLLBACK = 0x04`.

Version-log recovery is fail-fast: corrupt headers, malformed entries, truncated payloads, and CRC mismatches raise `std::runtime_error` during open instead of
being silently ignored.

---

## 11. API Servers

### 11.1 Configuration

API servers are enabled through `AkkEngineOptions::components.apiEnabled`. The server set is controlled by `AkkEngineOptions::api.backends`.
When `backends` is empty, the native server starts HTTP and TCP backends. GRPC is an available backend enum and can be enabled when the gRPC transport factory
has been registered by the gRPC module.

| Field                  | Default     | Description                                              |
|------------------------|-------------|----------------------------------------------------------|
| `backends`             | empty       | Values: `HTTP`, `TCP`, `GRPC`; empty means HTTP + TCP    |
| `bindHost`             | empty       | Required when API is enabled                             |
| `httpPort`             | 7070        | HTTP port                                                |
| `tcpPort`              | 7071        | Binary TCP port                                          |
| `grpcPort`             | 7072        | gRPC port when GRPC backend is enabled                   |
| `tcpWorkerThreads`     | 0           | TCP worker threads. `0` uses hardware concurrency.       |
| `tcpAcceptQueueLimit` | 4096      | Max accepted TCP sockets waiting for a worker.           |
| `tcpAcceptQueueTimeoutMs` | 60000 | Max time a socket may wait in the worker queue. `0` disables it. |
| `tcpListenBacklog`    | 1024        | Kernel listen backlog passed to `listen`.                |
| `tcpReadTimeoutMs`    | 60000       | TCP idle/partial-frame read timeout. `0` disables it.    |
| `tcpWriteTimeoutMs`   | 30000       | TCP response write timeout. `0` disables it.             |
| `transportMode`        | `TLS`       | `TLS` or `PLAIN`                                         |
| `tls`                  | empty paths | TLS/PSK options                                          |

### 11.2 Binary Protocol v2

Request header, 16 bytes:

```
char[4] magic = "AK5Q"
u8      version = 2
u8      opcode
u32     request_id
u16     key_len
u32     val_len
```

Response header, 13 bytes:

```
char[4] magic = "AK5S"
u8      status
u32     request_id
u32     val_len
```

TCP request frames are `[ApiRequestHeader][key bytes][value bytes][crc32c:u32le]`, where the request CRC32C covers `key bytes + value bytes`. TCP response frames
are `[ApiResponseHeader][value bytes][crc32c:u32le]`, where the response CRC32C covers `value bytes`.

Opcodes:

| Value  | Name     |
|--------|----------|
| `0x01` | `Get`    |
| `0x02` | `Put`    |
| `0x03` | `Remove` |
| `0x04` | `GetAt`  |
| `0x05` | `BatchPut` |
| `0x06` | `BatchGet` |
| `0x07` | `Ping` |
| `0x08` | `Exists` |
| `0x09` | `Count` |
| `0x0A` | `Scan` |
| `0x0B` | `History` |
| `0x0C` | `RollbackTo` |
| `0x0D` | `RollbackKey` |
| `0x0E` | `ForceSync` |
| `0x0F` | `ForceFlush` |
| `0x10` | `Stats` |

Statuses:

| Value  | Name       |
|--------|------------|
| `0x00` | `Ok`       |
| `0x01` | `NotFound` |
| `0xFF` | `Error`    |

### 11.3 HTTP API

The HTTP server exposes REST-style endpoints for basic key/value operations. Keys are passed as percent-encoded query parameters and values are request/response
bodies.

| Method   | Path           | Query              | Description                                                 |
|----------|----------------|--------------------|-------------------------------------------------------------|
| `GET`    | `/v1/ping`     | none               | Health check                                                |
| `POST`   | `/v1/put`      | `key`              | Store request body as value                                 |
| `GET`    | `/v1/get`      | `key`              | Return current value                                        |
| `DELETE` | `/v1/remove`   | `key`              | Write tombstone                                             |
| `GET`    | `/v1/exists`   | `key`              | Return point existence                                      |
| `GET`    | `/v1/count`    | `start`, `end`     | Count records in a half-open range                          |
| `GET`    | `/v1/scan`     | `start`, `end`, `limit` | Return range records, bounded by server limit          |
| `GET`    | `/v1/getAt`    | `key`, `seq`       | Return value visible at sequence when VersionLog is enabled |
| `GET`    | `/v1/history`  | `key`, `limit`     | Return version entries                                      |
| `POST`   | `/v1/rollbackTo` | `seq`            | Roll the engine back to a sequence                          |
| `POST`   | `/v1/rollbackKey` | `key`, `seq`    | Roll one key back to a sequence                             |
| `POST`   | `/v1/batchPut` | body               | Store multiple key/value pairs                              |
| `POST`   | `/v1/batchGet` | body               | Read multiple keys                                          |
| `POST`   | `/v1/forceSync` | none              | Force WAL sync                                              |
| `POST`   | `/v1/forceFlush` | none             | Force MemTable flush                                        |
| `GET`    | `/v1/stats`    | none               | Return engine/server stats                                  |

HTTP batch request bodies are little-endian binary payloads. `batchPut` uses
`count:u32le` followed by `count` entries of `key_len:u32le`, `value_len:u32le`, `key bytes`, and `value bytes`. `batchGet` uses `count:u32le` followed by
`count` entries of `key_len:u32le` and `key bytes`. The `batchGet` response uses `count:u32le` followed by `status:u8`, `value_len:u32le`, and `value bytes` per
entry. The TCP `BatchPut` and `BatchGet` opcodes use the same entry shapes, except per-entry key lengths are `u16le` to match the TCP request header key limit.

### 11.4 gRPC API

The gRPC module defines `akkaradb.grpcapi.v1.AkkaraDB` with unary RPCs for `Ping`, `Put`, `Get`, `Remove`, `Exists`, `Count`, `Scan`, `GetAt`, `History`,
`RollbackTo`, `RollbackKey`, `BatchPut`, `BatchGet`, `ForceSync`, `ForceFlush`, and `Stats`. It can run with insecure credentials or TLS credentials depending
on the configured certificate/key/CA paths.

---

## 12. Cluster & Replication

### 12.1 Cluster Modes

| Mode         | Description                                      |
|--------------|--------------------------------------------------|
| `Standalone` | No replication                                   |
| `Mirror`     | Writes are mirrored to all data-bearing nodes    |
| `Stripe`     | Keys are assigned to data nodes by router policy |

`Stripe` uses the cluster router's deterministic rendezvous-hash placement to assign each key to one data-bearing node. `ClusterRuntime` accepts Stripe configs
and exposes the same owner selection through `ClusterRuntime::router()`. Operational ownership migration remains an explicit administrative concern when changing
the node set or moving data between placement policies.

### 12.2 Node Roles

| Role         | Description                          |
|--------------|--------------------------------------|
| `Standalone` | Local-only operation                 |
| `Primary`    | Accepts and ships writes             |
| `Replica`    | Applies replicated records and blobs |

`ReplicationMode` and `NodeRole` are separate concerns. `Standalone`, `Mirror`, and `Stripe` describe deployment topology; `Primary` and `Replica` describe
runtime responsibility inside non-standalone topologies. `Standalone` mode does not require an explicit startup role. `Mirror` and `Stripe` require
`ClusterRuntimeOptions::startupRole` to be set to `PRIMARY` or `REPLICA`; `AUTO` is rejected at runtime.

### 12.3 Ack Policy

| Mode     | Description                      |
|----------|----------------------------------|
| `Async`  | Do not wait for acknowledgements |
| `All`    | Wait for all live replicas       |
| `Quorum` | Wait for configured quorum       |

### 12.4 Cluster Config

Cluster config is stored as `{dataDir}/cluster.akcc` by default and uses magic `0x35434B41` ("AKC5"), version 1. It stores:

- node ids
- host names advertised to peer nodes
- data ports
- replication ports
- node capabilities
- replication mode
- acknowledgement policy

Runtime-only TLS/transport paths and the local replication bind host are not serialized in the config file. They live in `ClusterRuntimeOptions`.
The advertised `NodeInfo.host` is the address peers dial; `ClusterRuntimeOptions::replBindHost` is the local address the primary listener binds to, defaulting to `0.0.0.0`.
Replication links run over TCP. `TransportMode::SECURE` wraps the TCP stream with the native secure channel; `TransportMode::PLAIN` is accepted only when every advertised node host is loopback or LAN/private address space.

### 12.5 Startup Role Resolution

There is no automatic primary election in the current runtime. For `Mirror` and `Stripe`, the process must start explicitly as either `PRIMARY` or `REPLICA`.

`PRIMARY` startup requirements:

- `selfNodeId` must exist in the cluster config
- the local node must be `coordinatorEligible()`
- the local node's configured `replPort` is used for the replication listener

`REPLICA` startup requirements:

- `primaryNodeId` must be provided either directly in `ClusterRuntimeOptions` or through `secure.expectedPrimaryNodeId`
- the primary target cannot be the local node
- a reachable primary host and replication port must be known
- if `primaryNodeId` exists in the cluster config, missing `primaryHost` and `primaryReplPort` are filled from that config entry
- if the configured primary node is found in the config, it must be `coordinatorEligible()`

Runtime startup fails fast with `std::runtime_error` when these requirements are not met. This is intentional: split-brain-safe failover, quorum leader election,
and automatic primary re-selection are out of scope for the current cluster layer.

Changing the primary for non-mirrored ownership still requires an explicit migration plan. The new primary must not retain unrelated user data, and owned data must
be moved back to the node selected by the placement policy before traffic is accepted.

### 12.6 Runtime Integration

`ClusterRuntime` receives engine callbacks for current sequence, last applied sequence, record application, blob application, and role changes. The engine calls
`ship_entry` and `ship_blob` after local writes when cluster runtime is active.

When running as `PRIMARY`, `ClusterRuntime` opens a `ReplicationServer` on the local node's configured replication port and binds it to
`ClusterRuntimeOptions::replBindHost`. When running as `REPLICA`, it opens a `ReplicationClient` and dials `primaryHost:primaryReplPort`.

`ClusterManager` also maintains `{dataDir}/cluster.akmf`. On successful startup it records `NodeJoin`; on clean shutdown it records `NodeLeave`; and when the
node starts as `PRIMARY` it records a `PrimaryLease` event containing the local node id and lease-until timestamp.

Replication ingress is primary-centric: replicas are replication consumers, not direct write ingress endpoints. In `Stripe` mode, key ownership is still decided by
the deterministic router, but ownership migration and operational traffic placement remain explicit administrative concerns.

---

## 13. TLS Support

TLS support is compiled into the current native target unconditionally. The CMake file fetches mbedTLS 3.6.2, links `mbedtls`, `mbedcrypto`, and `mbedx509`, and
defines `AKKARADB_TLS_ENABLED`.

### 13.1 TlsConfig

```
cert_path     PEM certificate path
key_path      PEM private key path
ca_path       CA path for peer verification
psk           Optional PSK bytes
psk_len       PSK length
psk_identity  Optional PSK identity
verify_peer   Whether peer verification is required
```

### 13.2 TlsStream

`TlsStream` wraps one TCP connection and provides blocking `connect`, `accept`, `send`, `recv`, `shutdown`, and `close` operations. It owns the accepted or
connected socket after setup begins.

TLS is used by API servers when enabled. Cluster replication does not use `TlsStream`; it uses the native secure channel when
`ClusterRuntimeOptions::transportMode == TransportMode::SECURE`, or plain TCP when `transportMode == TransportMode::PLAIN`.

---

## 14. Configuration Reference

### 14.1 StartupMode Presets

High-level `AkkaraDB::open` converts `StartupMode` into `AkkEngineOptions`.

| Mode         | WAL           | Blob     | Manifest | SST      | VersionLog | Close behavior      | MemTable threshold   |
|--------------|---------------|----------|----------|----------|------------|---------------------|----------------------|
| `ULTRA_FAST` | disabled      | disabled | disabled | disabled | disabled   | no force flush/sync | 512 MiB/shard        |
| `FAST`       | enabled async | enabled  | enabled  | enabled  | disabled   | force flush/sync    | 256 MiB/shard        |
| `NORMAL`     | enabled async | enabled  | enabled  | enabled  | disabled   | force flush/sync    | default 64 MiB/shard |
| `DURABLE`    | enabled sync  | enabled  | enabled  | enabled  | enabled    | force flush/sync    | default 64 MiB/shard |

`FAST` also enables `runtime.sstPromoteReads`.

### 14.2 AkkaraDB::Options Overrides

| Override                       | Maps to                              |
|--------------------------------|--------------------------------------|
| `memtableThresholdPerShard`    | `memtable.thresholdBytesPerShard`    |
| `versionLogEnabled`            | `components.versionLogEnabled`       |
| `sstCodec`                     | `sst.codec`                          |
| `blobCodec`                    | `blob.codec`                         |
| `blobThresholdBytes`           | `blob.thresholdBytes`                |
| `sstPromoteReads`              | `runtime.sstPromoteReads`            |
| `sstBloomBitsPerKey`           | `sst.bloomBitsPerKey`                |
| `maxL0SstFiles`                | `sst.maxL0Files`                     |

### 14.3 Path Defaults

If `paths.dataDir` is set, missing component paths are derived as:

| Path                  | Default                     |
|-----------------------|-----------------------------|
| `walDir`             | `{dataDir}/wal`            |
| `blobDir`            | `{dataDir}/blobs`          |
| `sstDir`             | `{dataDir}/sstable`        |
| `manifestPath`       | `{dataDir}/manifest.akmf`  |
| `versionLogPath`    | `{dataDir}/history.akvlog` |
| `clusterConfigPath` | `{dataDir}/cluster.akcc`   |
| `nodeIdPath`        | `{dataDir}/node.id`        |

### 14.4 Runtime Options

| Field                  | Default | Description                                |
|------------------------|---------|--------------------------------------------|
| `writerThreads`        | 0       | Derives MemTable/WAL shard counts when > 0 |
| `recoverWal`           | true    | Replay WAL at startup                      |
| `recoverSst`           | true    | Recover SST state at startup               |
| `pruneWalOnFlush`      | true    | Prune WAL after SST checkpoint             |
| `forceFlushOnClose`    | true    | Force MemTable flush during close          |
| `forceSyncOnClose`     | true    | Force WAL sync during close                |
| `sstPromoteReads`      | false   | Promote SST read hits into MemTable        |

### 14.5 Cluster Runtime Options

| Field                      | Default   | Description                                                             |
|----------------------------|-----------|-------------------------------------------------------------------------|
| `transportMode`            | `SECURE`  | Replication transport: native secure channel or plain TCP               |
| `replBindHost`             | `0.0.0.0` | Local address used by the primary replication listener                  |
| `startupRole`              | `AUTO`    | Explicit runtime role; `AUTO` is rejected for `Mirror` and `Stripe`     |
| `primaryHost`              | empty     | Replica-side override for the primary host                              |
| `primaryReplPort`          | `0`       | Replica-side override for the primary replication port                  |
| `primaryNodeId`            | `0`       | Replica-side override for the primary node id                           |
| `secure.identitySeedPath`  | empty     | Persistent local identity seed path for the native secure channel       |
| `secure.pinnedPeers`       | empty     | Optional raw-public-key pins keyed by cluster node id                   |
| `secure.expectedPrimaryNodeId` | `0`   | Replica-side expected primary id when `primaryNodeId` is not supplied   |

`PLAIN` replication is rejected for non-private advertised node hosts. Hostnames other than `localhost` are treated as non-private because the runtime does not
resolve DNS during configuration validation. When `transportMode == SECURE` and `secure.identitySeedPath` is empty, the runtime defaults it to
`{dataDir}/cluster.identity` if a database directory is available.

---

## 15. Public API Reference

### 15.1 AkkaraDB

```cpp
#include <akkaradb/AkkaraDB.hpp>

auto db = akkaradb::AkkaraDB::open("/var/lib/akkaradb", akkaradb::StartupMode::NORMAL);

akkaradb::AkkaraDB::Options opts;
opts.dataDir = "/var/lib/akkaradb";
opts.mode = akkaradb::StartupMode::FAST;
opts.overrides.blobThresholdBytes = 32 * 1024;
opts.overrides.sstCodec = akkaradb::Codec::ZSTD;
auto tuned = akkaradb::AkkaraDB::open(std::move(opts));

auto& engine = tuned->engine();
tuned->close();
```

### 15.2 AkkEngine

```cpp
std::vector<uint8_t> key = {'k'};
std::vector<uint8_t> value = {'v'};

engine.put(key, value);
auto got = engine.get(key);                 // optional<vector<uint8_t>>
bool ok = engine.getInto(key, value);      // reuse output vector
bool exists = engine.exists(key);
engine.remove(key);

akkaradb::core::BufferArena arena;
auto rows = engine.scan(arena);
for (auto it = rows.begin(); it != rows.end(); ++it) {
    auto row = *it;
}

auto hist = engine.history(key);
auto old = engine.getAt(key, 42);
engine.rollbackKey(key, 42);
engine.forceFlush();
engine.forceSync();
```

`putHinted` and `removeHinted` are available for callers that already computed `keyFp64` and `miniKey`.

### 15.3 PackedTable

```cpp
struct User {
    uint64_t id;
    std::string email;
    std::string name;
    uint32_t age;
};

AKKARADB_QUERYABLE(User, id, email, name, age)

auto users = db->table<&User::id>("users");
auto by_email = users.index<&User::email>();
users.indexed<&User::age>();

users.put(User{1, "a@example.test", "Alice", 30});
auto alice = users.get(1);

User out{};
bool found = users.getInto(1, out);

auto emailHits = by_email.find(std::string{"a@example.test"});
while (emailHits.hasNext()) {
    auto [id, user] = emailHits.next();
}

auto adults = users
    .query([](auto u) { return u.age >= 18; })
    .limit(100)
    .toVector();

auto first = users.query()
    .where([](auto u) { return u.email == "a@example.test"; })
    .first();

auto exact = users.findBy<&User::email>(std::string{"a@example.test"});
auto range = users.scan(1ULL, 100ULL);
```

`PackedTable` supports `put`, `get`, `getInto`, `remove`, `exists`, `upsert`, `count`, `scanAll`, `scan(startPk)`, `scan(startPk, endPk)`, `index`,
`indexed`, `findBy`, and `query` helpers. Query predicates support comparison, `&&`, `||`, `!`, `in`, `notIn`, `startsWith`, `contains`, `like`, `isNull`,
`isNotNull`, nested struct access through `.field<&Nested::member>()`, and map access through `.mapGet(key)` / `.get(key)`.

When a registered secondary index can provide a narrower source range, the query planner scans that index and applies the predicate as a residual filter. Equality,
`in`, optional null checks, and arithmetic range predicates can use indexes. String prefix/contains/like predicates may use the field index as the source and
still apply the full predicate in C++.

`PackedTable` is move-only and not documented as thread-safe. Use separate handles or external synchronization when sharing across threads.

### 15.4 Ref, Schema, and Joins

`Ref<T>` stores only the target primary key in BinPack, and can lazily resolve the target entity when a table binding is attached. `AKKARADB_ENTITY` combines
`AKKARADB_QUERYABLE` with `AKKARADB_REF_ENTITY`, which defines `RefTraits<T>` for the primary key.

```cpp
struct Author {
    uint64_t id;
    std::string name;
    uint32_t age;
    std::string email;
};

AKKARADB_ENTITY(Author, id, name, age, email);

struct Post {
    uint64_t id;
    akkaradb::Ref<Author> author;
    std::string body;
    uint32_t likes;
    std::string title;
};

AKKARADB_ENTITY(Post, id, author, body, likes);

auto schema = db->schema()
    .table<&Author::id>("authors")
    .table<&Post::id>("posts")
    .foreignKey<&Post::author>()
    .open();

auto& authors = schema.table<Author>();
auto& posts = schema.table<Post>();

authors.put({1, "Alice", 30, "alice@example.test"});
posts.put({100, akkaradb::ref<Author>(1), "hello", 5, "first"});

auto post = posts.get(100);
auto authorName = post->author->name; // lazy resolve through the attached binding

auto joined = posts.join<&Post::author>(authors).toVector();
```

`foreignKey<&Owner::refField>()` validates that the referenced entity exists before storing the owner row. Both `Ref<T>` foreign keys and plain comparable
fields can be registered through `Schema::foreignKey(...)`, and both `OnDelete` and `OnUpdate` support exactly one of `Cascade`, `Restrict`, or `SetNull`.
`SetNull` requires the owner field to be `std::optional<...>`. `Ref<T>` foreign keys currently target the referenced entity primary key; arbitrary non-primary
target fields are supported only for plain comparable owner fields.

Primary-key updates are explicit through `updatePrimaryKey(oldPk, entity)`. When a referenced target primary key changes, `OnUpdate::Cascade` rewrites the
owner-side foreign-key value, `OnUpdate::Restrict` rejects the change while references exist, and `OnUpdate::SetNull` clears optional owner-side references.
For `Ref<T>`, the internal remembered `RowId` is preserved so the reference continues to point at the same logical entity after the target primary-key rewrite.

Manual table binding is also available through `bindRef<&Owner::refField>(targetTable)`, and manual cascade registration is available through
`cascadeDeleteFrom<&Owner::refField>(sourceTable)`. Joins can use either a `Ref<T>` field or arbitrary comparable fields:

```cpp
auto byRef = posts.join<&Post::author>(authors);

struct PlainPost {
    uint64_t id;
    uint64_t authorId;
    std::string body;
    uint32_t likes;
};

AKKARADB_QUERYABLE(PlainPost, id, authorId, body, likes)

auto plainPosts = db->table<&PlainPost::id>("plain_posts");
auto byField = plainPosts.join<&PlainPost::authorId, &Author::id>(authors);
```

`PackedTable` also exposes stable row-identity helpers:

```cpp
auto rowId = authors.rowIdOf(1ULL);
auto sameAuthor = rowId ? authors.getByRowId(*rowId) : std::nullopt;
auto pk = rowId ? authors.primaryKeyOf(*rowId) : std::nullopt;
```

`rowIdOf(pk)` returns the stable row id for the current row version, `primaryKeyOf(rowId)` resolves the current primary key, and `getByRowId(rowId)` loads by
stable identity. These mappings are implementation metadata and not part of the user entity payload.

Persisted entities can mark fields as immutable with `akkaradb::Immutable<T>` or the shorter alias `akkaradb::Const<T>`:

```cpp
struct Author {
    uint64_t id;
    akkaradb::Const<std::string> externalId;
    std::string name;
};
```

Immutable fields are writable before the row is persisted, are sealed after `put(...)` / `get(...)`, and any later attempt to change their value causes
`std::runtime_error`. Primary-key fields themselves cannot use `Immutable<T>`.

`PackedTable::onUpdate<&Field>(handler)` registers local update hooks that run when a stored row is overwritten or when `updatePrimaryKey(...)` replaces it and
the watched field changed. Handlers may observe both old and new values and may mutate the new entity. Accepted callable shapes are:

```cpp
table.onUpdate<&Post::title>([](const std::string& oldValue, std::string& newValue) {
    if (newValue.empty()) { newValue = oldValue; }
});

table.onUpdate<&Post::body>([](const Post& oldEntity, Post& newEntity) {
    if (oldEntity.body != newEntity.body) { newEntity.likes = 0; }
});
```

Supported signatures are `(oldField, Field& newField)`, `(oldField, Field& newField, oldEntity, Entity& newEntity)`, `(oldEntity, Entity& newEntity)`, and
their read-only `newField` / `newEntity` variants.

### 15.5 BinPack

```cpp
auto bytes = akkaradb::binpack::BinPack::encode(value);
auto value2 = akkaradb::binpack::BinPack::decode<T>(bytes);
akkaradb::binpack::BinPack::encodeInto(value, out);
size_t n = akkaradb::binpack::BinPack::estimateSize(value);
```

Built-in adapters include:

- arithmetic and enum types
- `std::string` and write-only `std::string_view`
- `std::vector<uint8_t>`
- `std::optional<T>`
- `std::vector<T>`
- `std::array<T, N>`
- `std::map<K, V>` and `std::unordered_map<K, V>`
- `std::pair<A, B>` and `std::tuple<Ts...>`
- aggregate structs through Boost.PFR

Integers are encoded little-endian by BinPack.

`Ref<T>` is encoded as its key only. Aggregate structs are encoded field-by-field through Boost.PFR, except trivially copyable aggregates which currently use a
memcpy fast path.

### 15.6 Erasure Codecs

The low-level engine also exposes erasure-coding helpers under `akk/engine/erasure/`.

`ErasureCodec.hpp` defines:

- `XorErasureCodec`: `k + 1` layout with single-shard recovery.
- `DualXorErasureCodec`: `k + 2` layout with two dedicated parity shards and recovery of up to two missing data shards.
- `RsErasureCodec`: systematic Reed-Solomon over GF(256) for erasure recovery from any `k` valid shards.

All three share:

```cpp
std::vector<ErasureShard> encode(std::span<const uint8_t> value, ErasureLayout layout);
std::vector<uint8_t> decode(std::span<const ErasureShard> shards, ErasureLayout layout);
ErasureShard repairOne(uint16_t missingIndex, std::span<const ErasureShard> shards, ErasureLayout layout);
```

`ErasureShard` stores `index`, `originalSize`, `codec`, payload bytes, and a CRC32C over the shard payload. `repairOne(...)` is defined for the erasure case
where the missing shard index is already known.

`ErasureCodecExt.hpp` adds `ErsCodec`, which is intentionally separate from the erasure-only interface. `ErsCodec` uses the same systematic RS shard layout as
`RsErasureCodec::encode(...)`, but its recovery API can identify corrupted shards in addition to known erasures:

```cpp
auto shards = ErsCodec::encode(bytes, {.dataShards = 6, .parityShards = 3});
auto recovered = ErsCodec::recover(shards, {.dataShards = 6, .parityShards = 3});
auto recovered2 = ErsCodec::recover(shards, {.dataShards = 6, .parityShards = 3}, {1, 4});
```

`ErsRecoveryResult` returns the recovered value, any repaired shard payloads, explicit missing indices, and detected corrupt-shard indices. The current native
implementation performs bounded search for up to two unknown corrupted shards in addition to known erasures. `knownBadIndices` lets the caller pre-declare
missing or suspicious shard indices that should be treated as erasures before unknown-error search starts.

### 15.7 JNI Bridge

When `AKKARADB_BUILD_JNI=ON`, the native build produces `akkaradb_jni`. The JNI bridge exposes the raw engine operations, scan cursors, query scan transport,
and option-based open used by the JVM module. The public JVM engine API uses `ByteBufferL`; the current JNI native methods receive direct `java.nio.ByteBuffer`
objects made from those buffers and call native `AkkaraDB::open(options)`.

The JNI engine entry points are:

| JVM method         | Native method                | Notes                                                 |
|--------------------|------------------------------|-------------------------------------------------------|
| `open(options)`    | `nativeOpen`                 | Maps JVM startup mode, codecs, and overrides to C++   |
| `put`              | `nativePut`                  | Raw key and value bytes                               |
| `get`              | `nativeGet`                  | Returns `null` when the key is absent                 |
| `remove`           | `nativeRemove`               | Tombstone write                                       |
| `exists`           | `nativeExists`               | Point existence check                                 |
| `count`            | `nativeCount`                | Optional `[start_key, end_key)` range                 |
| `scan`             | `nativeOpenScan`             | Returns a native cursor handle                        |
| `scanQuery`        | `nativeOpenQueryScan`        | Adds query and schema payloads to the scan cursor     |
| `rollbackTo`       | `nativeRollbackTo`           | Requires version log state that can serve the target  |
| `close`            | `nativeClose`                | Closes the owning `AkkaraDB`                          |
| cursor `next`      | `NativeScanCursor.nativeNext` | Returns JVM `RowView(ByteBufferL key, ByteBufferL value)` |
| cursor `close`     | `NativeScanCursor.nativeClose` | Destroys the native cursor                            |

#### 15.7.1 JVM Query Scan Payloads

`nativeOpenQueryScan(handle, startKey, endKey, queryBytes, schemaBytes)` opens a normal native scan over `[startKey, endKey)` and evaluates the decoded query
against each row value before yielding it. The row value is decoded with the supplied schema. The query payload is produced by the JVM `AstSerializer`; the schema
payload is produced by `SchemaSerializer`.

All query and schema payload integer fields are little-endian. Payload strings are UTF-8.

#### 15.7.2 Query Payload

The query payload contains captures followed by one recursive expression tree:

```text
QueryPayload:
  capture_count:u32le
  captures[capture_count]:Literal
  where:Expr
```

Literal format:

```text
Literal:
  tag:u8
  payload
```

| Literal tag | Type     | Payload                         |
|-------------|----------|---------------------------------|
| `0x01`      | Bool     | `value:u8`, `0` false, else true |
| `0x02`      | Int8     | `value:u8` interpreted signed    |
| `0x03`      | Int16    | `value:i16le`                    |
| `0x04`      | Int32    | `value:i32le`                    |
| `0x05`      | Int64    | `value:i64le`                    |
| `0x06`      | Float    | IEEE-754 bits as `u32le`         |
| `0x07`      | Double   | IEEE-754 bits as `u64le`         |
| `0x08`      | String   | `len:i32le`, then UTF-8 bytes    |
| `0x09`      | Null     | none                             |
| `0x0A`      | List     | `count:u32le`, then `count` nested `Literal` values |
| `0x0B`      | Map      | `count:u32le`, then `count` nested `Literal` key/value pairs |

Expression format:

```text
Expr:
  Bin = tag 0x01, op:u8, lhs:Expr, rhs:Expr
  Un  = tag 0x02, op:u8, x:Expr
  Lit = tag 0x03, literal:Literal
  Col = tag 0x04, name_len:u16le, name_utf8
  Cap = tag 0x05, capture_index:u32le
```

`op` is `AkkOp.ordinal + 1`. The current JVM operator order is:

| op byte | Operator      |
|---------|---------------|
| `0x01`  | `GT`          |
| `0x02`  | `GE`          |
| `0x03`  | `LT`          |
| `0x04`  | `LE`          |
| `0x05`  | `EQ`          |
| `0x06`  | `NEQ`         |
| `0x07`  | `AND`         |
| `0x08`  | `OR`          |
| `0x09`  | `NOT`         |
| `0x0A`  | `IN`          |
| `0x0B`  | `NOT_IN`      |
| `0x0C`  | `IS_NULL`     |
| `0x0D`  | `IS_NOT_NULL` |
| `0x0E`  | `MAP_GET`     |
| `0x0F`  | `STARTS_WITH` |
| `0x10`  | `CONTAINS`    |
| `0x11`  | `LIKE`        |

The current native evaluator implements comparison, equality, boolean, membership, map lookup, string predicate, null-check, capture, literal, and column
expressions used by the JVM scan path. Unsupported operators must be treated as query-evaluation errors rather than silently matching rows.

#### 15.7.3 Schema Payload

The schema payload describes the BinPack layout of the row value. The root is always a struct schema and does not include a leading `Struct` kind byte:

```text
StructSchema:
  field_count:u8
  fields[field_count]:
    name_len:u16le
    name_utf8
    type:TypeDescriptor
```

Type descriptor format:

```text
TypeDescriptor:
  kind:u8
  payload depending on kind
```

| Kind   | Type     | Payload                                      |
|--------|----------|----------------------------------------------|
| `0x01` | Bool     | none                                         |
| `0x02` | Int8     | none                                         |
| `0x03` | Int16    | none                                         |
| `0x04` | Int32    | none                                         |
| `0x05` | Int64    | none                                         |
| `0x06` | Float    | none                                         |
| `0x07` | Double   | none                                         |
| `0x08` | String   | none                                         |
| `0x0A` | List     | `element:TypeDescriptor`                     |
| `0x0B` | Map      | `key:TypeDescriptor`, `value:TypeDescriptor` |
| `0x0C` | Struct   | nested `StructSchema`                        |
| `0x0D` | Nullable | inner non-null `TypeDescriptor`              |

Nested column names use dot-separated paths, for example `address.city`. During evaluation, native code walks the schema and skips unrelated BinPack fields.
Primitive values and strings can be read for predicates. Struct, list, and map values are currently skipped unless the expression descends through a struct field.

---

## 16. File Format Reference

### 16.1 Magic Numbers

| File              | Magic                 | Version    |
|-------------------|-----------------------|------------|
| WAL segment       | `AKWA` / `0x414B5741` | 1          |
| SST v2            | `AKS2` / `0x32534B41` | 2          |
| SST footer v2     | `A2SF` / `0x46533241` | 2          |
| Blob v5           | `AKB5` / `0x35424B41` | 1          |
| Manifest v5       | `AMV5` / `0x35564D41` | 1          |
| Cluster config v5 | `AKC5` / `0x35434B41` | 1          |
| API request       | `AK5Q`                | protocol 2 |
| API response      | `AK5S`                | protocol 2 |

### 16.2 Endianness

Internal disk structures in WAL, SST, Blob, Manifest, BinPack, PackedTable key prefixes/length fields, and JNI query/schema payloads use little-endian field
serialization. PackedTable integral primary keys and arithmetic secondary-index field bytes use sortable big-endian encoding so bytewise scans preserve numeric
order.

### 16.3 Checksum Policy

| Component            | Checksum                                                   |
|----------------------|------------------------------------------------------------|
| WAL segment header   | CRC32C over serialized header with `crc32c = 0`            |
| WAL entry            | CRC32C over entry header with `crc32c = 0`, key, and value |
| Blob header          | CRC32C over header with `header_crc32c = 0`                |
| Blob content         | CRC32C over original uncompressed content                  |
| SST header           | CRC32C over `SSTFileHeaderV2` with `crc32c = 0`            |
| SST block            | CRC32C over payload and offsets                            |
| Manifest file header | CRC32C over header with `crc32c = 0`                       |
| Manifest record      | CRC32C over payload                                        |

CRC32C uses the Castagnoli polynomial with hardware dispatch where available.

---

## 17. Threading & Concurrency Model

### 17.1 Thread-Safe Public Components

| Component        | Guarantee                                               |
|------------------|---------------------------------------------------------|
| `AkkEngine`      | Public methods are thread-safe                          |
| `MemTable`       | Public methods are thread-safe                          |
| `WalWriter`      | Append/sync/close paths are thread-safe                 |
| `BlobManager`    | Read/write/delete scheduling are thread-safe            |
| `Manifest`       | Public methods are thread-safe                          |
| `VersionLog`     | Public methods are thread-safe                          |
| `ClusterRuntime` | Start/close/ship paths serialize active endpoint access |

### 17.2 Write Serialization

`AkkEngine` uses `Impl::write_mu` to serialize top-level put/remove and replica-apply mutation paths. This ensures sequence assignment, WAL append, version-log
append, MemTable mutation, blob externalization, and replication shipping see a consistent write order.

### 17.3 Background Work

Depending on enabled components and sync modes, background work may include:

- WAL async flusher threads
- MemTable shard flushing
- SST compaction workers
- Manifest fast-mode flusher
- VersionLog async/batched flusher
- Blob cleanup
- API server accept/connection handling
- Cluster manager and replication endpoints

---

## 18. Error Handling & Recovery

### 18.1 Exception Policy

Unrecoverable I/O, corrupt-file, invalid-configuration, and closed-engine cases throw standard exceptions, typically `std::runtime_error` or
`std::invalid_argument`. Expected absence is represented with `std::optional` or `bool`.

This is the Native API contract:

| Case | API behavior |
|------|--------------|
| Missing key or missing historical value | `get`, `getAt`, and typed `PackedTable::get` return `std::nullopt`; `getInto`, `getIntoArena`, and typed `getInto` return `false`. |
| Empty result set | `scan`, `scanAll`, index ranges, `history`, `query().toVector()`, and joins return an empty iterator/vector. |
| Closed or moved-from engine handle | Public `AkkEngine` operations throw `std::runtime_error`. |
| Invalid configuration, unsupported backend, invalid cluster topology, unsafe plain transport, malformed primary key | Throws `std::invalid_argument` or `std::runtime_error` at the failing boundary. |
| Storage I/O failure, corrupt SST/blob/manifest/version-log data, CRC mismatch | Throws `std::runtime_error` instead of returning a negative result. |
| Version-log mutation while the version log is disabled | `rollbackTo` and `rollbackKey` throw `std::runtime_error`; `getAt` returns `std::nullopt` when no version-log reader is available. |
| Typed table `findBy` on an unregistered secondary index | Throws `std::runtime_error`. Register the index with `index<&T::field>()` or `indexed<&T::field>()` before using `findBy`. |
| Calling `next()` after `hasNext()` is false | Typed scan, query, and index ranges throw `std::out_of_range`. |
| Detached `Ref<T>` dereference or missing referenced entity | Throws `std::runtime_error`. Foreign-key writes also throw when the registered referenced entity is absent. |
| Truncated BinPack/wire payload | Low-level read helpers throw `std::runtime_error`; boolean decode wrappers only return `false` for decode paths that catch and classify malformed input. |

`core::Status` is an internal lightweight status value for subsystems that keep a non-throwing hot path. It does not make the public Native API zero-exception.

### 18.2 Startup Recovery

On `AkkEngine::open`:

1. Missing component paths are derived from `dataDir`.
2. Required directories are created.
3. Node id is loaded or generated.
4. Manifest is opened and replay-capable.
5. SST manager is created and recovers SST state when enabled.
6. WAL recovery replays valid records into MemTable when enabled.
7. WAL writer, BlobManager, VersionLog, ClusterRuntime, and API server are started as configured.

### 18.3 Close Order

`AkkEngine::close` is idempotent and closes in this order:

1. API server.
2. Cluster runtime.
3. MemTable force flush when configured.
4. SST manager shutdown.
5. WAL force sync and close when configured.
6. Manifest close.
7. Blob manager close.
8. VersionLog close.
9. MemTable release.

### 18.4 Corruption Handling

- WAL recovery accepts valid entries and stops or skips corrupted trailing data according to recovery logic.
- SST reader validates headers, footers, block metadata, and block CRC before trusting records.
- Blob reads validate header and content CRC.
- Manifest replay ignores malformed or CRC-invalid records rather than applying partial state transitions.

---

## 19. Build & Integration

### 19.1 Requirements

| Item      | Requirement                                   |
|-----------|-----------------------------------------------|
| C++       | C++23                                         |
| CMake     | 4.1 or newer                                  |
| Windows   | MSVC with initialized Visual Studio toolchain |
| Linux     | GCC/Clang with C++23 support                  |
| Zstd      | Fetched by CMake, v1.5.6                      |
| Boost.PFR | Fetched by CMake, boost-1.84.0                |
| mbedTLS   | Fetched by CMake, v3.6.2                      |

### 19.2 CMake Options

| Option                 | Default | Description                                   |
|------------------------|---------|-----------------------------------------------|
| `BUILD_SHARED_LIBS`    | `ON`    | Build shared library                          |
| `AKKARADB_BUILD_TESTS` | `OFF`   | Build unit tests if test directory is present |
| `AKKARADB_BUILD_JNI`   | `OFF`   | Build JNI bridge target `akkaradb_jni`        |

SIMD flags are currently always added by the top-level CMake file (`/arch:AVX2` on MSVC, `-msse4.2 -mavx2` otherwise). TLS is also currently always built and
linked.

### 19.3 Build

```cmake
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

JNI bridge:

```cmake
cmake -B build-jni -DCMAKE_BUILD_TYPE=Release -DAKKARADB_BUILD_JNI=ON
cmake --build build-jni --config Release --target akkaradb_jni
```

### 19.4 Linking

Installed CMake package exports the target as `AkkaraDB::akkaradb`.

```cmake
find_package(AkkaraDB CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE AkkaraDB::akkaradb)
```

When building from the source tree, the primary target is `akkaradb`.

### 19.5 Public Headers

| Header                             | Contents                                                |
|------------------------------------|---------------------------------------------------------|
| `akkaradb/AkkaraDB.hpp`            | High-level open API, `StartupMode`, `AkkaraDB::Options` |
| `akkaradb/PackedTable.hpp`         | Typed table, secondary indexes, query helpers           |
| `akkaradb/Ref.hpp`                 | `Ref<T>`, `RefTraits`, `RowId`, `Immutable<T>`, `Const` |
| `akkaradb/Stats.hpp`               | Engine statistics snapshot                              |
| `akkaradb/binpack/BinPack.hpp`     | Encode/decode facade                                    |
| `akkaradb/binpack/TypeAdapter.hpp` | Serialization adapters                                  |

Headers under `akkara/akkengine/include/akk` provide the public low-level AkkEngine API, including `akk/engine/erasure/ErasureCodec.hpp` and
`akk/engine/erasure/ErasureCodecExt.hpp`. The higher-level typed database API remains under `akkara/akkaradb/include/akkaradb`.

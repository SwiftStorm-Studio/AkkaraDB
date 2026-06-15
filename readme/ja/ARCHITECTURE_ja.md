# AkkaraDB Native Architecture

この文書は、native C++23 版 AkkaraDB engine の内部構造を説明します。正確な binary layout、magic number、CRC 範囲、protocol frame、設定 field、互換性規則は `SPEC.md` を source of truth とし、この文書では subsystem の責務と data flow を中心に扱います。

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
  - [Write Path](#write-path)
  - [Read Path](#read-path)
  - [Range Scan Path](#range-scan-path)
  - [Typed Table Path](#typed-table-path)
  - [JNI Query Scan Path](#jni-query-scan-path)
- [Recovery and Shutdown](#recovery-and-shutdown)
  - [Startup Recovery](#startup-recovery)
  - [Crash Scenarios](#crash-scenarios)
  - [Shutdown](#shutdown)
- [Concurrency Model](#concurrency-model)
- [Startup Modes](#startup-modes)
- [Relationship to SPEC.md](#relationship-to-specmd)

---

## Overview

AkkaraDB native は、binary-safe な key/value を LSM-tree 型の構造で保存する C++23 storage engine です。write はまず MemTable に入り、durability が有効な構成では WAL segment に追記されます。MemTable が threshold に到達すると immutable な SST file へ flush され、SST は level 間で compaction されます。

native engine は最小限の LSM 実装だけではなく、次の subsystem を含みます。

- Blob Manager による大きな value の外部化
- Version Log による per-key history、point-in-time read、rollback
- BinPack serialization と secondary index を使う typed C++ table API
- Kotlin/JVM layer から使う JNI entry point
- 組み込み HTTP、binary TCP、optional gRPC API server
- standalone、mirror、stripe deployment 向けの cluster / replication primitive
- mbedTLS と native secure channel を使う transport protection

全体像は次の通りです。

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
|  | - HTTP/TCP/gRPC   |      | - primary/replica            |  |
|  | - AK5 protocol    |      | - mirror/stripe routing      |  |
|  +-------------------+      +------------------------------+  |
+---------------------------------------------------------------+
```

---

## Storage Layout

`paths.dataDir` が設定されている場合、不足している component path はその directory から導出されます。

```text
{dataDir}/
|-- wal/             WAL segment files
|-- sstable/         SST levels
|   |-- L0/
|   |-- L1/
|   |-- ...
|   `-- L6/
|-- blobs/           externalized value payloads
|-- manifest.akmf    SST lifecycle and checkpoint metadata
|-- history.akvlog   version history when enabled
|-- cluster.akcc     cluster topology when enabled
`-- node.id          persistent node identity
```

各 persistent subsystem の責務は分かれています。

| Component | Role |
|---|---|
| WAL | crash 後に acknowledged / pending write を replay する |
| SST | immutable sorted key/value record を保存する |
| Manifest | live SST file と compaction transition を追跡する |
| Blob files | 大きな value を MemTable / WAL / SST payload の外側に保存する |
| Version Log | point-in-time read と rollback のための履歴を保存する |
| Cluster config | durable な cluster topology を保存する。runtime TLS path は含めない |

---

## Core Components

### Public API Layer

native repository は主に 2 つの API layer を公開します。

`engine::AkkEngine` は byte-oriented な core API です。key/value は raw byte span として扱われ、point read、write、batch write/read、remove、scan、history、rollback、flush、sync、close を提供します。

`AkkaraDB` と `PackedTable<&T::id>` は high-level typed API です。C++ aggregate entity を BinPack で raw key/value row に変換します。table key は 8-byte FNV-1a table prefix で namespace 化され、secondary index は `table_name + ":idx:" + field_name` から導出した別 prefix を使います。

high-level API は primary-key encoding、table scope、secondary-index maintenance、query planning、`Ref<T>` の lazy resolve、foreign-key validation、cascade delete、join を API 側で扱います。

JVM layer は `AKKARADB_BUILD_JNI=ON` のとき、JNI bridge 経由で同じ native engine にアクセスします。JNI bridge は raw operation、scan cursor、query scan payload evaluation、option-based open、rollback entry point を Kotlin module に提供します。

Public API の error boundary は `SPEC.md` と同じです。通常の「存在しない」は `std::optional`、`bool`、空の range で表し、呼び出しの誤りや storage operation の失敗は例外として表します。missing key は例外ではありませんが、closed engine、不正な option、typed API の foreign-key target 不在、未登録 `findBy()` index、永続化データ破損、I/O failure は標準例外として surface します。API server は protocol boundary で例外を捕捉し、client には error response または status として返します。

### AkkEngine

`AkkEngine` は storage component の coordinator です。主な責務は次の通りです。

- `dataDir` から component path を導出する
- option に従って WAL、SST、Blob、Manifest、VersionLog、API、Cluster component を作成または open する
- top-level write の sequence assignment を serialize する
- 大きな value を Blob Manager に外部化する
- WAL append、VersionLog append、MemTable mutation、Cluster shipping を一貫した順序で実行する
- MemTable record を SST へ flush する
- read を MemTable、SST、BlobManager に route する
- primary node として動く場合に write / blob を cluster runtime へ ship する
- component を決まった順序で close する

### Record and Key Model

raw engine は key を bytewise lexicographic comparison で順序付けます。上位 layer は、必要な探索順序に合うよう key を encode します。

MemTable 上の record は `OwnedRecord` で表されます。`OwnedRecord` は `MemHdr16`、64-bit key fingerprint、first up to 8 bytes の `miniKey`、inline または arena-backed の `[key][value]` payload を持つ compact な record object です。

SST 上の record は `SSTHdr32` に key bytes と value bytes が続く形式です。`keyFp64` と `miniKey` は fast reject / in-block search の hint で、正しさの基準は常に full key comparison です。

value が Blob Manager に外部化される場合、record value は 20-byte の `BlobRef` になり、blob flag が立ちます。read path は `BlobRef` を間接参照として扱い、blob header と content CRC を検証してから original value bytes を返します。

`PackedTable` の primary row key は `[table_prefix:8][encoded_pk]` です。integral primary key は fixed little-endian、non-integral primary key は BinPack で encode されます。secondary index key は `[index_prefix:8][field_len:u32le][encoded_field][encoded_pk]` です。indexed field は query range に使えるよう、整数と浮動小数では sortable big-endian encoding、それ以外では BinPack encoding を使います。

### Memory Management

native engine は hot path の allocation と ownership を明確にするため、用途別の buffer abstraction を使います。

`BufferArena` は scan や API call の一時 allocation を所有します。`AkkEngine::scan` が返す key/value span は、この arena lifetime 中だけ有効です。

`OwnedBuffer` は I/O 向けの aligned owning storage です。`BufferView` は対応する non-owning view で、必要に応じて owning buffer へ deep copy できます。

`SmallBuffer` は `OwnedRecord` に埋め込まれます。短い `[key][value]` payload は inline に保持され、大きな payload は arena-backed storage へ移されます。

### MemTable

MemTable は最初の read/write target です。contention を下げるため sharding されており、`RuntimeOptions::writerThreads` が設定される場合は MemTable と WAL の shard count が writer count から導出されます。

sequence model は次の操作を提供します。

```text
reserve_seq(1) -> globally monotonic seq
last_seq()     -> read snapshot for point reads and scans
replay(seq)    -> advances sequence state during recovery
```

lookup は指定 snapshot で見える record を返します。tombstone は MemTable 内で隠さず caller に返します。これは、古い SST value を抑止する必要があるためです。

flush lifecycle は次の流れです。

```text
shard crosses thresholdBytesPerShard
  |
  +-- shard can be sealed
  +-- engine on_flush callback receives sorted RecordView span
  +-- SSTManager::flush writes a new SST
  +-- Manifest checkpoint records the resulting state
  +-- WAL segments are pruned up to checkpoint when configured
```

### Write-Ahead Log

WAL は crash recovery のために mutation record を append-only で保存します。native v5 では `{dataDir}/wal` 以下に segmented WAL を持ち、各 segment は CRC-protected な `WalSegmentHeader` から始まります。

entry は次の形式で serialize されます。

```text
[WalEntryHeader:32][key bytes][value bytes]
```

entry header には record sequence、key fingerprint、entry 全体の length、value length、key length、flags、header-with-zero-crc + key + value に対する CRC32C が含まれます。startup recovery では segment header と entry CRC を検証してから fresh MemTable に entry を適用します。

WAL sync mode は `SYNC`、`ASYNC`、`OFF` です。high-level `StartupMode::FAST` と `NORMAL` は async、`DURABLE` は sync、`ULTRA_FAST` は WAL disabled になります。

### Blob Manager

Blob Manager は `blob.thresholdBytes` 以上の value を外部化します。default は 16 KiB です。これにより MemTable record、WAL entry、SST block を小さく保ちつつ、通常の `get` API では完全な value を返します。

```text
value size >= threshold
  |
  +-- BlobManager writes the payload to {dataDir}/blobs
  +-- optional Zstd compression is applied
  +-- Blob header and content CRC are stored
  +-- record value becomes BlobRef(blob_id, total_size, content_crc32c)
  +-- record flag includes blob
```

read path では `BlobRef` を parse し、blob file を読み、header CRC と content CRC を検証してから original value bytes を返します。

### SST Manager

SST Manager は immutable sorted file と compaction を担当します。default では `L0..L6` の 7 levels を管理します。L0 は overlapping file を持つ可能性があり、上位 level は compaction 後に non-overlapping になることが期待されます。

SST v2 file layout は次の通りです。

```text
[SSTFileHeaderV2:256]
[data blocks...]
[SSTBlockIndexEntryV2 array]
[key arena]
[SSTBloomHeaderV2][bloom bits]
[SSTFooterV2:48]
```

lookup は file min/max key check、Bloom filter negative check、block index binary search、in-block binary search の順で絞り込みます。data block は raw concatenated SST record または Zstd-compressed bytes を持ちます。

flush は sorted MemTable record から新しい SST を作ります。compaction は overlapping または budget 超過の SST set を lower level へ rewrite し、その transition を Manifest に記録します。`CompactionCommit` は input/output file の変更を replay 時に atomically 適用できるため、preferred な manifest event です。

### Manifest

Manifest は append-only で CRC-protected な storage lifecycle log です。live SST file、compaction transition、checkpoint、cluster metadata event を追跡します。

```text
[ManifestFileHeader:32][ManifestRecordHeader:8][payload]...
```

主な record type は次の通りです。

| Event | Purpose |
|---|---|
| `SSTSeal` | 新しい SST file が seal された |
| `Checkpoint` | named または unnamed checkpoint |
| `CompactionStart` | informational start marker |
| `CompactionCommit` | compaction の input/output transition |
| `Truncate` | informational truncation marker |
| `NodeJoin`, `NodeLeave`, `PrimaryLease` | cluster metadata |

Manifest replay は in-memory の SST lifecycle state を再構築します。malformed record や CRC-invalid record は適用しません。

### Version Log

Version Log は有効な場合に per-key history を記録します。次の機能の土台になります。

- `getAt(key, seq)`
- `history(key)`
- `rollbackTo(seq)`
- `rollbackKey(key, seq)`

各 version entry は sequence、source node id、timestamp、flags、value bytes を保存します。rollback によって生成される record は reserved rollback node id と rollback flag を使い、通常 write と区別できます。

Version history は `FAST` / `NORMAL` では default disabled、`DURABLE` では default enabled です。`AkkaraDB::Options::Overrides::versionLogEnabled` で変更できます。

### API Servers

`components.apiEnabled` が設定されている場合、engine は embedded API backend を起動できます。`api.backends` が空なら HTTP と TCP が起動対象です。GRPC は backend enum と transport factory を持ち、gRPC module が登録されている build で有効化できます。

| Backend | Default port | Purpose |
|---|---:|---|
| HTTP | 7070 | REST-style key/value operation |
| TCP | 7071 | binary AK5 request/response protocol |
| GRPC | 7072 | protobuf/gRPC service |

binary protocol は `AK5Q` request frame と `AK5S` response frame を使います。current opcode には `Get`、`Put`、`Remove`、`GetAt`、`BatchPut`、`BatchGet`、`Ping`、`Exists`、`Count`、`Scan`、`History`、`RollbackTo`、`RollbackKey`、`ForceSync`、`ForceFlush`、`Stats` があります。

HTTP API は `/v1/ping`、`/v1/put`、`/v1/get`、`/v1/remove`、`/v1/exists`、`/v1/count`、`/v1/scan`、`/v1/getAt`、`/v1/history`、`/v1/rollbackTo`、`/v1/rollbackKey`、`/v1/batchPut`、`/v1/batchGet`、`/v1/forceSync`、`/v1/forceFlush`、`/v1/stats` を公開します。

### Cluster Runtime and TLS

cluster layer は `Standalone`、`Mirror`、`Stripe` の deployment mode を持ちます。

| Mode | Meaning |
|---|---|
| `Standalone` | local-only operation |
| `Mirror` | write を全 data-bearing node に mirror する |
| `Stripe` | key を router policy で data node に割り当てる |

Stripe runtime creation は受理されます。router は deterministic rendezvous hash によって key ごとの owner data-bearing node を 1 つ選び、選択結果は `ClusterRuntime::router()` から参照できます。node set や placement policy の変更に伴う ownership migration は、明示的な運用手順として扱います。

node role は `Standalone`、`Primary`、`Replica` です。Primary は write を受け取り、record と blob を replica に ship します。Replica は engine から渡された callback を使って replicated record / blob を適用します。acknowledgement policy は `Async`、`All`、`Quorum` です。

replication link は TCP です。`TransportMode::SECURE` は native secure channel で TCP stream を保護し、`TransportMode::PLAIN` は node host が loopback または LAN/private address の場合だけ許可されます。`localhost` 以外の hostname は config validation では non-private と扱われます。

Primary selection は coordinator-eligible な最小 `node_id` による deterministic selection です。これは shared filesystem state なしで LAN/WAN node 間に同じ primary view を作りますが、quorum consensus ではなく split-brain-safe な automatic failover はこの layer の対象外です。

TLS support は mbedTLS によって current native target に組み込まれます。API server と replication link は runtime option に応じて TLS / secure transport または plain transport を使います。

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

sequence assignment、WAL append、version-log append、MemTable mutation、blob externalization、replication shipping は engine write mutex の下で順序付けられます。これにより public method が並行に呼ばれても mutation order が一貫します。

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
        +-- optional sstPromoteReads -> insert SST hit into MemTable
```

MemTable は current snapshot の最初の authority です。SST は flush 済み data の fallback です。Blob dereference は winning record が選ばれた後に行われます。

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

返される view は arena lifetime に紐づきます。これにより scanned key/value を毎回 independent heap object に copy せずに済みます。

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

secondary index entry は non-unique です。encoded primary key suffix によって同じ indexed field value を持つ row を区別し、index scan から primary row を回収できます。

`Ref<T>` field がある場合、table は attached binding を使って dirty reference を先に保存し、foreign key が登録されていれば参照先の存在を検証します。

### JNI Query Scan Path

```text
JVM scanQuery(start, end, queryBytes, schemaBytes)
  |
  +-- JNI opens native scan cursor over [start, end)
  +-- native side decodes schema payload
  +-- native side decodes query expression payload
  +-- each row value is decoded enough to evaluate predicates
  +-- matching rows are returned to JVM RowView(ByteBuffer key, ByteBuffer value)
```

query と schema payload の整数は little-endian です。unsupported operator は、黙って match させるのではなく query-evaluation error として扱います。

---

## Recovery and Shutdown

### Startup Recovery

`AkkEngine::open` は決まった順序で startup します。

```text
1. Derive missing component paths from dataDir
2. Create required directories
3. Load or generate persistent node id
4. Open Manifest and prepare replay-capable state
5. Create SST Manager and recover SST state when enabled
6. Replay WAL into a fresh MemTable when enabled
7. Start WAL writer, BlobManager, VersionLog, ClusterRuntime, and API server as configured
```

recovery policy は component ごとに局所化されています。WAL recovery は segment header と entry CRC を検証し、SST reader は header、footer、block metadata、block CRC を検証します。Blob read は header CRC と original content CRC を検証し、Manifest replay は malformed / CRC-invalid record を適用しません。

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

`AkkEngine::close` は idempotent です。component は次の順序で close されます。

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

この順序により、まず外部 traffic を止め、その後 local mutable state を drain し、最後に persistence component を閉じます。

---

## Concurrency Model

public storage component は public method boundary で thread-safe に扱えるよう設計されています。

| Component | Guarantee |
|---|---|
| `AkkEngine` | public method は thread-safe |
| `MemTable` | public method は thread-safe |
| `WalWriter` | append / sync / close path は thread-safe |
| `BlobManager` | read / write / delete scheduling は thread-safe |
| `Manifest` | public method は thread-safe |
| `VersionLog` | public method は thread-safe |
| `ClusterRuntime` | start / close / ship path は endpoint access を serialize する |

top-level write は `AkkEngine::Impl::write_mu` によって serialize されます。sequence assignment と write に伴う side effect を一貫した順序で扱うためです。

background work には WAL async flusher、MemTable shard flushing、SST compaction worker、Manifest fast-mode flusher、VersionLog async flusher、Blob cleanup、API server accept / connection handling、Cluster manager / replication endpoint が含まれます。

---

## Startup Modes

high-level `AkkaraDB::open` は `StartupMode` preset を `AkkEngineOptions` に変換します。

| Mode | WAL | Blob | Manifest | SST | VersionLog | Close behavior | MemTable threshold |
|---|---|---|---|---|---|---|---|
| `ULTRA_FAST` | disabled | disabled | disabled | disabled | disabled | no force flush/sync | 512 MiB/shard |
| `FAST` | async | enabled | enabled | enabled | disabled | force flush/sync | 256 MiB/shard |
| `NORMAL` | async | enabled | enabled | enabled | disabled | force flush/sync | 64 MiB/shard |
| `DURABLE` | sync | enabled | enabled | enabled | enabled | force flush/sync | 64 MiB/shard |

`FAST` は SST read promotion も有効化します。fine-grained override によって MemTable threshold、VersionLog、SST / blob codec、blob threshold、Bloom filter density、L0 compaction trigger、SST read promotion を変更できます。

---

## Relationship to SPEC.md

この architecture document は `SPEC.md` を置き換えるものではありません。component の責務、data flow、recovery / shutdown の意図を理解するためにこの文書を使い、正確な binary layout、magic number、CRC range、protocol frame、configuration field、compatibility rule は `SPEC.md` を参照します。

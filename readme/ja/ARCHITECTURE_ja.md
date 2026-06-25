# AkkaraDB Native アーキテクチャ

この文書は、AkkaraDB の native C++23 engine の全体像を説明します。v5 technical specification の補助資料であり、`SPEC.md` が正確な format と compatibility rule を定義する source of truth であるのに対して、ここでは各 subsystem の責務と data flow を説明します。

## 目次

- [概要](#概要)
- [ストレージ構成](#ストレージ構成)
- [主要コンポーネント](#主要コンポーネント)
  - [公開 API 層](#公開-api-層)
  - [AkkEngine](#akkengine)
  - [レコードとキーのモデル](#レコードとキーのモデル)
  - [メモリ管理](#メモリ管理)
  - [MemTable](#memtable)
  - [書き込み先行ログ](#書き込み先行ログ)
  - [Blob マネージャ](#blob-マネージャ)
  - [SST マネージャ](#sst-マネージャ)
  - [マニフェスト](#マニフェスト)
  - [バージョンログ](#バージョンログ)
  - [API サーバー](#api-サーバー)
  - [Cluster Runtime と TLS](#cluster-runtime-と-tls)
- [データフロー](#データフロー)
- [リカバリとシャットダウン](#リカバリとシャットダウン)
- [並行性モデル](#並行性モデル)
- [起動モード](#起動モード)

---

## 概要

AkkaraDB native は、AkkaraDB における canonical な C++23 storage engine です。binary-safe な key/value を LSM-tree で保存し、write はまず in-memory の MemTable に入り、durable 構成では WAL segment に追記され、flush によって immutable な SST file が作られ、level 間で compaction されます。

native engine には、最小構成の LSM 実装を超える subsystem も含まれます。

- Blob Manager による large value の外部化
- Version Log による任意の per-key history
- BinPack serialization と secondary index key を使う typed C++ table
- Kotlin/JVM layer が使う JNI entry point
- 組み込みの HTTP / binary TCP API server
- standalone、mirror、stripe deployment 向けの cluster / replication primitive
- mbedTLS による TLS transport support

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
|  | - HTTP/TCP        |      | - primary/replica            |  |
|  | - AK5 protocol    |      | - mirror/stripe routing      |  |
|  +-------------------+      +------------------------------+  |
+---------------------------------------------------------------+
```

---

## ストレージ構成

`paths.dataDir` が設定されている場合、未設定の component path はそこから自動導出されます。

```text
{dataDir}/
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
|-- cluster.akmf     Node-local cluster runtime event log when enabled
`-- node.id          Persistent node identity
```

正確な binary format は `SPEC.md` にあります。アーキテクチャ上重要なのは、各 persistent subsystem の責務が分離されていることです。

| Component | 役割 |
|---|---|
| WAL | crash 後に acknowledged / pending write を replay する |
| SST | immutable な sorted key/value record を保持する |
| Manifest | SST lifecycle、checkpoint、cluster runtime event を追跡する |
| Blob files | MemTable / WAL / SST payload の外に大きな値を保存する |
| Version Log | point-in-time read と rollback 用の履歴を保存する |
| Cluster config | runtime TLS path ではなく durable な cluster topology を保存する |

---

## 主要コンポーネント

### 公開 API 層

native repository には 2 段階の API があります。

`engine::AkkEngine` は byte 指向の core API です。raw byte span の key/value を受け取り、point read、write、remove、scan、history、rollback、flush、sync、close を提供します。

`AkkaraDB` と `PackedTable<&T::id>` は high-level typed API です。C++ aggregate entity を BinPack で raw key/value row に変換します。table key は 8-byte FNV-1a table prefix で namespace 分離され、secondary index は `table_name + ":idx:" + field_name` から導いた別 prefix を使います。各 row には primary key とは独立した stable `RowId` も割り当てられます。

high-level API は `Ref<T>` の lazy resolve、foreign-key validation、`OnDelete` / `OnUpdate` action、`Immutable<T>` / `Const<T>` による immutable field、stable row-id lookup、`onUpdate<&Field>(...)` hook も担当します。

low-level public header には、storage engine の open/close path と独立して `XorErasureCodec`、`DualXorErasureCodec`、`RsErasureCodec`、`ErsCodec` などの erasure coding utility も含まれます。

`AKKARADB_BUILD_JNI=ON` の場合、JVM layer は JNI を通じて同じ native engine に接続します。JNI bridge は raw operation、scan cursor、query scan payload evaluation、option-based open、rollback entry point を Kotlin module に公開します。

API error の境界は `SPEC.md` と揃っています。期待される「存在しない」は `std::optional`、`bool`、空の range で表し、不正な使い方や storage operation failure は exception を投げます。missing key 自体は例外ではありませんが、closed engine、invalid option、typed foreign-key target の欠落、未登録 index に対する `findBy()`、persisted data の破損、I/O failure などは standard exception として surface します。API server は protocol boundary でそれらを catch し、client には error response / status として返します。

### AkkEngine 本体

`AkkEngine` は coordinator です。storage component を所有または接続し、system 全体に thread-safe な read/write surface を与えます。

主な責務:

- `dataDir` から component path を導出する
- option に応じて WAL、SST、Blob、Manifest、VersionLog、API、cluster component を create/open する
- top-level write の sequence assignment を serialize する
- record を書く前に large value を外部化する
- 有効な場合は MemTable mutation の前に WAL と VersionLog に append する
- MemTable record を SST に flush する
- read を MemTable、SST、BlobManager に route する
- primary として動作している場合は cluster runtime へ write/blob を ship する
- component を制御された順序で close する

### レコードとキーのモデル

raw engine は key を bytewise lexicographic order で比較します。必要な順序性を得るための key encoding は higher layer の責務です。`PackedTable` は整数 primary key に sortable な fixed-width big-endian encoding を使うので、typed primary-key scan でも数値順が保たれます。secondary index も arithmetic field には同じ sortable encoding 方針を使います。

in-memory では `OwnedRecord` が compact な 64-byte metadata object として record を保持します。

```text
+-------------------+------------------------------------------+
| MemHdr16          | seq, key length, value length, flags     |
| key_fp64          | 64-bit key fingerprint                   |
| mini_key          | first up to 8 key bytes, little-endian   |
| SmallBuffer       | inline or arena-backed [key][value]      |
+-------------------+------------------------------------------+
```

on-disk の SST record は 32-byte の `SSTHdr32` と、それに続く key bytes / value bytes です。`key_fp64` と `mini_key` は fast reject / in-block search 用の hint であり、最終的な正当性は full key compare が担保します。

Blob Manager に保存される value は、record value が 20-byte の `BlobRef` に置き換わり、blob flag が立ちます。read path ではそれを indirection として扱い、blob header と content CRC を検証した上で元の value を返します。

### メモリ管理

native engine は hot path の挙動を予測しやすくするため、小さく明示的な buffer abstraction を使います。

`BufferArena` は scan や API call の一時 allocation を所有します。`AkkEngine::scan` が返す key/value span は arena の寿命に依存します。

`OwnedBuffer` は I/O 向けの aligned owning storage で、`BufferView` は対応する non-owning view です。

`SmallBuffer` は `OwnedRecord` に埋め込まれています。短い `[key][value]` payload は inline に収まり、大きい payload だけが arena-backed storage に逃がされます。これにより common case を compact に保てます。

### MemTable

MemTable は最初の read/write target です。contention を下げるため sharding されており、`RuntimeOptions::writer_threads` が設定されている場合は MemTable と WAL の shard count を writer count から導出できます。

write sequence は MemTable の sequence model を通じて割り当てられます。

```text
reserve_seq(1) -> globally monotonic seq
last_seq()     -> read snapshot for point reads and scans
replay(seq)    -> advances sequence state during recovery
```

lookup は指定 snapshot で可視な record を返します。tombstone は MemTable 内で隠蔽せず caller に返されます。これは古い SST value を suppress する必要があるためです。

flush lifecycle:

```text
shard crosses thresholdBytesPerShard
  |
  +-- shard can be sealed
  +-- engine on_flush callback receives sorted RecordView span
  +-- SSTManager::flush writes a new SST
  +-- Manifest checkpoint records the resulting state
  +-- WAL segments are pruned up to checkpoint when configured
```

### 書き込み先行ログ

WAL は crash recovery のための append-only mutation log です。native v5 では `{dataDir}/wal` 以下に segmented WAL を持ち、各 segment は CRC 保護された `WalSegmentHeader` で始まります。

entry の format:

```text
[WalEntryHeader:32][key bytes][value bytes]
```

entry header には record sequence、key fingerprint、entry length、value length、key length、flags、そして header-with-zero-crc + key + value に対する CRC32C が含まれます。

WAL の sync mode:

| モード | 意味 |
|---|---|
| `Sync` | 同期的な durability path |
| `Async` | grouped background flush path |
| `Off` | 明示 sync なし。cache / test 向け |

startup recovery では segment header と entry CRC を検証してから fresh MemTable に適用します。

### Blob マネージャ

Blob Manager は `blob.thresholdBytes` 以上の value を外部化します。既定値は 16 KiB です。これにより MemTable record、WAL entry、SST block を小さく保ちながら、通常の `get` API 形状は変えずに済みます。

write path:

```text
value size >= threshold
  |
  +-- BlobManager writes the payload to {dataDir}/blobs
  +-- optional Zstd compression is applied
  +-- Blob header and content CRC are stored
  +-- record value becomes BlobRef(blob_id, total_size, content_crc32c)
  +-- record flag includes blob
```

read path:

```text
record flag includes blob
  |
  +-- parse BlobRef
  +-- read blob file
  +-- validate header CRC and content CRC
  +-- return original value bytes
```

### SST マネージャ

SST Manager は immutable sorted file と compaction を担当します。既定では 7 level、`L0..L6` を管理します。L0 は overlapping file を持ち得ますが、高い level では compaction 後に non-overlapping になる想定です。

SST v2 file layout:

```text
[SSTFileHeaderV2:256]
[data blocks...]
[SSTBlockIndexEntryV2 array]
[key arena]
[SSTBloomHeaderV2][bloom bits]
[SSTFooterV2:48]
```

record は block 内で key order を保ちます。key は optional な block compression の前に previous key に対する prefix-delta encoding になっていることがあり、reader は block load 時に full key を再構成します。

lookup 前段:

```text
file min/max key check
  |
  +-- Bloom filter negative check
  +-- block index binary search
  +-- in-block binary search using SSTHdr32, mini_key, and full key compare
```

data block は raw concatenated SST record か Zstd-compressed bytes です。block index は key boundary を持ち、Bloom filter は absent key に対する disk / cache work を減らします。

flush は sorted MemTable record から新しい SST を作ります。compaction は overlapping もしくは budget 超過の SST set を lower level に rewrite し、その transition を Manifest に記録します。`CompactionCommit` を使うことで replay 時に input/output file change を atomic に適用できます。

### マニフェスト

Manifest は append-only かつ CRC 保護された storage lifecycle log です。main engine manifest (`manifest.akmf`) は live SST file、compaction transition、checkpoint を追跡し、別の node-local cluster manifest (`cluster.akmf`) は cluster runtime event を記録します。

format:

```text
[ManifestFileHeader:32][ManifestRecordHeader:8][payload]...
```

主な record type:

| Event | 役割 |
|---|---|
| `SSTSeal` | 新しい SST file が seal された |
| `Checkpoint` | named / unnamed checkpoint |
| `CompactionStart` | 情報用の start marker |
| `CompactionCommit` | atomic な compaction input/output transition |
| `Truncate` | 情報用の truncation marker |
| `NodeJoin`, `NodeLeave`, `PrimaryLease` | `cluster.akmf` に書かれる cluster runtime event |

`manifest.akmf` の replay で in-memory SST lifecycle state を再構築します。cluster manifest は現在、startup / shutdown や primary lease の breadcrumb を残す local durable event log として使われます。malformed または CRC-invalid な record は適用されません。

### バージョンログ

Version Log は有効時に per-key history を保存し、次を支えます。

- `getAt(key, seq)`
- `history(key)`
- `rollbackTo(seq)`
- `rollbackKey(key, seq)`

各 version entry には sequence、source node id、timestamp、flags、value bytes が入ります。rollback 由来の record は reserved rollback node id と rollback flag を使って通常 write と区別されます。

`SYNC` mode は entry を書き込んで durability sync してから return します。`BATCHED_SYNC` は in-memory history を先に更新し、background flusher に batch sync させます。`ASYNC` も background flusher を使いますが、method return 時点で batch durability は保証しません。header corruption、malformed entry、truncation、CRC mismatch は silent skip ではなく exception になります。

Version history は `FAST` / `NORMAL` では default disabled、`DURABLE` では default enabled です。

### API サーバー

`components.apiEnabled` が有効なら、engine は embedded API backend を起動できます。

対応 backend:

| Backend | Default port | 用途 |
|---|---:|---|
| HTTP | 7070 | REST 風の key/value operation |
| TCP | 7071 | binary AK5 request/response protocol |
| GRPC | 7072 | gRPC module が登録されている場合の Protobuf/gRPC service |

binary protocol は `AK5Q` request frame と `AK5S` response frame を使います。現在の opcode には `Get`、`Put`、`Remove`、`GetAt`、`BatchPut`、`BatchGet`、`Ping`、`Exists`、`Count`、`Scan`、`History`、`RollbackTo`、`RollbackKey`、`ForceSync`、`ForceFlush`、`Stats` があります。

HTTP API は `/v1/ping`、`/v1/put`、`/v1/get`、`/v1/remove`、`/v1/exists`、`/v1/count`、`/v1/scan`、`/v1/getAt`、`/v1/history`、`/v1/rollbackTo`、`/v1/rollbackKey`、`/v1/batchPut`、`/v1/batchGet`、`/v1/forceSync`、`/v1/forceFlush`、`/v1/stats` を提供します。

### クラスタ実行系と TLS

cluster layer は 3 種類の deployment mode を持ちます。

| モード | 意味 |
|---|---|
| `Standalone` | local-only operation |
| `Mirror` | 全 data-bearing node へ write を mirror する |
| `Stripe` | router policy により key を data node に割り当てる |

Stripe runtime 自体は生成できます。router は deterministic rendezvous hashing で key ごとに 1 つの data-bearing owner を選び、選択結果は `ClusterRuntime::router()` から取得できます。node set や placement policy 変更後の ownership migration は、まだ自動ではなく明示的な運用手順として扱う前提です。

node role は `Standalone`、`Primary`、`Replica` です。primary は write を受け付けて replica へ record / blob を ship し、replica は engine から渡された callback で replicated record / blob を適用します。

`ReplicationMode` と runtime role は別概念です。`Standalone`、`Mirror`、`Stripe` は topology を、`Primary` と `Replica` は process の起動形態を表します。`Standalone` mode では明示 startup role は不要です。`Mirror` と `Stripe` では `ClusterRuntimeOptions::startupRole` を `PRIMARY` か `REPLICA` に設定する必要があり、`AUTO` は runtime error になります。

ack policy は `Async`、`All`、`Quorum` です。

cluster config の `NodeInfo.host` は peer が dial する advertise address です。primary の replication listener は runtime-only な `repl_bind_host` に bind し、既定は `0.0.0.0` です。replication link は TCP を使い、`TransportMode::SECURE` では native secure channel で包み、`TransportMode::PLAIN` は全 node host が loopback または private/LAN address の場合のみ許可されます。`localhost` 以外の hostname は config validation 時には non-private とみなします。

現時点の runtime には自動 primary election はありません。`Mirror` / `Stripe` の startup 要件:

- `PRIMARY` は local `selfNodeId` が cluster config に存在し、coordinator-eligible であること
- `REPLICA` は primary node id と到達可能な primary host / replication port を持つこと
- `primaryHost` と `primaryReplPort` は `primaryNodeId` に対応する config entry から補完可能
- `REPLICA` は自分自身を primary として指定できない

満たさなければ startup は fail fast します。split-brain-safe failover、quorum leader election、自動 primary re-selection は現在 scope 外です。

`PRIMARY` として動作する場合、runtime は local node の replication port で `ReplicationServer` を開きます。`REPLICA` の場合は `ReplicationClient` を開き、configured primary に dial します。replication ingress は primary 中心で、replica は consumer であり direct external write endpoint ではありません。

`ClusterManager` は `{dataDir}` 以下に node-local な `cluster.akmf` も書きます。successful startup 後に `NodeJoin`、clean shutdown 時に `NodeLeave`、`PRIMARY` として起動したときに `PrimaryLease` を記録します。

TLS support は current native target では mbedTLS でコンパイルされています。replication link は native secure channel (`TransportMode::SECURE`) または plain TCP (`TransportMode::PLAIN`) を使います。

---

## データフロー

### 書き込みパス

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

sequence assignment、WAL append、version-log append、MemTable mutation、blob externalization、replication shipping は engine write mutex の下で順序付けられます。これにより、public method が並行に呼ばれても mutation order が一貫します。

### 読み取りパス

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

現在の snapshot については MemTable が常に最初の authority です。SST は flush 済みデータの fallback で、Blob dereference は勝った record が決まってから行います。

### 範囲走査パス

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

返される view は arena の寿命に紐付きます。各 scanned key/value を独立した heap object に都度コピーしないための設計です。

### 型付きテーブルのパス

```text
PackedTable<User, id>.put(user)
  |
  +-- encode primary row key:
  |     [table_prefix:8][encoded_pk]
  |
  +-- allocate or reuse stable RowId
  +-- maintain [pk -> rowid] and [rowid -> pk] metadata
  |
  +-- BinPack::encode(user)
  +-- engine.put(primary_key, encoded_user)
  |
  +-- for each secondary index:
        [index_prefix:8][field_len:u32le][encoded_field][encoded_pk] -> empty value
```

secondary index entry は non-unique です。encoded primary key の suffix が duplicate field value を区別し、index scan から primary row を復元できるようにします。

entity に `Ref<T>` field がある場合、dirty reference は attached table binding を通じて先に書かれます。その後 foreign-key validation が target の存在を確認します。target primary key が `updatePrimaryKey(...)` で変わったときは、schema 設定に応じて `OnUpdate` action が propagate / reject / null 化を行います。watched field が変化していた場合は、保存前に local `onUpdate<&Field>(...)` hook が replacement entity を書き換えることもできます。

### JNI クエリ走査パス

```text
JVM scanQuery(start, end, queryBytes, schemaBytes)
  |
  +-- JNI opens native scan cursor over [start, end)
  +-- native side decodes schema payload
  +-- native side decodes query expression payload
  +-- each row value is decoded enough to evaluate predicates
  +-- matching rows are returned to JVM RowView(ByteBufferL key, ByteBufferL value)
```

query と schema の payload integer は little-endian です。unsupported operator は silent match ではなく query-evaluation error として扱う必要があります。

---

## リカバリとシャットダウン

### 起動時リカバリ

`AkkEngine::open` は固定順序で startup します。

```text
1. Derive missing component paths from dataDir
2. Create required directories
3. Load or generate persistent node id
4. Open Manifest and prepare replay-capable state
5. Create SST Manager and recover SST state when enabled
6. Replay WAL into a fresh MemTable when enabled
7. Start WAL writer, BlobManager, VersionLog, ClusterRuntime, and API server as configured
```

recovery policy は component ごとに分離されています。

- WAL recovery は segment header と entry CRC を検証してから entry を適用する
- SST reader は header、footer、block metadata、block CRC を検証する
- Blob read は header CRC と original content CRC の両方を検証する
- Manifest replay は malformed / CRC-invalid な record を適用しない

### 障害時シナリオ

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

### シャットダウン

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

まず外部 traffic を止め、その後 mutable state を drain し、最後に persistent component を閉じる流れです。

---

## 並行性モデル

public storage component は public method 境界で thread-safe になるよう設計されています。

| Component | 保証 |
|---|---|
| `AkkEngine` | public method は thread-safe |
| `MemTable` | public method は thread-safe |
| `WalWriter` | append、sync、close path は thread-safe |
| `BlobManager` | read、write、delete scheduling は thread-safe |
| `Manifest` | public method は thread-safe |
| `VersionLog` | public method は thread-safe |
| `ClusterRuntime` | start、close、ship path では endpoint access を serialize する |

top-level write は `AkkEngine::Impl::write_mu` で serialize されます。これは意図的な設計で、sequence assignment と write の副作用全体を一貫した順序に保つためです。

background work の例:

- WAL async flusher thread
- MemTable shard flushing
- SST compaction worker
- Manifest fast-mode flusher
- VersionLog async / batched flusher
- Blob cleanup
- API server の accept / connection handling
- Cluster manager と replication endpoint

---

## 起動モード

high-level `AkkaraDB::open` は `StartupMode` preset を `AkkEngineOptions` に変換します。

| モード | WAL | Blob | マニフェスト | SST | バージョンログ | close 時の挙動 | MemTable しきい値 |
|---|---|---|---|---|---|---|---|
| `ULTRA_FAST` | disabled | disabled | disabled | disabled | disabled | force flush/sync なし | 512 MiB/shard |
| `FAST` | async | enabled | enabled | enabled | disabled | force flush/sync あり | 256 MiB/shard |
| `NORMAL` | async | enabled | enabled | enabled | disabled | force flush/sync あり | 64 MiB/shard |
| `DURABLE` | sync | enabled | enabled | enabled | enabled | force flush/sync あり | 64 MiB/shard |

`FAST` では SST read promotion も有効です。細かい override では MemTable しきい値、VersionLog、SST / blob codec、blob threshold、Bloom filter density、L0 compaction trigger、SST read promotion を変更できます。

---

## SPEC.md との関係

この文書は `SPEC.md` の代わりになるものではありません。component の責務と data flow を把握するためにはこの文書を使い、正確な binary layout、magic number、CRC 範囲、protocol frame、configuration field、compatibility rule は `SPEC.md` を参照してください。

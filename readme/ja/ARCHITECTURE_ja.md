# AkkaraDB Native アーキテクチャ

この文書は AkkaraDB native C++23 engine の内部構造を、実装の責務とデータフローを中心に説明します。正確な binary layout、magic number、CRC 範囲、protocol frame、設定 field の完全な一覧は `SPEC.md` を source of truth とします。

## 概要

AkkaraDB native は binary-safe な key/value を LSM-tree 構造で保存する C++23 storage engine です。write はまず MemTable に入り、durability が有効な構成では WAL segment に追記されます。MemTable は threshold 到達後に immutable な SST file へ flush され、SST は level 間で compaction されます。

native engine は最小限の LSM 実装だけではなく、次の subsystem を持っています。

- Blob Manager による大きな value の外部化
- Version Log による per-key history、point-in-time read、rollback
- BinPack serialization と secondary index を使う typed table API
- `Ref<T>`、foreign key、cascade delete、join を扱う high-level API
- Kotlin/JVM 層から使う JNI entry point
- 組み込み HTTP / binary TCP / optional gRPC API server
- standalone / mirror replication のための cluster runtime
- mbedTLS による TLS transport support

全体像は次のようになります。

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
|  | - AK5 protocol    |      | - mirror routing             |  |
|  +-------------------+      +------------------------------+  |
+---------------------------------------------------------------+
```

## ストレージレイアウト

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

各永続化 component の役割は分かれています。

| Component | 役割 |
|---|---|
| WAL | crash 後に acknowledged / pending write を再生する |
| SST | immutable sorted key/value record を保存する |
| Manifest | live SST file と compaction transition を管理する |
| Blob files | 大きな value を MemTable / WAL / SST payload の外に保存する |
| Version Log | point-in-time read と rollback 用の履歴を保存する |
| Cluster config | cluster topology を保存する。runtime TLS path は含めない |

## Public API layer

native repository は主に 2 つの API layer を公開します。

`engine::AkkEngine` は byte-oriented な core API です。key/value は raw byte span として扱い、point read、write、batch write/read、remove、scan、history、rollback、flush、sync、close を提供します。public method は thread-safe になるよう設計されています。

`AkkaraDB` と `PackedTable<&T::id>` は high-level typed API です。C++ aggregate entity を BinPack で raw key/value row に変換します。table key は 8-byte の FNV-1a table prefix で namespace 化され、secondary index は `table_name + ":idx:" + field_name` から導出される別 prefix を使います。

高レベル API は次の責務も持ちます。

- primary key の encode と table scope の維持
- secondary index entry の作成、更新、削除
- query predicate から index source scan への簡易 planning
- `Ref<T>` の lazy resolve と dirty reference の cascade put
- foreign key の存在検証
- cascade delete
- `Ref<T>` join と任意 field join

JVM 層は `AKKARADB_BUILD_JNI=ON` のとき、JNI bridge 経由で同じ native engine にアクセスします。JNI bridge は raw operation、scan cursor、query scan payload evaluation、option-based open、rollback entry point を Kotlin module に提供します。

## AkkEngine

`AkkEngine` は storage component の coordinator です。主な責務は次の通りです。

- `dataDir` から component path を導出する
- option に従って WAL、SST、Blob、Manifest、VersionLog、API、Cluster component を作成または open する
- top-level write の sequence assignment を serialize する
- 大きな value を Blob Manager に外部化する
- WAL、VersionLog、MemTable mutation、cluster shipping を一貫した順序で実行する
- MemTable record を SST へ flush する
- read を MemTable、SST、BlobManager に route する
- component を決まった順序で close する

## Record と key のモデル

raw engine は key を bytewise lexicographic comparison で順序付けします。上位層は必要な探索順序に合うように key を encode します。

MemTable 上の record は `OwnedRecord` で表されます。これは `MemHdr16`、64-bit key fingerprint、first 8 bytes の `miniKey`、inline または arena-backed の `[key][value]` payload を持つ compact な record object です。

SST 上の record は `SSTHdr32` に key bytes と value bytes が続く形式です。`keyFp64` と `miniKey` は高速 reject / block 内 search 用の hint で、正しさの基準は常に full key comparison です。

value が Blob Manager に外部化される場合、record の value は 20-byte の `BlobRef` になり、blob flag が立ちます。read 側は `BlobRef` を間接参照として扱い、blob header と content CRC を検証したうえで元の value を返します。

`PackedTable` の primary row key は `[table_prefix:8][encoded_pk]` です。integral primary key は固定長 little-endian で、non-integral primary key は BinPack で encode されます。secondary index key は `[index_prefix:8][field_len:u32le][encoded_field][encoded_pk]` です。indexed field は query range に使うため、整数と浮動小数では sortable big-endian encoding を使い、それ以外では BinPack encoding を使います。

## メモリ管理

native engine は hot path を読みやすく保つため、用途別の buffer abstraction を使います。

`BufferArena` は scan や API call の一時 allocation を所有します。`AkkEngine::scan` が返す key/value span は、この arena の lifetime 中だけ有効です。

`OwnedBuffer` は I/O 向けの aligned owning storage です。`BufferView` は対応する non-owning view で、view から所有 buffer へ deep copy できます。

`SmallBuffer` は `OwnedRecord` に埋め込まれ、短い `[key][value]` payload を inline に保持します。大きい payload は arena-backed storage に移されます。

## MemTable

MemTable は最初の read/write target です。contention を下げるため shard 化されており、`RuntimeOptions::writerThreads` が設定されている場合、MemTable と WAL の shard count は writer count から導出され、設定された上限で cap されます。

write sequence は MemTable の sequence model から割り当てられます。

```text
reserve_seq(1) -> globally monotonic seq
last_seq()     -> read snapshot for point reads and scans
replay(seq)    -> advances sequence state during recovery
```

lookup は指定 snapshot で見える record を返します。tombstone は caller に返され、古い SST value を抑止するために使われます。

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

## WAL

WAL は crash recovery のために mutation record を append-only で保存します。native v5 は `{dataDir}/wal` 配下に segmented WAL を持ち、各 segment は CRC で保護された `WalSegmentHeader` から始まります。

entry は次の形式で serialize されます。

```text
[WalEntryHeader:32][key bytes][value bytes]
```

entry header には record sequence、key fingerprint、entry 全体の長さ、value length、key length、flags、header-with-zero-crc + key + value に対する CRC32C が含まれます。startup recovery では segment header と entry CRC を検証してから fresh MemTable に entry を適用します。

WAL の sync mode は `SYNC`、`ASYNC`、`OFF` です。高レベル `StartupMode::FAST` と `NORMAL` は async、`DURABLE` は sync、`ULTRA_FAST` は WAL disabled になります。

## Blob Manager

Blob Manager は `blob.thresholdBytes` 以上の value を外部化します。default は 16 KiB です。これにより MemTable record、WAL entry、SST block を小さく保ちつつ、通常の `get` API では透過的に元の value を返せます。

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

## SST Manager

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

lookup は file min/max key check、Bloom filter negative check、block index binary search、in-block binary search の順に絞り込みます。data block は raw concatenated SST record または Zstd-compressed bytes を持ちます。

flush は sorted MemTable record から新しい SST を作ります。compaction は overlapping または budget 超過の SST set を lower level へ rewrite し、その transition を Manifest に記録します。

## Manifest

Manifest は append-only で CRC-protected な storage lifecycle log です。live SST file、compaction transition、checkpoint、cluster metadata event を追跡します。

```text
[ManifestFileHeader:32][ManifestRecordHeader:8][payload]...
```

主な record type は `SSTSeal`、`Checkpoint`、`CompactionStart`、`CompactionCommit`、`Truncate`、`NodeJoin`、`NodeLeave`、`PrimaryLease` です。Manifest replay は in-memory の SST lifecycle state を再構築し、malformed record や CRC-invalid record は適用しません。

## Version Log

Version Log は有効な場合に per-key history を記録します。次の機能の土台になります。

- `getAt(key, seq)`
- `history(key)`
- `rollbackTo(seq)`
- `rollbackKey(key, seq)`

各 version entry は sequence、source node id、timestamp、flags、value bytes を保存します。rollback によって生成される record は reserved rollback node id と rollback flag を使い、通常 write と区別できます。

Version history は `FAST` / `NORMAL` では default disabled、`DURABLE` では default enabled です。`AkkaraDB::Options::Overrides::versionLogEnabled` で変更できます。

## API Servers

`components.apiEnabled` が設定されている場合、engine は embedded API backend を起動できます。`api.backends` が空なら HTTP と TCP が起動対象です。GRPC は backend enum と transport factory があり、gRPC module が登録されている build で有効化できます。

| Backend | Default port | 目的 |
|---|---:|---|
| HTTP | 7070 | REST-style key/value operation |
| TCP | 7071 | binary AK5 request/response protocol |
| GRPC | 7072 | protobuf/gRPC service |

binary protocol は `AK5Q` request frame と `AK5S` response frame を使います。現在の opcode には `Get`、`Put`、`Remove`、`GetAt`、`BatchPut`、`BatchGet`、`Ping`、`Exists`、`Count`、`Scan`、`History`、`RollbackTo`、`RollbackKey`、`ForceSync`、`ForceFlush`、`Stats` があります。

HTTP API は `/v1/ping`、`/v1/put`、`/v1/get`、`/v1/remove`、`/v1/exists`、`/v1/count`、`/v1/scan`、`/v1/getAt`、`/v1/history`、`/v1/rollbackTo`、`/v1/rollbackKey`、`/v1/batchPut`、`/v1/batchGet`、`/v1/forceSync`、`/v1/forceFlush`、`/v1/stats` を公開します。

## Cluster Runtime と TLS

cluster layer は `Standalone`、`Mirror`、`Stripe` の mode を持ちます。`Stripe` は cluster router の deterministic rendezvous hash によって、key ごとに data-bearing node を 1 つ選びます。現行 runtime は Stripe config を受け付け、同じ owner selection を `ClusterRuntime::router()` から参照できます。node set の変更や placement policy の変更に伴う ownership migration は、明示的な運用手順として扱います。

node role は `Standalone`、`Primary`、`Replica` です。Primary は write を受け取り record と blob を replica に ship します。Replica は engine から渡される callback を使って replicated record / blob を適用します。acknowledgement policy は `Async`、`All`、`Quorum` です。

replication link は TCP です。`TransportMode::SECURE` は native secure channel で TCP stream を保護し、`TransportMode::PLAIN` は node host が loopback または LAN/private address の場合だけ許可されます。`localhost` 以外の hostname は config validation では non-private として扱われます。

Primary selection は coordinator-eligible な最小 `node_id` による deterministic selection です。これは quorum consensus ではないため、split-brain-safe な automatic failover はこの layer の対象外です。

TLS support は mbedTLS によって current native target に組み込まれています。API server と replication link は runtime option に応じて TLS または plain transport を使います。

## データフロー

write path は次の流れです。

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

read path は MemTable を先に見て、見つからない場合に SST を参照します。blob flag がある record は Blob Manager を通して dereference されます。`runtime.sstPromoteReads` が有効なら、SST hit は MemTable に promotion されます。

range scan は caller が渡した `BufferArena` に紐づく view を返します。MemTable と SST iterator を merge し、同じ key では新しい visible record が勝ち、tombstone は古い value を抑止します。

typed table write は次の流れです。

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

`Ref<T>` field がある場合、table は attached binding を使って dirty reference を先に保存し、foreign key が登録されていれば参照先の存在を検証します。

## Recovery と shutdown

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

recovery policy は component ごとに局所化されています。WAL recovery は header と entry CRC を検証し、SST reader は header、footer、block metadata、block CRC を検証します。Blob read は header CRC と original content CRC を検証し、Manifest replay は malformed / CRC-invalid record を適用しません。

`AkkEngine::close` は idempotent で、次の順序で component を close します。

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

## 並行性モデル

public storage component は public method boundary で thread-safe に扱えるよう設計されています。

| Component | 保証 |
|---|---|
| `AkkEngine` | public method は thread-safe |
| `MemTable` | public method は thread-safe |
| `WalWriter` | append / sync / close path は thread-safe |
| `BlobManager` | read / write / delete scheduling は thread-safe |
| `Manifest` | public method は thread-safe |
| `VersionLog` | public method は thread-safe |
| `ClusterRuntime` | start / close / ship path は endpoint access を serialize する |

top-level write は `AkkEngine::Impl::write_mu` によって serialize されます。sequence assignment と write に付随する side effect を一貫した順序で扱うためです。

background work には WAL async flusher、MemTable shard flushing、SST compaction worker、Manifest fast-mode flusher、VersionLog async flusher、Blob cleanup、API server accept / connection handling、Cluster manager / replication endpoint が含まれます。

## StartupMode

high-level `AkkaraDB::open` は `StartupMode` preset を `AkkEngineOptions` に変換します。

| Mode | WAL | Blob | Manifest | SST | VersionLog | Close behavior | MemTable threshold |
|---|---|---|---|---|---|---|---|
| `ULTRA_FAST` | disabled | disabled | disabled | disabled | disabled | no force flush/sync | 512 MiB/shard |
| `FAST` | async | enabled | enabled | enabled | disabled | force flush/sync | 256 MiB/shard |
| `NORMAL` | async | enabled | enabled | enabled | disabled | force flush/sync | 64 MiB/shard |
| `DURABLE` | sync | enabled | enabled | enabled | enabled | force flush/sync | 64 MiB/shard |

`FAST` は SST read promotion も有効化します。fine-grained override によって MemTable threshold、VersionLog、SST / blob codec、blob threshold、Bloom filter density、L0 compaction trigger、SST read promotion を変更できます。

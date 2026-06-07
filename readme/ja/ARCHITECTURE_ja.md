# AkkaraDB Native アーキテクチャ

この文書は、AkkaraDB native C++23 エンジンの内部構成を説明します。v5 技術仕様書である `SPEC.md` は正確なファイル形式や互換性条件を定義する文書で、この文書は各コンポーネントがどうつながり、データがどう流れるかを理解するための設計解説です。

## 目次

- [概要](#概要)
- [ストレージレイアウト](#ストレージレイアウト)
- [コアコンポーネント](#コアコンポーネント)
  - [Public API レイヤー](#public-api-レイヤー)
  - [AkkEngine](#akkengine)
  - [レコードとキーのモデル](#レコードとキーのモデル)
  - [メモリ管理](#メモリ管理)
  - [MemTable](#memtable)
  - [Write-Ahead Log](#write-ahead-log)
  - [Blob Manager](#blob-manager)
  - [SST Manager](#sst-manager)
  - [Manifest](#manifest)
  - [Version Log](#version-log)
  - [API Servers](#api-servers)
  - [Cluster Runtime と TLS](#cluster-runtime-と-tls)
- [データフロー](#データフロー)
- [リカバリとシャットダウン](#リカバリとシャットダウン)
- [並行性モデル](#並行性モデル)
- [Startup Mode](#startup-mode)

---

## 概要

AkkaraDB native は、AkkaraDB の正準となる C++23 ストレージエンジンです。バイナリ安全な key/value を LSM-tree 構造で保存します。書き込みはまずインメモリの MemTable に入り、永続化が有効な構成では WAL segment に追記され、flush によって immutable な SST file が作られます。その後、SST は level 間で compaction されます。

native エンジンは最小限の LSM 実装だけではなく、次のようなサブシステムも持っています。

- Blob Manager による大きな値の外部化
- Version Log による任意の per-key history
- BinPack serialization と secondary index を使う型付き C++ table
- Kotlin/JVM 層から使うための JNI entry point
- 組み込み HTTP / binary TCP API server
- standalone / mirror / stripe deployment のための cluster / replication primitive
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
|  | - HTTP/TCP        |      | - primary/replica            |  |
|  | - AK5 protocol    |      | - mirror/stripe routing      |  |
|  +-------------------+      +------------------------------+  |
+---------------------------------------------------------------+
```

---

## ストレージレイアウト

`paths.data_dir` が設定されている場合、不足している各コンポーネントの path はその directory から導出されます。

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

正確な binary format は `SPEC.md` に定義されています。アーキテクチャ上は、永続化コンポーネントごとに役割が分かれている点が重要です。

| Component | 役割 |
|---|---|
| WAL | crash 後に acknowledged / pending write を再生する |
| SST | immutable sorted key/value record を保存する |
| Manifest | live SST file と compaction transition を管理する |
| Blob files | 大きな値を MemTable / WAL / SST payload の外に保存する |
| Version Log | point-in-time read と rollback 用の履歴を保存する |
| Cluster config | cluster topology を保存する。runtime TLS path は含めない |

---

## コアコンポーネント

### Public API レイヤー

native repository は主に 2 つの API レベルを公開しています。

`engine::AkkEngine` は byte-oriented な core API です。key/value は raw byte span として扱い、point read、write、remove、scan、history operation、rollback、flush、sync、close を提供します。

`AkkaraDB` と `PackedTable<&T::id>` は high-level typed API です。C++ aggregate entity を BinPack serialization によって raw key/value row に変換します。table key は 8-byte の FNV-1a table prefix で namespace 化され、secondary index は `table_name + ":idx:" + field_name` から導出される別 prefix を使います。

JVM 層は `AKKARADB_BUILD_JNI=ON` のとき、JNI bridge 経由で同じ native engine にアクセスします。JNI bridge は raw operation、scan cursor、query scan payload evaluation、option-based open、rollback entry point を Kotlin module に提供します。

### AkkEngine

`AkkEngine` は全体の coordinator です。storage component を所有または接続し、外側に thread-safe な mutation / read surface を提供します。

主な責務は次の通りです。

- `data_dir` から component path を導出する
- option に従って WAL、SST、Blob、Manifest、VersionLog、API、cluster component を作成または open する
- top-level write の sequence assignment を serialize する
- record を書く前に大きな値を externalize する
- 有効な場合は MemTable mutation の前に WAL と VersionLog へ append する
- MemTable record を SST へ flush する
- read を MemTable、SST、BlobManager に route する
- primary node として動作している場合、write と blob を cluster runtime に ship する
- component を決まった順序で close する

### レコードとキーのモデル

raw engine は key を bytewise lexicographic comparison で順序付けします。上位層は必要な順序が保たれるように key を encode する必要があります。`PackedTable` はよく使う primary-key layout で unsigned numeric order と byte order が合うように、integral primary key に little-endian encoding を使います。

メモリ上の record は `OwnedRecord` で表されます。これは 64-byte の compact な metadata object として設計されています。

```text
+-------------------+------------------------------------------+
| MemHdr16          | seq, key length, value length, flags     |
| key_fp64          | 64-bit key fingerprint                   |
| mini_key          | first up to 8 key bytes, little-endian   |
| SmallBuffer       | inline or arena-backed [key][value]      |
+-------------------+------------------------------------------+
```

SST 上の record は `SSTHdr32` を使います。これは 32-byte header に key bytes と value bytes が続く形式です。正しさの基準は常に full key comparison です。`key_fp64` と `mini_key` は高速 reject や block 内 search のための hint です。

value が Blob Manager に外部化される場合、record の value は 20-byte の `BlobRef` になり、blob flag が立ちます。read 側はそれを間接参照として扱い、blob header と content CRC を検証したうえで元の value を返します。

### メモリ管理

native engine は hot path を予測しやすくするため、小さく明示的な buffer abstraction を使います。

`BufferArena` は scan や API call 用の一時 allocation を所有します。`AkkEngine::scan` が返す view の key/value span は、この arena の lifetime 中だけ有効です。

`OwnedBuffer` は I/O 向けの aligned owning storage です。`BufferView` は対応する non-owning view です。

`SmallBuffer` は `OwnedRecord` に埋め込まれており、短い `[key][value]` payload を inline に保持します。大きい payload は record の arena-backed storage に移されます。これにより、よくある小さな record を compact に扱えます。

### MemTable

MemTable は最初の read / write target です。contention を減らすため shard 化されており、shard count は自動導出できます。`RuntimeOptions::writer_threads` が設定されている場合、MemTable と WAL の shard count は writer count から導出され、設定された上限で cap されます。

write sequence は MemTable の sequence model から割り当てられます。

```text
reserve_seq(1) -> globally monotonic seq
last_seq()     -> read snapshot for point reads and scans
replay(seq)    -> advances sequence state during recovery
```

lookup は指定 snapshot で見える record を返します。tombstone は MemTable 内で隠されず caller に返されます。これは、tombstone が古い SST value を抑止しなければならないためです。

flush lifecycle は次の流れです。

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

WAL は crash recovery のために mutation record を append-only で保存します。native v5 は `{data_dir}/wal` 配下に segmented WAL を持ち、各 segment は CRC で保護された `WalSegmentHeader` から始まります。

entry は次の形式で serialize されます。

```text
[WalEntryHeader:32][key bytes][value bytes]
```

entry header には record sequence、key fingerprint、entry 全体の長さ、value length、key length、flags、そして header-with-zero-crc + key + value に対する CRC32C が含まれます。

WAL には 3 つの sync mode があります。

| Mode | 意味 |
|---|---|
| `Sync` | synchronous durability path |
| `Async` | grouped background flush path |
| `Off` | 明示的な sync を行わない。cache / test mode 向け |

startup recovery では、segment header と entry CRC を検証してから fresh MemTable に entry を適用します。

### Blob Manager

Blob Manager は `blob.threshold_bytes` 以上の value を外部化します。default は 16 KiB です。これにより MemTable record、WAL entry、SST block を小さく保ちつつ、通常の `get` API では透過的に元の value を返せます。

write path は次の通りです。

```text
value size >= threshold
  |
  +-- BlobManager writes the payload to {data_dir}/blobs
  +-- optional Zstd compression is applied
  +-- Blob header and content CRC are stored
  +-- record value becomes BlobRef(blob_id, total_size, content_crc32c)
  +-- record flag includes blob
```

read path は次の通りです。

```text
record flag includes blob
  |
  +-- parse BlobRef
  +-- read blob file
  +-- validate header CRC and content CRC
  +-- return original value bytes
```

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

lookup は full record comparison に入る前に複数の filter を通ります。

```text
file min/max key check
  |
  +-- Bloom filter negative check
  +-- block index binary search
  +-- in-block binary search using SSTHdr32, mini_key, and full key compare
```

data block は raw concatenated SST record、または Zstd-compressed bytes を持ちます。block index は block の key boundary を保存し、Bloom filter は存在しない key に対する disk / cache work を減らします。

flush は sorted MemTable record から新しい SST を作ります。compaction は overlapping または budget 超過の SST set を lower level へ rewrite し、その transition を Manifest に記録します。`CompactionCommit` は input/output file change を replay 時に atomic に適用できるため、preferred な manifest event です。

### Manifest

Manifest は append-only で CRC-protected な storage lifecycle log です。live SST file、compaction transition、checkpoint、cluster metadata event を追跡します。

file は `ManifestFileHeader` で始まり、その後に record が append されます。

```text
[ManifestFileHeader:32][ManifestRecordHeader:8][payload]...
```

主な record type は次の通りです。

| Event | 目的 |
|---|---|
| `SSTSeal` | 新しい SST file が seal された |
| `Checkpoint` | named / unnamed checkpoint |
| `CompactionStart` | compaction 開始の informational marker |
| `CompactionCommit` | atomic な compaction input/output transition |
| `Truncate` | truncation の informational marker |
| `NodeJoin`, `NodeLeave`, `PrimaryLease` | cluster metadata |

Manifest replay は in-memory の SST lifecycle state を再構築します。malformed record や CRC-invalid record は適用されません。

### Version Log

Version Log は、有効な場合に per-key history を記録します。次の機能の土台になります。

- `get_at(key, seq)`
- `history(key)`
- `rollback_to(seq)`
- `rollback_key(key, seq)`

各 version entry は sequence、source node id、timestamp、flags、value bytes を保存します。rollback によって生成された record は reserved rollback node id と rollback flag を使い、通常 write と区別できます。

Version history は `FAST` / `NORMAL` では default disabled、`DURABLE` startup preset では default enabled です。override で変更できます。

### API Servers

`components.api_enabled` が設定されている場合、engine は embedded API backend を起動できます。

対応 backend は次の通りです。

| Backend | Default port | 目的 |
|---|---:|---|
| HTTP | 7070 | REST-style key/value operation |
| TCP | 7071 | binary AK5 request/response protocol |

binary protocol は `AK5Q` request frame と `AK5S` response frame を使います。現在の opcode には `Get`、`Put`、`Remove`、`GetAt` があります。

HTTP API は `/v1/ping`、`/v1/put`、`/v1/get`、`/v1/remove`、`/v1/get_at` のような基本 endpoint を公開します。

### Cluster Runtime と TLS

cluster layer は 3 つの deployment mode を持ちます。

| Mode | 意味 |
|---|---|
| `Standalone` | local-only operation |
| `Mirror` | write を data-bearing node 全体へ mirror する |
| `Stripe` | router policy によって key を data node に割り当てる |

`Stripe` の routing primitive はありますが、runtime は現時点では Stripe config を拒否します。distributed write forwarding と ownership migration が未実装のためです。

node role は `Standalone`、`Primary`、`Replica` です。Primary は write を受け取り、record や blob を replica に ship します。Replica は engine から渡された callback を使って replicated record / blob を適用します。

acknowledgement policy は `Async`、`All`、`Quorum` です。

cluster config の `NodeInfo.host` は peer が dial する advertise address です。primary の replication listener は runtime option の `repl_bind_host` に bind し、default は `0.0.0.0` です。replication link は TCP です。`TransportMode::TLS` は TCP stream を mbedTLS で包み、`TransportMode::Plain` は全 node host が loopback または LAN/private address の場合だけ許可されます。`localhost` 以外の hostname は config validation では non-private として扱います。primary selection は coordinator-eligible な最小 `node_id` による deterministic selection です。LAN/WAN の別マシンで同じ primary を選べますが、quorum consensus ではないため split-brain-safe な automatic failover はこの layer の対象外です。

TLS support は mbedTLS によって current native target に組み込まれています。API server と replication link は、runtime option に応じて TLS または plain transport を使えます。

---

## データフロー

### 書き込みフロー

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

sequence assignment、WAL append、VersionLog append、MemTable mutation、blob externalization、replication shipping は engine write mutex の下で順序付けられます。public method が並行に呼ばれても mutation order が一貫するようにするためです。

### 読み取りフロー

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

MemTable は常に current snapshot に対する最初の authority です。SST はすでに flush 済みの data に対する fallback です。Blob dereference は勝った record が選ばれた後に行われます。

### Range Scan フロー

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

返される view は arena lifetime に紐づきます。scan される key/value をすべて独立した heap object に copy しないための設計です。

### Typed Table フロー

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

secondary index entry は non-unique です。encoded primary key suffix によって同じ field value を持つ entry を区別でき、index scan から primary row を復元できます。

### JNI Query Scan フロー

```text
JVM scanQuery(start, end, queryBytes, schemaBytes)
  |
  +-- JNI opens native scan cursor over [start, end)
  +-- native side decodes schema payload
  +-- native side decodes query expression payload
  +-- each row value is decoded enough to evaluate predicates
  +-- matching rows are returned to JVM RowView(ByteBufferL key, ByteBufferL value)
```

query / schema payload の integer field は little-endian です。これは JVM serializer が `little-endian writer` を使うためです。未対応 operator は silently matching rows ではなく query-evaluation error として扱うべきです。

---

## リカバリとシャットダウン

### Startup Recovery

`AkkEngine::open` は決まった順序で startup します。

```text
1. Derive missing component paths from data_dir
2. Create required directories
3. Load or generate persistent node id
4. Open Manifest and prepare replay-capable state
5. Create SST Manager and recover SST state when enabled
6. Replay WAL into a fresh MemTable when enabled
7. Start WAL writer, BlobManager, VersionLog, ClusterRuntime, and API server as configured
```

recovery policy は component ごとに局所化されています。

- WAL recovery は segment header と entry CRC を検証してから entry を適用します。
- SST reader は header、footer、block metadata、block CRC を検証してから record を信頼します。
- Blob read は header CRC と original content CRC の両方を検証します。
- Manifest replay は malformed record や CRC-invalid record を適用しません。

### Crash Scenario

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

`AkkEngine::close` は idempotent です。component は次の順で close されます。

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

## 並行性モデル

public storage component は、public method boundary で thread-safe に扱えるように設計されています。

| Component | 保証 |
|---|---|
| `AkkEngine` | public method は thread-safe |
| `MemTable` | public method は thread-safe |
| `WalWriter` | append、sync、close path は thread-safe |
| `BlobManager` | read、write、delete scheduling は thread-safe |
| `Manifest` | public method は thread-safe |
| `VersionLog` | public method は thread-safe |
| `ClusterRuntime` | start、close、ship path は endpoint access を serialize する |

top-level write は `AkkEngine::Impl::write_mu` によって serialize されます。これは意図的な設計です。sequence assignment と write に付随する side effect を一貫した順序で扱えます。

background work には次のものが含まれます。

- WAL async flusher thread
- MemTable shard flushing
- SST compaction worker
- Manifest fast-mode flusher
- VersionLog async flusher
- Blob cleanup
- API server accept / connection handling
- Cluster manager / replication endpoint

---

## Startup Mode

high-level `AkkaraDB::open` は `StartupMode` preset を `AkkEngineOptions` に変換します。

| Mode | WAL | Blob | Manifest | SST | VersionLog | Close behavior | MemTable threshold |
|---|---|---|---|---|---|---|---|
| `ULTRA_FAST` | disabled | disabled | disabled | disabled | disabled | no force flush/sync | 512 MiB/shard |
| `FAST` | async | enabled | enabled | enabled | disabled | force flush/sync | 256 MiB/shard |
| `NORMAL` | async | enabled | enabled | enabled | disabled | force flush/sync | 64 MiB/shard |
| `DURABLE` | sync | enabled | enabled | enabled | enabled | force flush/sync | 64 MiB/shard |

`FAST` は SST read promotion も有効化します。fine-grained override によって MemTable threshold、VersionLog、SST / blob codec、blob threshold、Bloom filter density、L0 compaction trigger、SST read promotion を変更できます。

---

## SPEC.md との関係

この architecture document は `SPEC.md` の代替ではありません。この文書は component responsibility と data flow を理解するために使います。正確な binary layout、magic number、CRC range、protocol frame、configuration field、compatibility rule は `SPEC.md` を source of truth とします。

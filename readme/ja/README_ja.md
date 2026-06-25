# AkkaraDB

**WAL、LSM ストレージ、型付きテーブル、JNI アクセス、任意のクラスタリングを備えた低レイテンシの組み込み KV エンジンです。**

> C++23 | LSM-tree | WAL | SST | Blob store | Version history | HTTP/TCP API | JNI | AGPL-3.0

---

## 特徴

| 分類 | 内容 |
|---|---|
| ストレージ | マルチシャード MemTable、WAL、SST レベル、Bloom filter、leveled compaction |
| 耐久性 | CRC32C で保護されたレコード、sync/async WAL、Manifest による SST ライフサイクル管理 |
| 大きな値 | 20 バイトの BlobRef を使った自動 blob 外部化 |
| 圧縮 | SST と blob payload に Zstandard を利用。codec 情報はファイルごとの metadata に保持 |
| 読み取り | まず MemTable を参照し、必要に応じて SST と BlobManager にフォールバック |
| 履歴 | 任意の per-key version log、特定時点読み取り、全体またはキー単位の rollback |
| API サーバー | HTTP REST と binary TCP backend。既定ポートは 7070 / 7071 |
| 型付きテーブル | `PackedTable<&T::id>`、BinPack serialization、stable `RowId`、scan、join、secondary index |
| 型付き更新 | `Immutable<T>` / `Const<T>`、`onUpdate<&Field>(...)` hook、foreign-key の `OnDelete` / `OnUpdate` |
| JVM ブリッジ | public low-level API では `ByteBufferL` を使う Kotlin/JVM + JNI ラッパー |
| クラスタリング | standalone、mirror、stripe の replication primitive |
| 消失訂正 | low-level API に `XOR`、`DualXOR`、`RS`、`ERS` codec を同梱 |
| 可搬性 | Windows/MSVC と Linux/GCC/Clang をサポート |

---

## クイックスタート

### ビルド

```cmake
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

よく使うフラグ:

```cmake
-DBUILD_SHARED_LIBS=ON        # shared library をビルド。既定は ON
-DBUILD_SHARED_LIBS=OFF       # static library をビルド
-DAKKARADB_BUILD_TESTS=ON     # tests を追加
-DAKKARADB_BUILD_JNI=ON       # JVM wrapper 用の akkaradb_jni をビルド
```

現在の native CMake 設定では TLS と SIMD が有効です。ビルド時には mbedTLS をリンクし、対応するコンパイラでは SSE4.2 / AVX2 のフラグを追加します。

Windows では、compiler、linker、Windows SDK が初期化された Visual Studio / MSVC 環境からビルドしてください。

### インストール済みターゲットの利用

```cmake
find_package(AkkaraDB REQUIRED)
target_link_libraries(my_app PRIVATE AkkaraDB::akkaradb)
```

---

## C++ 低レベル API

キー、値、scan、耐久性、履歴を byte 指向で制御したい場合は `AkkEngine` を直接使います。

```cpp
#include "engine/AkkEngine.hpp"

#include <span>
#include <string_view>

using namespace akkaradb::engine;

std::span<const uint8_t> bytes(std::string_view s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

AkkEngineOptions opts;
opts.paths.dataDir = "data";
opts.components.versionLogEnabled = true;
opts.wal.syncMode = wal::WalSyncMode::ASYNC;
opts.blob.thresholdBytes = 16 * 1024;
opts.runtime.sstPromoteReads = true;

auto engine = AkkEngine::open(std::move(opts));

engine->put(bytes("user:1"), bytes("Alice"));

auto value = engine->get(bytes("user:1"));
if (value) {
    // value is std::vector<uint8_t>
}

std::vector<uint8_t> out;
if (engine->getInto(bytes("user:1"), out)) {
    // hot read path without optional allocation
}

engine->remove(bytes("user:1"));
engine->forceSync();
engine->close();
```

range scan では caller 側が所有する arena を使います。

```cpp
akkaradb::core::BufferArena arena;
auto rows = engine->scan(arena, bytes("user:"), bytes("user;"));

for (auto it = rows.begin(); !(it == rows.end()); ++it) {
    auto key = it->key;
    auto value = it->value;
}
```

`versionLogEnabled` が有効なら履歴 API が使えます。

```cpp
auto at = engine->getAt(bytes("user:1"), target_seq);
auto history = engine->history(bytes("user:1"));

engine->rollbackKey(bytes("user:1"), target_seq);
engine->rollbackTo(target_seq);
```

low-level header には erasure coding helper も含まれます。

```cpp
#include "akk/engine/erasure/ErasureCodec.hpp"
#include "akk/engine/erasure/ErasureCodecExt.hpp"

using namespace akkaradb::engine::erasure;

const ErasureLayout layout{.dataShards = 6, .parityShards = 3};
auto shards = RsErasureCodec::encode(bytes("payload"), layout);
auto decoded = RsErasureCodec::decode(shards, layout);
auto recovered = ErsCodec::recover(ErsCodec::encode(bytes("payload"), layout), layout, {1});
```

---

## C++ 高レベル API

BinPack による自動 serialization と table ごとの key namespace を使いたい場合は `AkkaraDB` と `PackedTable` を使います。

```cpp
#include <akkaradb/AkkaraDB.hpp>

#include <cstdint>
#include <string>

struct User {
    uint64_t id;
    std::string email;
    std::string name;
    uint32_t age;
};

AKKARADB_QUERYABLE(User, id, email, name, age)

auto db = akkaradb::AkkaraDB::open("data", akkaradb::StartupMode::FAST);
auto users = db->table<&User::id>("users");

auto by_age = users.index<&User::age>();
auto by_email = users.index<&User::email>();

users.put({1, "alice@example.test", "Alice", 30});
users.put({2, "bob@example.test", "Bob", 30});

auto alice = users.get(1ULL);

User out{};
if (users.getInto(2ULL, out)) {
    // out contains Bob
}

auto age30 = by_age.find(30U);
while (age30.hasNext()) {
    auto entry = age30.next();
    auto id = entry.id;
    auto user = entry.value;
}

auto adults = users
    .query([](const User& user) { return user.age >= 18; })
    .limit(100)
    .toVector();

auto first_bob = users.findBy<&User::email>(std::string{"bob@example.test"});

users.upsert(2ULL, [](User& user) {
    user.name = "Bobby";
});

users.remove(1ULL);
db->close();
```

typed table の key は table ごとに namespace 分離されます。primary key は 8-byte FNV-1a table prefix に encoded primary key を続けた構造で、整数 primary key には sortable な fixed-width big-endian encoding を使うため、range scan でも数値順を保てます。secondary index は `table_name + ":idx:" + field_name` を namespace として使います。

stable row identity は `rowIdOf(pk)`、`primaryKeyOf(rowId)`、`getByRowId(rowId)` で扱えます。永続化後に変更させたくない field は `akkaradb::Immutable<T>` または `akkaradb::Const<T>` にでき、`onUpdate<&Field>(...)` hook で保存前に replacement entity を書き換えることもできます。

schema 管理の foreign key は `OnDelete` / `OnUpdate` として `Cascade`、`Restrict`、`SetNull` をサポートします。`Ref<T>` は内部に target row id を覚えるため、primary key が更新された後も同じ logical entity を追跡できます。

```cpp
struct User {
    uint64_t id;
    akkaradb::Const<std::string> externalId;
    std::string email;
    std::string name;
};

AKKARADB_QUERYABLE(User, id, externalId, email, name)

users.onUpdate<&User::email>([](const std::string& old_value, std::string& new_value) {
    if (new_value.empty()) { new_value = old_value; }
});

if (auto row_id = users.rowIdOf(1ULL)) {
    auto same_user = users.getByRowId(*row_id);
    (void)same_user;
}
```

高レベル API の詳細は [API_USAGE_ja.md](API_USAGE_ja.md) を参照してください。

---

## JVM 低レベル API

JVM wrapper の public low-level API は raw `ByteArray` ではなく `ByteBufferL` を使います。

```kotlin
import dev.swiftstorm.akkaradb.core.buffer.ByteBufferL
import dev.swiftstorm.akkaradb.engine.AkkEngine
import dev.swiftstorm.akkaradb.engine.AkkaraOptions
import dev.swiftstorm.akkaradb.engine.Codec
import dev.swiftstorm.akkaradb.engine.StartupMode

val engine = AkkEngine.open(
    AkkaraOptions(
        dataDir = "data",
        mode = StartupMode.FAST,
        overrides = AkkaraOptions.Overrides(
            blobThresholdBytes = 32L * 1024L,
            sstCodec = Codec.ZSTD,
            blobCodec = Codec.ZSTD,
            sstPromoteReads = true,
            sstBloomBitsPerKey = 10,
            maxL0SstFiles = 8
        )
    )
)

val key = ByteBufferL.wrap(byteArrayOf(1, 2, 3))
val value = ByteBufferL.wrap("hello".encodeToByteArray())

engine.put(key, value)

val got: ByteBufferL? = engine.get(key)
val exists: Boolean = engine.exists(key)
val count: Long = engine.count()

for (row in engine.scan()) {
    val rowKey = row.key
    val rowValue = row.value
}

engine.remove(key)
engine.close()
```

`ByteBufferL.allocate(capacity, direct = true)` は little-endian の direct buffer を作ります。heap-backed な入力や test では `ByteBufferL.wrap(ByteArray)` が便利です。

---

## JVM 高レベル API

Kotlin entity では `AkkaraDB.table<T, ID>()` を使います。JVM 側の high-level table は、C++ 側と同じ table namespace と index key layout を踏襲します。

```kotlin
import dev.swiftstorm.akkaradb.engine.AkkaraDB
import dev.swiftstorm.akkaradb.engine.AkkaraOptions
import dev.swiftstorm.akkaradb.engine.StartupMode

data class User(
    val id: Long,
    val name: String,
    val age: Int
)

val db = AkkaraDB.open(AkkaraOptions("data", StartupMode.FAST))
val users = db.table<User, Long>("users") { it.id }

val byName = users.index("name") { it.name }
val byAge = users.index("age") { it.age }

users.put(User(1L, "Alice", 30))
users.put(User(2L, "Bob", 25))

val alice: User? = users.get(1L)
val alices: List<User> = byName.find("Alice").toList()
val adults: List<User> = users.filter { it.age >= 18 }.toList()
val allRows: List<User> = users.scanAll().toList()

users.remove(2L)
db.close()
```

手動の query 実行には `runQ(AkkQuery(...))` が使えます。`query { ... }`、`firstOrNull { ... }`、`runToList { ... }` のような DSL は compiler plugin による rewrite を前提としており、plugin なしでは runtime error になる設計です。

---

## 起動モード

| モード | WAL | close 時の挙動 | version history | 主な用途 |
|---|---|---|---|---|
| `ULTRA_FAST` | Off | 強制 flush/sync なし | Off | in-memory cache、test、一時データ |
| `FAST` | Async | close 時に flush/sync | Off | 高スループット ingestion |
| `NORMAL` | Async | close 時に flush/sync | Off | 一般用途の既定値 |
| `DURABLE` | Sync | close 時に flush/sync | On | audit log、強い耐久性が必要な用途 |

細かい override は C++ と JVM の両方から設定できます。

| 項目 | C++ | JVM |
|---|---|---|
| MemTable threshold per shard | `memtableThresholdPerShard` | `memtableThresholdPerShard` |
| Version log | `versionLogEnabled` | `versionLogEnabled` |
| SST codec | `sstCodec` | `sstCodec` |
| Blob codec | `blobCodec` | `blobCodec` |
| Blob threshold | `blobThresholdBytes` | `blobThresholdBytes` |
| SST read promotion | `sstPromoteReads` | `sstPromoteReads` |
| Bloom bits per key | `sstBloomBitsPerKey` | `sstBloomBitsPerKey` |
| Max L0 SST files | `maxL0SstFiles` | `maxL0SstFiles` |

---

## アーキテクチャ概要

```text
put(key, value)
  |
  +-- value >= blob threshold
  |     |
  |     +-- BlobManager writes .blob payload
  |     +-- MemTable stores a 20-byte BlobRef
  |
  +-- value < blob threshold
        |
        +-- WAL append with CRC32C
        +-- VersionLog append when enabled
        +-- MemTable insert
              |
              +-- shard threshold reached
                    |
                    +-- flush to SST
                    +-- compaction across levels
```

read path:

```text
MemTable -> SSTManager -> BlobManager when the value is externalized
```

SST read では Bloom filter と sparse block index を使います。`sstPromoteReads` が有効なら、SST hit を MemTable に昇格できます。

---

## 設定の要点

```cpp
akkaradb::AkkaraDB::Options opts;
opts.dataDir = "data";
opts.mode = akkaradb::StartupMode::FAST;

opts.overrides.memtableThresholdPerShard = 128ULL << 20;
opts.overrides.versionLogEnabled = true;
opts.overrides.sstCodec = akkaradb::Codec::ZSTD;
opts.overrides.blobCodec = akkaradb::Codec::ZSTD;
opts.overrides.blobThresholdBytes = 32ULL * 1024ULL;
opts.overrides.sstPromoteReads = true;
opts.overrides.sstBloomBitsPerKey = 10;
opts.overrides.maxL0SstFiles = 8;

auto db = akkaradb::AkkaraDB::open(std::move(opts));
```

native configuration 全体は [SPEC.md section 14](../../SPEC.md#14-configuration-reference) を参照してください。

---

## API サーバー

`components.apiEnabled` を有効にすると、internal engine から HTTP と TCP API backend を起動できます。

```cpp
AkkEngineOptions opts;
opts.paths.dataDir = "data";
opts.components.apiEnabled = true;
opts.api.bind_host = "127.0.0.1";
opts.api.backends = {
    AkkEngineOptions::ApiBackend::Http,
    AkkEngineOptions::ApiBackend::Tcp,
};
opts.api.http_port = 7070;
opts.api.tcp_port = 7071;
```

HTTP endpoint:

| Method | Path |
|---|---|
| `GET` | `/v1/ping` |
| `POST` | `/v1/put?key=<percent-encoded>` |
| `GET` | `/v1/get?key=<percent-encoded>` |
| `DELETE` | `/v1/remove?key=<percent-encoded>` |
| `GET` | `/v1/getAt?key=<percent-encoded>&seq=<number>` |

binary TCP protocol は `AK5Q` request frame と `AK5S` response frame を使います。frame layout は [SPEC.md section 11.2](../../SPEC.md#112-binary-protocol-v2) を参照してください。

---

## ファイル配置

```text
{dataDir}/
|-- wal/             Write-ahead log segments
|-- sstable/         SST levels
|   |-- L0/
|   |-- L1/
|   |-- ...
|   `-- L6/
|-- blobs/           Externalized value payloads
|-- history.akvlog   Version log when enabled
|-- manifest.akmf    SST lifecycle manifest
|-- cluster.akcc     Cluster topology when enabled
`-- node.id          Persistent node identity
```

---

## ベンチマークと smoke test

native CMake では benchmark と smoke test の executable を `build/bin` に出力します。

```powershell
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release

.\build\bin\akkaradb_akkengine_smoke_test.exe
.\build\bin\akkaradb_typed_api_smoke_test.exe
.\build\bin\akkaradb_benchmark.exe
```

benchmark source は次の場所にあります。

```text
benchmarks/suite/
benchmarks/smoke/
benchmarks/throughput/
benchmarks/api/
```

---

## 依存ライブラリ

| Library | Version | Notes |
|---|---|---|
| Zstandard | 1.5.6 | SST と blob payload の圧縮 |
| Boost.PFR | boost-1.84.0 | BinPack 用 aggregate reflection |
| mbedTLS | 4.1.0 | API と replication transport の TLS |

dependency は CMake の `FetchContent` で取得します。

---

## 仕様書

現在の native specification は [SPEC.md](../../SPEC.md) です。record layout、WAL / SST / blob / manifest format、API framing、configuration、concurrency、recovery behavior をまとめています。

## アーキテクチャ

native engine の全体像は [ARCHITECTURE_ja.md](ARCHITECTURE_ja.md) を参照してください。public API、`AkkEngine`、MemTable、WAL、Blob Manager、SST Manager、Manifest、VersionLog、API server、cluster runtime、JNI scan path の関係を説明しています。

## Native API の使い方

native low-level / high-level API の詳細は [API_USAGE_ja.md](API_USAGE_ja.md) を参照してください。`AkkEngine` の直接利用と、typed な `AkkaraDB` / `PackedTable` API を扱っています。

---

## ライセンス

AkkaraDB は GNU Affero General Public License v3.0 のもとで配布される free software です。

Copyright (C) 2026 Swift Storm Studio.

ライセンス全文は [LICENSE](../../LICENSE) を参照してください。

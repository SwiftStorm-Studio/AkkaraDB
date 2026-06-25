# AkkaraDB Native API 使用ガイド

この文書では、native C++ の low-level API と high-level API を扱います。

low-level API は `akkaradb::engine::AkkEngine` です。byte 指向の API で、キーと値は `std::span<const uint8_t>` として受け渡しされます。キー設計、serialize / deserialize、scan、durability、history の扱いは caller 側が決めます。

high-level API は `akkaradb::AkkaraDB` と `akkaradb::PackedTable` です。C++ struct を BinPack で保存し、table ごとの key namespace、primary key 操作、stable row id、secondary index、scan、join、foreign key、query helper を提供します。

## ビルドと include

AkkaraDB を CMake package として導入している場合は、export された target をリンクします。

```cmake
find_package(AkkaraDB CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE AkkaraDB::akkaradb)
target_compile_features(my_app PRIVATE cxx_std_23)
```

high-level API だけなら、通常は次の include で足ります。

```cpp
#include <akkaradb/AkkaraDB.hpp>
```

low-level engine を直接使う場合は engine header を include します。

```cpp
#include "akk/engine/AkkEngine.hpp"
```

## 低レベル API

`AkkEngine` は raw key/value API です。engine 自体は schema を解釈しないため、key design、value encoding、decode logic は caller 側の責務です。

```cpp
#include "akk/engine/AkkEngine.hpp"

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace engine = akkaradb::engine;

std::span<const uint8_t> bytes(std::string_view value) {
    return {
        reinterpret_cast<const uint8_t*>(value.data()),
        value.size()
    };
}

std::string text(std::span<const uint8_t> value) {
    return {
        reinterpret_cast<const char*>(value.data()),
        value.size()
    };
}

int main() {
    engine::AkkEngineOptions opts;
    opts.paths.dataDir = "data";
    opts.components.versionLogEnabled = true;
    opts.wal.syncMode = engine::wal::WalSyncMode::ASYNC;
    opts.blob.thresholdBytes = 32 * 1024;
    opts.runtime.sstPromoteReads = true;

    auto db = engine::AkkEngine::open(std::move(opts));

    db->put(bytes("user:1"), bytes("Alice"));
    db->put(bytes("user:2"), bytes("Bob"));

    if (auto value = db->get(bytes("user:1"))) {
        auto name = std::string{
            reinterpret_cast<const char*>(value->data()),
            value->size()
        };
        (void)name;
    }

    std::vector<uint8_t> out;
    if (db->getInto(bytes("user:2"), out)) {
        auto name = std::string{
            reinterpret_cast<const char*>(out.data()),
            out.size()
        };
        (void)name;
    }

    const bool exists = db->exists(bytes("user:1"));
    const size_t count = db->count(bytes("user:"), bytes("user;"));
    (void)exists;
    (void)count;

    db->remove(bytes("user:1"));
    db->forceSync();
    db->close();
}
```

`get()` は `std::optional<std::vector<uint8_t>>` を返します。hot path で caller 所有の buffer を再利用したい場合は `getInto()`、arena-backed な view が欲しい場合は `getIntoArena()` を使います。

### 走査

`scan()` は caller 所有の `BufferArena` を受け取り、`ArenaGenerator<ScanRecordView>` を返します。返される key/value view は arena の寿命に依存するため、scan 中は arena を生かしておく必要があります。

```cpp
akkaradb::core::BufferArena arena;
auto rows = db->scan(arena, bytes("user:"), bytes("user;"));

for (auto it = rows.begin(); !(it == rows.end()); ++it) {
    std::span<const uint8_t> key = it->key;
    std::span<const uint8_t> value = it->value;
}
```

返される `key` と `value` は non-owning view です。arena を reset、clear、destroy した後に使ってはいけません。scan 結果を arena の寿命より長く保持したい場合は、すぐに owning copy を作ってください。

native AkkaraDB の buffer 型として保持するなら、`BufferView::to_owned()` を優先するのが分かりやすいです。bytes を `OwnedBuffer` に deep copy して、arena から寿命を切り離せます。

```cpp
#include "akk/core/buffer/BufferView.hpp"
#include "akk/core/buffer/OwnedBuffer.hpp"

auto value_bytes = std::as_bytes(row.value);
akkaradb::core::BufferView value_view{value_bytes};
akkaradb::core::OwnedBuffer owned_value = value_view.to_owned();
```

すでに `std::vector<uint8_t>` を使っているコードなら、vector にコピーしても問題ありません。

```cpp
std::vector<uint8_t> owned_key{row.key.begin(), row.key.end()};
std::vector<uint8_t> owned_value{row.value.begin(), row.value.end()};
```

`start_key` と `end_key` は half-open range を表す前提です。prefix scan では次の prefix を end key として渡します。上の例では `"user:"` から `"user;"` までを走査しています。

### バージョン履歴

`components.versionLogEnabled` を有効にすると、per-key history、point-in-time read、rollback が使えます。

```cpp
db->put(bytes("profile:1"), bytes("v1"));
db->put(bytes("profile:1"), bytes("v2"));

auto history = db->history(bytes("profile:1"));
if (!history.empty()) {
    auto old_value = db->getAt(bytes("profile:1"), history.front().seq);
    db->rollbackKey(bytes("profile:1"), history.front().seq);
}
```

`rollbackTo()` は engine 全体を指定 sequence まで戻します。`rollbackKey()` より影響範囲が大きいため、production では durability と運用上の前提を整理した上で呼ぶべき API です。

### 主な low-level 操作

| 操作 | API | 用途 |
|---|---|---|
| write | `put(key, value)` | key/value を保存 |
| batch write | `putBatch(entries)` | 複数の key/value をまとめて保存 |
| hint 付き write | `putHinted(key, value, fp64, miniKey)` | caller 側で key fingerprint を既に持っている hot path |
| delete | `remove(key)` | key を削除 |
| hint 付き delete | `removeHinted(key, fp64, miniKey)` | fingerprint を再計算せず削除したい hot path |
| read | `get(key)` | optional vector として取得 |
| batch read | `getBatch(keys)` | 複数 key をまとめて取得 |
| buffer へ read | `getInto(key, out)` | caller 所有の vector に書き込む |
| arena read | `getIntoArena(key, arena, out)` | arena 依存の view として読む |
| exists | `exists(key)` | key の存在確認 |
| count | `count(start, end)` | range 内の key 数を数える |
| scan | `scan(arena, start, end)` | range 内の key/value を反復 |
| stats | `stats()` | engine、WAL、MemTable、SST、Blob の統計を見る |
| flush | `forceFlush()` | MemTable を SST 側へ flush |
| sync | `forceSync()` | durable state を明示同期 |
| blob GC | `runBlobGc()` | 未参照 blob の回収を明示実行 |
| close | `close()` | engine を閉じる |

### 消失訂正 codec

low-level header には `akk/engine/erasure/` 以下の erasure coding helper もあります。

```cpp
#include "akk/engine/erasure/ErasureCodec.hpp"
#include "akk/engine/erasure/ErasureCodecExt.hpp"

using namespace akkaradb::engine::erasure;

const ErasureLayout layout{.dataShards = 6, .parityShards = 3};
auto shards = RsErasureCodec::encode(bytes("payload"), layout);
auto decoded = RsErasureCodec::decode(shards, layout);
auto repaired = RsErasureCodec::repairOne(2, std::span<const ErasureShard>{shards}.subspan(1), layout);

auto ers_shards = ErsCodec::encode(bytes("payload"), layout);
auto recovered = ErsCodec::recover(ers_shards, layout, {1});
```

`XorErasureCodec` は 1 shard 欠損、`DualXorErasureCodec` は 2 parity による最大 2 data shard 欠損、`RsErasureCodec` は systematic Reed-Solomon による任意の `k` 個からの復元を扱います。`ErsCodec` は同じ RS shard layout を使いつつ、既知 erasure だけでなく corruption の検出も扱う別インターフェースです。

## 高レベル API

`AkkaraDB` は `AkkEngine` の上にある薄い facade です。`PackedTable<&T::id>` は entity を BinPack で serialize し、primary key を table name ごとに分離して保存します。

```cpp
#include <akkaradb/AkkaraDB.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

struct User {
    uint64_t id;
    std::string email;
    std::string name;
    uint32_t age;
};

AKKARADB_QUERYABLE(User, id, email, name, age)

int main() {
    auto db = akkaradb::AkkaraDB::open("data", akkaradb::StartupMode::FAST);
    auto users = db->table<&User::id>("users");

    users.put({1, "alice@example.test", "Alice", 30});
    users.put({2, "bob@example.test", "Bob", 25});

    auto alice = users.get(1);
    (void)alice;

    User bob{};
    if (users.getInto(2, bob)) {
        bob.age = 26;
    }

    users.upsert(2, [](User& user) {
        user.name = "Bobby";
        user.age = 26;
    });

    const bool exists = users.exists(1);
    const size_t rows = users.count();
    (void)exists;
    (void)rows;

    users.remove(1);
    db->close();
}
```

primary key は `table<&User::id>("users")` のような member pointer で選びます。`PackedTable` は `put()` 時に entity から primary key を読み取るので、渡す entity には妥当な primary-key field が入っている必要があります。

各 row には encoded primary key とは独立した stable `akkaradb::RowId` も割り当てられます。`rowIdOf(pk)`、`primaryKeyOf(rowId)`、`getByRowId(rowId)` でその対応関係にアクセスできます。

### セカンダリインデックス

`index<&T::field>()` は index handle を返します。`indexed<&T::field>()` は index を登録して table 自身を返すため、chain しやすくなります。

```cpp
auto users = db->table<&User::id>("users");
auto by_email = users.index<&User::email>();
auto by_age = users.index<&User::age>();

users.put({1, "alice@example.test", "Alice", 30});
users.put({2, "bob@example.test", "Bob", 30});

auto found = users.findBy<&User::email>(std::string{"alice@example.test"});

auto age30 = by_age.find(30U);
while (age30.hasNext()) {
    auto entry = age30.next();
    uint64_t id = entry.id;
    User user = entry.value;
    (void)id;
    (void)user;
}
```

index は non-unique です。同じ indexed value を持つ entity が複数あれば、`find()` は複数 row を返します。`findBy()` は登録済み index を使い、最初の一致だけを返します。

### 走査と query helper

table scan は table namespace の範囲だけを対象にします。

```cpp
auto all = users.scanAll();
while (all.hasNext()) {
    auto entry = all.next();
}

auto range = users.scan(10ULL, 100ULL);
while (range.hasNext()) {
    auto entry = range.next();
}
```

query helper は predicate を受け取り、`first()`、`any()`、`count()`、`limit()`、`toVector()` を提供します。

```cpp
auto adults = users
    .query([](auto user) {
        return user.age >= 18;
    })
    .limit(100)
    .toVector();

auto bob = users
    .query([](auto user) {
        return user.email == "bob@example.test";
    })
    .first();

const bool has_senior = users
    .query([](auto user) {
        return user.age > 65;
    })
    .any();
```

field expression を使うには `AKKARADB_QUERYABLE(User, id, email, name, age)` を定義します。expression layer は `==`、`!=`、`>`、`>=`、`<`、`<=`、`&&`、`||`、`!`、`in()`、`notIn()`、`startsWith()`、`contains()`、`like()`、`isNull()`、`isNotNull()` をサポートします。

```cpp
auto selected = users
    .query([](auto user) {
        return user.email.startsWith("alice@") || user.age.in({30U, 31U});
    })
    .toVector();
```

登録済み index があり、predicate を index lookup に落とし込める場合は index scan を使います。そうでなければ table scan にフォールバックし、predicate を C++ 側で評価します。

### Ref、Schema、外部キー、Join

`Ref<T>` は serialized entity には target primary key だけを保存します。table binding に接続された状態なら、dereference 時に database を通じて target entity を lazily resolve します。

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
std::string author_name = post->author->name;

auto joined = posts
    .join<&Post::author>(authors)
    .where([](const Post& post, const Author& author) {
        return post.likes > 0 && author.name == "Alice";
    })
    .toVector();
```

`foreignKey<&Post::author>()` は row 保存前に referenced entity の存在を検証します。Schema の foreign key は `OnDelete` と `OnUpdate` を 1 つずつ持てて、値は `Cascade`、`Restrict`、`SetNull` です。`SetNull` を使うには owner 側 field が `std::optional<...>` である必要があります。`Ref<T>` foreign key は現在、referenced entity の primary key を target とします。任意の non-primary target field を使えるのは plain comparable field のみです。

target primary key を `updatePrimaryKey(oldPk, entity)` で変更したとき、`OnUpdate::Cascade` は owner 側 foreign key を書き換え、`OnUpdate::Restrict` は参照が残っている限り変更を拒否し、`OnUpdate::SetNull` は optional な owner 側参照を `nullopt` にします。`Ref<T>` は stable row id を覚えているため、primary key 書き換え後も同じ logical entity を指し続けられます。

join は `Ref<T>` field に限りません。互換性のある plain field 同士でも join できます。

```cpp
struct PlainPost {
    uint64_t id;
    uint64_t authorId;
    std::string body;
    uint32_t likes;
};

AKKARADB_QUERYABLE(PlainPost, id, authorId, body, likes)

auto plain_posts = db->table<&PlainPost::id>("plain_posts");
auto joined_by_id = plain_posts.join<&PlainPost::authorId, &Author::id>(authors).toVector();
```

stable row-id helper は `PackedTable` に用意されています。

```cpp
if (auto row_id = authors.rowIdOf(1ULL)) {
    auto same_author = authors.getByRowId(*row_id);
    auto current_pk = authors.primaryKeyOf(*row_id);
    (void)same_author;
    (void)current_pk;
}
```

persisted entity の field は `akkaradb::Immutable<T>`、または短い alias の `akkaradb::Const<T>` で immutable にできます。

```cpp
struct ExternalAuthor {
    uint64_t id;
    akkaradb::Const<std::string> externalId;
    std::string name;
};
```

immutable field は row を永続化する前なら代入できますが、`put()` または `get()` 後は sealed され、以後の変更は `std::runtime_error` になります。primary-key field 自体には `Immutable<T>` を使えません。

`PackedTable::onUpdate<&Field>(handler)` は、既存 row を置き換えるときに watched field が変化していた場合だけ走る local hook です。

```cpp
posts.onUpdate<&Post::title>([](const std::string& old_value, std::string& new_value) {
    if (new_value.empty()) { new_value = old_value; }
});

posts.onUpdate<&Post::body>([](const Post& old_entity, Post& new_entity) {
    if (old_entity.body != new_entity.body) { new_entity.likes = 0; }
});
```

handler の形は `(oldField, Field& newField)`、`(oldField, Field& newField, oldEntity, Entity& newEntity)`、`(oldEntity, Entity& newEntity)`、および read-only な `newField` / `newEntity` 版をサポートします。

### エラーハンドリング

public Native API は zero-exception API ではありません。期待される「存在しない」は return value で表し、不正な使い方や storage operation failure は exception で表します。

`get()`、`getAt()`、typed `PackedTable::get()` は値が存在しないときに `std::nullopt` を返します。`getInto()`、`getIntoArena()`、typed `getInto()` は同じケースで `false` を返します。空の scan、query、join、history は空の iterator または vector になります。

closed engine へのアクセス、invalid configuration、利用できない API backend、unsafe な cluster transport 設定、I/O failure、persisted data の破損、CRC mismatch、detached `Ref<T>` の dereference、foreign-key target の欠落、永続化後 immutable field の書き換え、未登録 index に対する `findBy()` は、通常 `std::runtime_error` または `std::invalid_argument` を投げます。typed scan/query/index range は `hasNext()` を確認してから `next()` を呼ぶ前提で、枯渇後に `next()` を呼ぶと `std::out_of_range` です。

## StartupMode と Options

high-level API では `StartupMode` preset で代表的な durability profile を選べます。

| Mode | WAL | close 時の flush/sync | version history | 主な用途 |
|---|---|---|---|---|
| `ULTRA_FAST` | Off | なし | Off | test、一時データ、in-memory cache |
| `FAST` | Async | あり | Off | 高スループット ingestion |
| `NORMAL` | Async | あり | Off | 一般用途 |
| `DURABLE` | Sync | あり | On | audit log、強い耐久性が必要な用途 |

より細かく制御したい場合は `AkkaraDB::Options` を使います。

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

low-level API では `AkkEngineOptions` を直接設定します。component を個別に無効化できるので、in-memory test や subsystem benchmark にも向いています。

```cpp
akkaradb::engine::AkkEngineOptions opts;
opts.components.walEnabled = false;
opts.components.blobEnabled = false;
opts.components.manifestEnabled = false;
opts.components.sstEnabled = false;

auto db = akkaradb::engine::AkkEngine::open(std::move(opts));
```

## API の選び方

application code が typed entity を中心に動くなら、まず high-level API を使うのが自然です。table namespace、primary-key encoding、BinPack serialization、secondary-index maintenance が database API の内側に閉じるため、呼び出し側のコードが読みやすくなります。

storage engine の挙動を直接検証したい場合、custom binary key layout が必要な場合、既存の protocol / JNI / server layer が byte buffer を前提にしている場合は low-level API が向いています。low-level API では schema と key design を caller が管理するので、production に入れる前に prefix layout と serialization format を早めに固定しておくほうが安全です。

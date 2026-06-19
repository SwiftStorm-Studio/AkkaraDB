# AkkaraDB Native API 使用方法

この文書は native C++ 側の主要 API の使い方を、現行コードに合わせてまとめたものです。低レベル API は `akkaradb::engine::AkkEngine` を直接使う byte-oriented な API で、キーと値を `std::span<const uint8_t>` として扱います。高レベル API は `akkaradb::AkkaraDB` と `akkaradb::PackedTable` を使う typed table API で、C++ aggregate を BinPack で保存し、primary key、secondary index、query、join、`Ref<T>` を扱えます。

## ビルドと include

インストール済みの AkkaraDB を使う場合は CMake target を link します。

```cmake
find_package(AkkaraDB CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE AkkaraDB::akkaradb)
target_compile_features(my_app PRIVATE cxx_std_23)
```

高レベル API は通常この include だけで使えます。

```cpp
#include <akkaradb/AkkaraDB.hpp>
```

低レベル engine を直接使う場合は次の header を include します。

```cpp
#include "akk/engine/AkkEngine.hpp"
```

## 低レベル API

`AkkEngine` は raw key/value API です。schema や typed entity は解釈せず、呼び出し側が key layout と serialize / deserialize を管理します。

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

    auto value = db->get(bytes("user:1"));

    std::vector<uint8_t> out;
    bool found = db->getInto(bytes("user:2"), out);

    bool exists = db->exists(bytes("user:1"));
    size_t count = db->count(bytes("user:"), bytes("user;"));

    db->remove(bytes("user:1"));
    db->forceSync();
    db->close();

    (void)value;
    (void)found;
    (void)exists;
    (void)count;
}
```

`get()` は `std::optional<std::vector<uint8_t>>` を返します。hot path で allocation を抑えたい場合は呼び出し側の `std::vector<uint8_t>` を再利用する `getInto()`、arena lifetime の view として読みたい場合は `getIntoArena()` を使います。

```cpp
akkaradb::core::BufferArena arena;
std::span<const uint8_t> view;
if (db->getIntoArena(bytes("user:2"), arena, view)) {
    std::vector<uint8_t> owned{view.begin(), view.end()};
}
```

### Scan

range scan は caller-owned の `BufferArena` を受け取り、`ArenaGenerator<ScanRecordView>` を返します。返される key/value view は arena の lifetime 中だけ有効です。

```cpp
akkaradb::core::BufferArena arena;
auto rows = db->scan(arena, bytes("user:"), bytes("user;"));

for (auto it = rows.begin(); !(it == rows.end()); ++it) {
    std::span<const uint8_t> key = it->key;
    std::span<const uint8_t> value = it->value;
    (void)key;
    (void)value;
}
```

### Version History

VersionLog を有効にすると、履歴、point-in-time read、rollback が使えます。

```cpp
db->put(bytes("profile:1"), bytes("v1"));
db->put(bytes("profile:1"), bytes("v2"));

auto history = db->history(bytes("profile:1"));
if (!history.empty()) {
    auto oldValue = db->getAt(bytes("profile:1"), history.front().seq);
    db->rollbackKey(bytes("profile:1"), history.front().seq);
    (void)oldValue;
}
```

### Main Low-Level Operations

主な低レベル API は `put`、`putHinted`、`putBatch`、`remove`、`removeHinted`、`get`、`getBatch`、`exists`、`getInto`、`getIntoArena`、`count`、`scan`、`getAt`、`history`、`rollbackTo`、`rollbackKey`、`stats`、`forceSync`、`forceFlush`、`close` です。

## 高レベル API

`AkkaraDB` は `AkkEngine` の上に typed table を作る facade です。`PackedTable<&T::id>` は entity を BinPack で serialize し、table name ごとに primary key namespace を分けます。

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

int main() {
    auto db = akkaradb::AkkaraDB::open("data", akkaradb::StartupMode::FAST);
    auto users = db->table<&User::id>("users");

    users.put({1, "alice@example.test", "Alice", 30});
    users.put({2, "bob@example.test", "Bob", 25});

    auto alice = users.get(1);

    User bob{};
    if (users.getInto(2, bob)) {
        bob.age = 26;
    }

    users.upsert(2, [](User& user) {
        user.name = "Bobby";
        user.age = 26;
    });

    bool exists = users.exists(1);
    size_t rows = users.count();

    users.remove(1);
    db->close();

    (void)alice;
    (void)exists;
    (void)rows;
}
```

`AKKARADB_QUERYABLE(Type, A, B, C, D)` は query proxy を作る macro です。現行 macro は 4 field 分を受け取ります。`AKKARADB_ENTITY(Type, PrimaryKey, B, C, D)` は `AKKARADB_QUERYABLE` に加えて `Ref<T>` 用の `RefTraits<T>` も定義します。

### Secondary Indexes

`index<&T::field>()` は index handle を返し、`indexed<&T::field>()` は table に index を登録して同じ table を返します。index は non-unique なので、同じ field value を持つ entity が複数あれば `find()` は複数行を返します。

```cpp
auto users = db->table<&User::id>("users");
auto byEmail = users.index<&User::email>();
users.indexed<&User::age>();

users.put({1, "alice@example.test", "Alice", 30});
users.put({2, "bob@example.test", "Bob", 30});

auto first = users.findBy<&User::email>(std::string{"alice@example.test"});

auto age30 = users.index<&User::age>().find(30U);
while (age30.hasNext()) {
    auto entry = age30.next();
    uint64_t id = entry.id;
    User user = entry.value;
    (void)id;
    (void)user;
}

(void)byEmail;
(void)first;
```

indexed field の保存形式は query range に使えるよう調整されています。整数と浮動小数は bytewise order が値の順序に合う sortable encoding を使い、それ以外は BinPack encoding を使います。

### Scans And Query Helpers

table scan は table namespace の中だけを対象にします。

```cpp
auto all = users.scanAll();
while (all.hasNext()) {
    auto entry = all.next();
    (void)entry;
}

auto range = users.scan(10ULL, 100ULL);
while (range.hasNext()) {
    auto entry = range.next();
    (void)entry;
}
```

query helper は predicate を受け取り、`first()`、`any()`、`count()`、`limit()`、`toVector()` で結果を扱えます。

```cpp
auto adults = users
    .query([](auto user) {
        return user.age >= 18;
    })
    .limit(100)
    .toVector();

auto bob = users
    .query()
    .where([](auto user) {
        return user.email == "bob@example.test";
    })
    .first();

bool hasSenior = users
    .query([](auto user) {
        return user.age > 65;
    })
    .any();
```

predicate では `==`、`!=`、`>`、`>=`、`<`、`<=`、`&&`、`||`、`!`、`in()`、`notIn()`、`startsWith()`、`contains()`、`like()`、`isNull()`、`isNotNull()` が使えます。

```cpp
auto selected = users
    .query([](auto user) {
        return user.email.startsWith("alice@") || user.age.in({30U, 31U});
    })
    .toVector();
```

`std::optional<T>` は null 判定に使えます。ネストした aggregate field は `.field<&Nested::member>()`、map は `.mapGet(key)` または `.get(key)` で参照します。

```cpp
struct Profile {
    uint64_t id;
    std::string email;
    std::string name;
    std::optional<uint32_t> age;
};

AKKARADB_QUERYABLE(Profile, id, email, name, age)

auto missingAge = profiles.query([](auto profile) {
    return profile.age.isNull();
}).toVector();
```

登録済み index があり、predicate が index source に変換できる形なら、query planner は index scan を使ってから C++ 側で残りの predicate を評価します。対応しない形は table scan に fallback します。

### Ref, Schema, Foreign Keys, And Joins

`Ref<T>` は参照先の primary key だけを保存し、binding が attached されていれば dereference 時に lazily resolve します。

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
std::string authorName = post->author->name;

auto joined = posts
    .join<&Post::author>(authors)
    .where([](const Post& post, const Author& author) {
        return post.likes > 0 && author.name == "Alice";
    })
    .toVector();
```

`foreignKey<&Post::author>()` は保存前に参照先の存在を検証します。schema helper の `foreignKey` は現在 `OnDelete::Cascade` を使うため、参照先を削除すると参照元も cascade delete されます。schema を使わずに table 同士を手動で結びたい場合は `bindRef<&Post::author>(authors)`、cascade だけを手動登録したい場合は `cascadeDeleteFrom<&Post::author>(posts)` を使えます。

`join` は `Ref<T>` だけでなく、比較可能な field 同士にも使えます。

```cpp
struct PlainPost {
    uint64_t id;
    uint64_t authorId;
    std::string body;
    uint32_t likes;
};

AKKARADB_QUERYABLE(PlainPost, id, authorId, body, likes)

auto plainPosts = db->table<&PlainPost::id>("plain_posts");
auto joinedById = plainPosts.join<&PlainPost::authorId, &Author::id>(authors).toVector();
```

`foreignKey<&Post::author>()` は参照先 entity の存在を保存前に検証します。schema の foreign key は `OnDelete` と `OnUpdate` をそれぞれ 1 つずつ指定でき、値は `Cascade` / `Restrict` / `SetNull` です。`SetNull` は owner 側 field が `std::optional<...>` のときだけ使えます。`Ref<T>` の foreign key は現状では参照先 primary key を target にする場合に限られ、任意 field を target にできるのは plain comparable field 側です。

参照先 primary key を変更するときは `updatePrimaryKey(oldPk, entity)` を使います。`OnUpdate::Cascade` は owner 側 foreign key 値を書き換え、`OnUpdate::Restrict` は参照が残っている間その変更を拒否し、`OnUpdate::SetNull` は optional な owner 側参照を `nullopt` にします。`Ref<T>` は内部で stable な `RowId` も覚えるので、cascade 後も同じ logical entity を追跡できます。

stable row identity 用に `PackedTable` には次の helper があります。

```cpp
if (auto rowId = authors.rowIdOf(1ULL)) {
    auto sameAuthor = authors.getByRowId(*rowId);
    auto currentPk = authors.primaryKeyOf(*rowId);
    (void)sameAuthor;
    (void)currentPk;
}
```

永続化後に書き換えさせたくない field は `akkaradb::Immutable<T>` または短い alias の `akkaradb::Const<T>` で表せます。

```cpp
struct ExternalAuthor {
    uint64_t id;
    akkaradb::Const<std::string> externalId;
    std::string name;
};
```

これらの field は保存前は代入できますが、`put()` / `get()` 後は sealed 状態になり、値変更は `std::runtime_error` になります。primary key field 自体に `Immutable<T>` は使えません。

`PackedTable::onUpdate<&Field>(handler)` は既存 row を上書きするとき、監視 field が変わっていた場合に local hook を実行します。hook は new entity をその場で変更できます。

```cpp
posts.onUpdate<&Post::title>([](const std::string& oldValue, std::string& newValue) {
    if (newValue.empty()) { newValue = oldValue; }
});

posts.onUpdate<&Post::body>([](const Post& oldEntity, Post& newEntity) {
    if (oldEntity.body != newEntity.body) { newEntity.likes = 0; }
});
```

受け付ける signature は `(oldField, Field& newField)`、`(oldField, Field& newField, oldEntity, Entity& newEntity)`、`(oldEntity, Entity& newEntity)` と、その read-only 版です。

### Erasure Codecs

low-level header には erasure coding utility もあります。

```cpp
#include "akk/engine/erasure/ErasureCodec.hpp"
#include "akk/engine/erasure/ErasureCodecExt.hpp"

using namespace akkaradb::engine::erasure;

const ErasureLayout layout{.dataShards = 6, .parityShards = 3};
auto shards = RsErasureCodec::encode(bytes("payload"), layout);
auto decoded = RsErasureCodec::decode(shards, layout);

auto ersShards = ErsCodec::encode(bytes("payload"), layout);
auto recovered = ErsCodec::recover(ersShards, layout, {1});
```

`XorErasureCodec` は 1 shard 欠損復元、`DualXorErasureCodec` は 2 parity で最大 2 data shard 欠損復元、`RsErasureCodec` は systematic RS による erasure recovery です。`ErsCodec` は同じ RS shard layout を使いますが、known erasure だけでなく壊れた shard の検出も行う別 interface です。

### Error Handling

Native API は zero-exception API ではありません。通常の「存在しない」は戻り値で表し、呼び出しの誤りやストレージ処理の失敗は標準例外で表します。

`get()`、`getAt()`、typed `PackedTable::get()` は値が存在しない場合に `std::nullopt` を返します。`getInto()`、`getIntoArena()`、typed `getInto()` は同じ状況で `false` を返します。scan、query、join、history の結果が空の場合は、空の range/vector として扱われます。

一方で、close 済み engine への操作、不正な設定、未利用 backend、危険な cluster transport 設定、I/O 失敗、永続化データの破損、CRC 不一致、detached `Ref<T>` の dereference、存在しない foreign-key target、persisted 後の immutable field 書き換え、未登録 index に対する `findBy()` は `std::runtime_error` または `std::invalid_argument` を投げます。typed scan/query/index range は `hasNext()` を確認してから `next()` を呼ぶ前提で、終端後の `next()` は `std::out_of_range` を投げます。
## StartupMode と Options

高レベル API では `StartupMode` で durability profile を選びます。

| Mode | WAL | close 時の flush/sync | version history | 主な用途 |
|---|---|---|---|---|
| `ULTRA_FAST` | Off | なし | Off | test、一時データ、cache |
| `FAST` | Async | あり | Off | throughput 重視の ingestion |
| `NORMAL` | Async | あり | Off | 通常用途 |
| `DURABLE` | Sync | あり | On | 強い durability や履歴が必要な用途 |

細かい設定を変える場合は `AkkaraDB::Options` を使います。

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

低レベル API では `AkkEngineOptions` を直接設定します。component を個別に off にできるため、純粋な in-memory test や特定 subsystem の benchmark でも使いやすくなっています。

```cpp
akkaradb::engine::AkkEngineOptions opts;
opts.components.walEnabled = false;
opts.components.blobEnabled = false;
opts.components.manifestEnabled = false;
opts.components.sstEnabled = false;

auto db = akkaradb::engine::AkkEngine::open(std::move(opts));
```


## Choosing The Right API

application code ? typed entity ???????????? API ??????table namespace?primary-key encoding?BinPack serialization?secondary-index maintenance ? database API ???????????????? code ???????????

storage engine ?????????????? binary key layout ???????????? protocol / JNI / server layer ? byte buffer ?????????????? API ?????????? API ?? schema ? key design ? caller responsibility ?????????? prefix layout ? serialization format ???????????????

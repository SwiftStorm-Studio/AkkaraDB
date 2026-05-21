# AkkaraDB Native API 使い方

このドキュメントは native C++ 側の低レベル API と高レベル API の使い分けをまとめたものです。

低レベル API は `akkaradb::engine::AkkEngine` を直接使う byte-oriented な API です。キーと値を `std::span<const uint8_t>` として扱い、WAL、SST、Blob、VersionLog、scan、flush、sync などを細かく制御したい場合に向いています。

高レベル API は `akkaradb::AkkaraDB` と `akkaradb::PackedTable` を使う typed table API です。C++ の struct を BinPack で保存し、primary key、secondary index、scan、query helper をテーブル単位で扱いたい場合に向いています。

## ビルドと include

インストール済みの AkkaraDB を使う場合は CMake target を link します。

```cmake
find_package(AkkaraDB REQUIRED)
target_link_libraries(my_app PRIVATE AkkaraDB::akkaradb)
target_compile_features(my_app PRIVATE cxx_std_23)
```

高レベル API だけを使う場合は、通常は次の include で足ります。

```cpp
#include <akkaradb/AkkaraDB.hpp>
```

低レベル API を直接使う場合は engine header を include します。

```cpp
#include "engine/AkkEngine.hpp"
```

## 低レベル API

`AkkEngine` は raw key/value API です。キーや値の schema を engine は解釈しないため、呼び出し側が byte layout、prefix 設計、serialize / deserialize を管理します。

```cpp
#include "engine/AkkEngine.hpp"

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
    opts.paths.data_dir = "data";
    opts.components.version_log_enabled = true;
    opts.wal.sync_mode = engine::wal::WalSyncMode::Async;
    opts.blob.threshold_bytes = 32 * 1024;
    opts.runtime.sst_promote_reads = true;

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
    if (db->get_into(bytes("user:2"), out)) {
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
    db->force_sync();
    db->close();
}
```

`get()` は `std::optional<std::vector<uint8_t>>` を返します。hot path で一時 allocation を抑えたい場合は、呼び出し側の `std::vector<uint8_t>` に書き込む `get_into()` を使います。さらに arena 管理の読み取りが必要な場合は `get_into_arena()` も使えます。

### scan

`scan()` は caller-owned な `BufferArena` を受け取り、`ArenaGenerator<ScanRecordView>` を返します。返される key/value view は arena に依存するため、scan 中は arena を生かしておきます。

```cpp
akkaradb::core::BufferArena arena;
auto rows = db->scan(arena, bytes("user:"), bytes("user;"));

for (auto it = rows.begin(); !(it == rows.end()); ++it) {
    std::span<const uint8_t> key = it->key;
    std::span<const uint8_t> value = it->value;
}
```

scan から返る `key` と `value` は non-owning view です。arena の `reset()`、`clear()`、destruction の後には参照できません。scan 結果を arena の lifetime より長く保持したい場合は、返却直後に own 化します。

AkkaraDB の native buffer として保持する場合は、`BufferView::to_owned()` で `OwnedBuffer` に deep copy する方法を強く推奨します。`OwnedBuffer` は move-only の所有型で、view の lifetime を arena から切り離せるため、API 境界を越えて値を保持する用途に向いています。

```cpp
#include "core/buffer/BufferView.hpp"
#include "core/buffer/OwnedBuffer.hpp"

auto value_bytes = std::as_bytes(row.value);
akkaradb::core::BufferView value_view{value_bytes};
akkaradb::core::OwnedBuffer owned_value = value_view.to_owned();
```

アプリケーション側で `std::vector<uint8_t>` を標準の所有型として使っている場合は、vector への copy でも問題ありません。

```cpp
std::vector<uint8_t> owned_key{row.key.begin(), row.key.end()};
std::vector<uint8_t> owned_value{row.value.begin(), row.value.end()};
```

`start_key` と `end_key` は半開区間として扱う設計です。prefix scan をしたい場合は、次の prefix に相当する end key を呼び出し側で渡します。上の例では `"user:"` から `"user;"` までを scan しています。

### version history

`components.version_log_enabled` を有効にすると、per-key history、point-in-time read、rollback が使えます。

```cpp
db->put(bytes("profile:1"), bytes("v1"));
db->put(bytes("profile:1"), bytes("v2"));

auto history = db->history(bytes("profile:1"));
if (!history.empty()) {
    auto old_value = db->get_at(bytes("profile:1"), history.front().seq);
    db->rollback_key(bytes("profile:1"), history.front().seq);
}
```

`rollback_to()` は engine 全体を指定 sequence に戻す操作です。対象範囲が広いため、運用コードでは実行タイミングと durability 設定を明確にしてから使います。

### low-level API の主な操作

| 操作 | API | 用途 |
|---|---|---|
| write | `put(key, value)` | key/value を保存する |
| write with hint | `put_hinted(key, value, fp64, mini_key)` | 呼び出し側が fingerprint を持っている hot path 用 |
| delete | `remove(key)` | key を削除する |
| read | `get(key)` | optional vector として読む |
| read into buffer | `get_into(key, out)` | 呼び出し側の vector を再利用して読む |
| arena read | `get_into_arena(key, arena, out)` | arena lifetime に紐づく view として読む |
| exists | `exists(key)` | key の存在確認 |
| count | `count(start, end)` | 範囲内の件数を数える |
| scan | `scan(arena, start, end)` | 範囲内の key/value を順に読む |
| stats | `stats()` | engine、WAL、MemTable、SST、Blob の統計を見る |
| flush | `force_flush()` | MemTable を SST 側へ flush する |
| sync | `force_sync()` | 永続化の同期を明示する |
| close | `close()` | engine を閉じる |

## 高レベル API

`AkkaraDB` は `AkkEngine` の上に typed table を作る薄い facade です。`PackedTable<&T::id>` は entity を BinPack で serialize し、table name ごとに primary key namespace を分けます。

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
    if (users.get_into(2, bob)) {
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

primary key は `table<&User::id>("users")` のように member pointer で指定します。`PackedTable` は保存時に entity から primary key を取り出すため、`put()` に渡す entity の primary key field は有効な値を持っている必要があります。

### secondary index

`index<&T::field>()` は index handle を返します。`indexed<&T::field>()` は table に index を登録したうえで同じ table を返すため、chain したい場合に使えます。

```cpp
auto users = db->table<&User::id>("users");
auto by_email = users.index<&User::email>();
auto by_age = users.index<&User::age>();

users.put({1, "alice@example.test", "Alice", 30});
users.put({2, "bob@example.test", "Bob", 30});

auto found = users.find_by<&User::email>(std::string{"alice@example.test"});

auto age30 = by_age.find(30U);
while (age30.has_next()) {
    auto entry = age30.next();
    uint64_t id = entry.id;
    User user = entry.value;
    (void)id;
    (void)user;
}
```

index は non-unique です。同じ field value を持つ entity が複数ある場合、`find()` は複数行を返します。`find_by()` は登録済み index を使って最初の一致を返します。

### scan と query helper

table scan は table namespace の中だけを対象にします。

```cpp
auto all = users.scan_all();
while (all.has_next()) {
    auto entry = all.next();
}

auto range = users.scan(10ULL, 100ULL);
while (range.has_next()) {
    auto entry = range.next();
}
```

query helper は predicate を受け取り、`first()`、`any()`、`count()`、`limit()`、`to_vector()` で結果を扱えます。

```cpp
auto adults = users
    .query([](auto user) {
        return user.age >= 18;
    })
    .limit(100)
    .to_vector();

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

`AKKARADB_QUERYABLE(User, id, email, name, age)` を定義しておくと、query predicate の中で field expression を使えます。`==`、`!=`、`>`、`>=`、`<`、`<=`、`&&`、`||`、`in()`、`not_in()`、`starts_with()`、`contains()`、`like()` が用意されています。

```cpp
auto selected = users
    .query([](auto user) {
        return user.email.starts_with("alice@") || user.age.in({30U, 31U});
    })
    .to_vector();
```

登録済み index があり、predicate が index に変換できる形の場合は index scan が使われます。それ以外の場合は table scan に fallback し、predicate を C++ 側で評価します。

## StartupMode と Options

高レベル API では `StartupMode` でよく使う durability profile を選べます。

| Mode | WAL | close 時の flush/sync | version history | 主な用途 |
|---|---|---|---|---|
| `ULTRA_FAST` | Off | なし | Off | test、一時データ、in-memory cache |
| `FAST` | Async | あり | Off | throughput 重視の ingestion |
| `NORMAL` | Async | あり | Off | 通常用途 |
| `DURABLE` | Sync | あり | On | 監査ログ、強い durability が必要な用途 |

細かい設定を変える場合は `AkkaraDB::Options` を使います。

```cpp
akkaradb::AkkaraDB::Options opts;
opts.data_dir = "data";
opts.mode = akkaradb::StartupMode::FAST;
opts.overrides.memtable_threshold_per_shard = 128ULL << 20;
opts.overrides.version_log_enabled = true;
opts.overrides.sst_codec = akkaradb::Codec::Zstd;
opts.overrides.blob_codec = akkaradb::Codec::Zstd;
opts.overrides.blob_threshold_bytes = 32ULL * 1024ULL;
opts.overrides.sst_promote_reads = true;
opts.overrides.sst_bloom_bits_per_key = 10;
opts.overrides.max_l0_sst_files = 8;

auto db = akkaradb::AkkaraDB::open(std::move(opts));
```

低レベル API では `AkkEngineOptions` を直接設定します。component を個別に off にできるため、純粋な in-memory test や特定 subsystem の benchmark でも使いやすくなっています。

```cpp
akkaradb::engine::AkkEngineOptions opts;
opts.components.wal_enabled = false;
opts.components.blob_enabled = false;
opts.components.manifest_enabled = false;
opts.components.sst_enabled = false;

auto db = akkaradb::engine::AkkEngine::open(std::move(opts));
```

## 使い分け

アプリケーションコードで型付き entity を扱うなら、まず高レベル API を使うのが自然です。table namespace、primary key encoding、BinPack serialization、secondary index の更新を API 側に寄せられるため、呼び出し側のコードが読みやすくなります。

storage engine の挙動を直接検証したい場合、独自の binary key layout を使いたい場合、既存の protocol / JNI / server layer から byte列をそのまま流し込みたい場合は、低レベル API が向いています。低レベル API では schema と key design の責任が呼び出し側に残るため、prefix 設計と serialize format を先に固定しておくと運用しやすくなります。

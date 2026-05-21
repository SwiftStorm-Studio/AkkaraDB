# AkkaraDB Native API Usage

This guide covers the native C++ low-level and high-level APIs.

The low-level API is `akkaradb::engine::AkkEngine`. It is byte-oriented: keys and values are passed as `std::span<const uint8_t>`, and the caller controls byte layout, serialization, scans, durability, and history behavior.

The high-level API is `akkaradb::AkkaraDB` with `akkaradb::PackedTable`. It stores C++ structs through BinPack, scopes keys by table, and provides primary-key operations, secondary indexes, scans, and query helpers.

## Build And Include

When AkkaraDB is installed as a CMake package, link the exported target:

```cmake
find_package(AkkaraDB REQUIRED)
target_link_libraries(my_app PRIVATE AkkaraDB::akkaradb)
target_compile_features(my_app PRIVATE cxx_std_23)
```

For the high-level API, this include is usually enough:

```cpp
#include <akkaradb/AkkaraDB.hpp>
```

For direct low-level engine access, include the engine header:

```cpp
#include "engine/AkkEngine.hpp"
```

## Low-Level API

`AkkEngine` is a raw key/value API. The engine does not interpret your schema, so the caller owns key design, value encoding, and decode logic.

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

`get()` returns `std::optional<std::vector<uint8_t>>`. For hot read paths where you want to reuse caller-owned storage, use `get_into()`. For arena-backed reads, use `get_into_arena()`.

### Scan

`scan()` takes a caller-owned `BufferArena` and returns an `ArenaGenerator<ScanRecordView>`. The returned key/value views depend on the arena, so keep the arena alive for the duration of the scan.

```cpp
akkaradb::core::BufferArena arena;
auto rows = db->scan(arena, bytes("user:"), bytes("user;"));

for (auto it = rows.begin(); !(it == rows.end()); ++it) {
    std::span<const uint8_t> key = it->key;
    std::span<const uint8_t> value = it->value;
}
```

The returned `key` and `value` are non-owning views. They must not be used after the arena is reset, cleared, or destroyed. If scan results need to outlive the arena, make an owning copy immediately.

When retaining data as a native AkkaraDB buffer, prefer `BufferView::to_owned()` strongly. It deep-copies the bytes into an `OwnedBuffer`, a move-only owning type, and cleanly detaches the value lifetime from the arena before it crosses an API boundary.

```cpp
#include "core/buffer/BufferView.hpp"
#include "core/buffer/OwnedBuffer.hpp"

auto value_bytes = std::as_bytes(row.value);
akkaradb::core::BufferView value_view{value_bytes};
akkaradb::core::OwnedBuffer owned_value = value_view.to_owned();
```

Copying into `std::vector<uint8_t>` is also fine when the application already uses vector as its owning byte container.

```cpp
std::vector<uint8_t> owned_key{row.key.begin(), row.key.end()};
std::vector<uint8_t> owned_value{row.value.begin(), row.value.end()};
```

The `start_key` and `end_key` arguments are intended to describe a half-open range. For prefix scans, pass the next prefix as the end key. The example above scans from `"user:"` to `"user;"`.

### Version History

Enable `components.version_log_enabled` to use per-key history, point-in-time reads, and rollback.

```cpp
db->put(bytes("profile:1"), bytes("v1"));
db->put(bytes("profile:1"), bytes("v2"));

auto history = db->history(bytes("profile:1"));
if (!history.empty()) {
    auto old_value = db->get_at(bytes("profile:1"), history.front().seq);
    db->rollback_key(bytes("profile:1"), history.front().seq);
}
```

`rollback_to()` rolls the whole engine back to the requested sequence. Because that has a wider blast radius than `rollback_key()`, production code should call it only with explicit durability and operational expectations.

### Main Low-Level Operations

| Operation | API | Purpose |
|---|---|---|
| write | `put(key, value)` | Store a key/value pair |
| write with hint | `put_hinted(key, value, fp64, mini_key)` | Hot path when the caller already has key fingerprints |
| delete | `remove(key)` | Delete a key |
| read | `get(key)` | Read as an optional vector |
| read into buffer | `get_into(key, out)` | Read into caller-owned vector storage |
| arena read | `get_into_arena(key, arena, out)` | Read as a view tied to arena lifetime |
| exists | `exists(key)` | Check whether a key exists |
| count | `count(start, end)` | Count keys in a range |
| scan | `scan(arena, start, end)` | Iterate key/value pairs in a range |
| stats | `stats()` | Inspect engine, WAL, MemTable, SST, and Blob stats |
| flush | `force_flush()` | Flush MemTable data toward SST storage |
| sync | `force_sync()` | Explicitly synchronize durable state |
| close | `close()` | Close the engine |

## High-Level API

`AkkaraDB` is a small facade over `AkkEngine`. `PackedTable<&T::id>` serializes entities with BinPack and scopes primary keys by table name.

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

The primary key is selected with a member pointer, such as `table<&User::id>("users")`. `PackedTable` reads the primary key from the entity when storing data, so the entity passed to `put()` must contain a valid primary-key field.

### Secondary Indexes

`index<&T::field>()` returns an index handle. `indexed<&T::field>()` registers the index and returns the table, which is useful for chaining.

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

Indexes are non-unique. If multiple entities have the same indexed field value, `find()` returns multiple rows. `find_by()` uses a registered index and returns the first match.

### Scans And Query Helpers

Table scans are scoped to the table namespace.

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

Query helpers accept a predicate and expose `first()`, `any()`, `count()`, `limit()`, and `to_vector()`.

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

Define `AKKARADB_QUERYABLE(User, id, email, name, age)` to use field expressions in query predicates. The expression layer supports `==`, `!=`, `>`, `>=`, `<`, `<=`, `&&`, `||`, `in()`, `not_in()`, `starts_with()`, `contains()`, and `like()`.

```cpp
auto selected = users
    .query([](auto user) {
        return user.email.starts_with("alice@") || user.age.in({30U, 31U});
    })
    .to_vector();
```

When a registered index exists and the predicate can be planned as an index lookup, the query uses an index scan. Otherwise it falls back to a table scan and evaluates the predicate in C++.

## StartupMode And Options

The high-level API provides `StartupMode` presets for common durability profiles.

| Mode | WAL | Flush/sync on close | Version history | Typical use |
|---|---|---|---|---|
| `ULTRA_FAST` | Off | No | Off | Tests, temporary data, in-memory cache |
| `FAST` | Async | Yes | Off | High-throughput ingestion |
| `NORMAL` | Async | Yes | Off | General use |
| `DURABLE` | Sync | Yes | On | Audit logs and strict durability |

Use `AkkaraDB::Options` for finer control.

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

The low-level API uses `AkkEngineOptions` directly. Components can be disabled individually, which is useful for in-memory tests and subsystem benchmarks.

```cpp
akkaradb::engine::AkkEngineOptions opts;
opts.components.wal_enabled = false;
opts.components.blob_enabled = false;
opts.components.manifest_enabled = false;
opts.components.sst_enabled = false;

auto db = akkaradb::engine::AkkEngine::open(std::move(opts));
```

## Choosing The Right API

Use the high-level API first when application code works with typed entities. It keeps table namespaces, primary-key encoding, BinPack serialization, and secondary-index maintenance close to the database API, which makes calling code easier to read.

Use the low-level API when you are testing storage-engine behavior directly, when you need a custom binary key layout, or when an existing protocol, JNI layer, or server layer already works with byte buffers. With the low-level API, schema and key design remain caller responsibilities, so it is worth fixing prefix layout and serialization format before production use.

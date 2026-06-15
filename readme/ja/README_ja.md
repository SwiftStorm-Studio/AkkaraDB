# AkkaraDB

**WAL、LSM ストレージ、型付きテーブル、JNI アクセス、任意のクラスタリングを備えた低レイテンシ組み込み KV エンジンです。**

> C++23 | LSM-tree | WAL | SST | Blob store | Version history | HTTP/TCP API | JNI | AGPL-3.0

---

## 特徴

| 分類           | 内容                                                                          |
|--------------|-----------------------------------------------------------------------------|
| Storage      | multi-shard MemTable、WAL、SST levels、Bloom filter、leveled compaction         |
| Durability   | CRC32C 付き record、sync/async WAL、manifest による SST lifecycle 管理               |
| Large values | 大きな値を blob payload に外出しし、MemTable には 20-byte BlobRef を保存                    |
| Compression  | SST と blob payload に Zstandard を利用可能。codec は file metadata に保持              |
| Reads        | MemTable を先に見て、必要に応じて SST / BlobManager へ fallback                          |
| History      | 任意の per-key version log、point-in-time read、global/per-key rollback          |
| API servers  | HTTP REST と binary TCP backend。default port は 7070 / 7071                   |
| Typed tables | `PackedTable<&T::id>` による BinPack serialization、scan、helper、secondary index |
| JVM bridge   | public low-level API は `ByteBufferL` ベース。JNI 経由で native engine を利用          |
| Clustering   | standalone、mirror、stripe replication 用の設定 primitive                         |
| Portability  | Windows/MSVC と Linux/GCC/Clang                                              |

---

## Quick Start

### Build

```cmake
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

よく使う CMake flag:

```cmake
-DBUILD_SHARED_LIBS=ON        # shared library を build。default ON
-DBUILD_SHARED_LIBS=OFF       # static library を build
-DAKKARADB_BUILD_TESTS=ON     # tests/ を追加
-DAKKARADB_BUILD_JNI=ON       # JVM wrapper 用の akkaradb_jni を build
```

現状の native CMake configuration では TLS と SIMD は常に有効です。mbedTLS を link し、対応 compiler では SSE4.2/AVX2 の compile flag が追加されます。

Windows では Visual Studio/MSVC 環境から build してください。compiler、linker、Windows SDK が初期化された状態で実行する必要があります。

### Install Target

```cmake
find_package(AkkaraDB REQUIRED)
target_link_libraries(my_app PRIVATE AkkaraDB::akkaradb)
```

---

## C++ Low-Level API

key/value の byte 列、scan、durability、history を直接制御したい場合は `AkkEngine` を使います。

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
    // optional allocation を避ける hot read path
}

engine->remove(bytes("user:1"));
engine->forceSync();
engine->close();
```

range scan は caller 側で arena を持ちます。

```cpp
akkaradb::core::BufferArena arena;
auto rows = engine->scan(arena, bytes("user:"), bytes("user;"));

for (auto it = rows.begin(); !(it == rows.end()); ++it) {
    auto key = it->key;
    auto value = it->value;
}
```

`versionLogEnabled` が有効な場合は version history も使えます。

```cpp
auto at = engine->getAt(bytes("user:1"), target_seq);
auto history = engine->history(bytes("user:1"));

engine->rollbackKey(bytes("user:1"), target_seq);
engine->rollbackTo(target_seq);
```

---

## C++ High-Level API

型付き entity、automatic BinPack serialization、table-scoped key を使いたい場合は `AkkaraDB` と `PackedTable` を使います。

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

typed table の key は table ごとに namespace 化されます。primary key layout は 8-byte FNV-1a table prefix と encoded primary key です。secondary index は
`table_name + ":idx:" + field_name` を namespace として使います。

---

## JVM Low-Level API

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

`ByteBufferL.allocate(capacity, direct = true)` は little-endian の direct buffer を作ります。test や heap-backed input では `ByteBufferL.wrap(ByteArray)` が使えます。

---

## JVM High-Level API

Kotlin entity を扱う場合は `AkkaraDB.table<T, ID>()` を使います。JVM high-level table は C++ と同じ table namespace / index key layout に合わせています。

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

`runQ(AkkQuery(...))` を使うと query AST を手で組んで実行できます。`query { ... }`、`firstOrNull { ... }`、`runToList { ... }` などの DSL function は compiler
plugin による rewrite が前提です。plugin が適用されていない場合、runtime では意図的に失敗します。

---

## Startup Modes

| Mode         | WAL   | close behavior       | version history | 主な用途                       |
|--------------|-------|----------------------|-----------------|----------------------------|
| `ULTRA_FAST` | Off   | forced flush/sync なし | Off             | in-memory cache、test、一時データ |
| `FAST`       | Async | close 時に flush/sync  | Off             | high-throughput ingestion  |
| `NORMAL`     | Async | close 時に flush/sync  | Off             | general-purpose default    |
| `DURABLE`    | Sync  | close 時に flush/sync  | On              | audit log、強い durability    |

C++ と JVM の両方で細かい override ができます。

| Override                     | C++                            | JVM                         |
|------------------------------|--------------------------------|-----------------------------|
| MemTable threshold per shard | `memtableThresholdPerShard` | `memtableThresholdPerShard` |
| Version log                  | `versionLogEnabled`          | `versionLogEnabled`         |
| SST codec                    | `sstCodec`                    | `sstCodec`                  |
| Blob codec                   | `blobCodec`                   | `blobCodec`                 |
| Blob threshold               | `blobThresholdBytes`         | `blobThresholdBytes`        |
| Promote SST reads            | `sstPromoteReads`            | `sstPromoteReads`           |
| Bloom bits per key           | `sstBloomBitsPerKey`       | `sstBloomBitsPerKey`        |
| Max L0 SST files             | `maxL0SstFiles`             | `maxL0SstFiles`             |

---

## Architecture Overview

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

SST read は Bloom filter と sparse block index を使います。`sstPromoteReads` が有効な場合、SST hit を MemTable に戻すことができます。

---

## Configuration Highlights

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

native configuration の詳細は [SPEC.md section 14](../../SPEC.md#14-configuration-reference) を参照してください。

---

## API Servers

`components.apiEnabled` を有効にすると、internal engine は HTTP と TCP API backend を起動できます。

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

HTTP endpoints:

| Method   | Path                                            |
|----------|-------------------------------------------------|
| `GET`    | `/v1/ping`                                      |
| `POST`   | `/v1/put?key=<percent-encoded>`                 |
| `GET`    | `/v1/get?key=<percent-encoded>`                 |
| `DELETE` | `/v1/remove?key=<percent-encoded>`              |
| `GET`    | `/v1/getAt?key=<percent-encoded>&seq=<number>` |

binary TCP protocol は `AK5Q` request frame と `AK5S` response frame を使います。frame layout は [SPEC.md section 11.2](../../SPEC.md#112-binary-protocol-v2)
にあります。

---

## File Layout

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

## Benchmarks And Smoke Tests

native CMake file は benchmark と smoke test executable を `build/bin` に生成します。

```powershell
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release

.\build\bin\akkaradb_akkengine_smoke_test.exe
.\build\bin\akkaradb_typed_api_smoke_test.exe
.\build\bin\akkaradb_benchmark.exe
```

benchmark source は以下にあります。

```text
benchmarks/suite/
benchmarks/smoke/
benchmarks/throughput/
benchmarks/api/
```

---

## Dependencies

| Library   | Version      | Notes                                     |
|-----------|--------------|-------------------------------------------|
| Zstandard | 1.5.6        | SST と blob payload の compression          |
| Boost.PFR | boost-1.84.0 | BinPack 用 aggregate reflection            |
| mbedTLS   | 3.6.2        | API / replication transport の TLS support |

dependency は CMake の `FetchContent` で取得されます。

---

## Specification

現在の native specification は [SPEC.md](../../SPEC.md) です。record layout、WAL/SST/blob/manifest format、API framing、configuration、concurrency、recovery behavior
をまとめています。

---

## Architecture

native engine ? subsystem?data flow?recovery?concurrency model ? [readme/ja/ARCHITECTURE_ja.md](ARCHITECTURE_ja.md) ?????????

---

## Native API Usage

???? `AkkEngine` API ????? typed table API ????? [readme/ja/API_USAGE_ja.md](API_USAGE_ja.md) ?????????

---

## License

AkkaraDB は GNU Affero General Public License v3.0 で提供される free software です。

Copyright (C) 2026 Swift Storm Studio.

全文は [LICENSE](../../LICENSE) を参照してください。

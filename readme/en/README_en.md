# AkkaraDB

**A low-latency embedded KV engine with WAL, LSM storage, typed tables, JNI access, and optional clustering.**

> C++23 | LSM-tree | WAL | SST | Blob store | Version history | HTTP/TCP API | JNI | AGPL-3.0

---

## Features

| Category     | Capability                                                                              |
|--------------|-----------------------------------------------------------------------------------------|
| Storage      | Multi-shard MemTable, WAL, SST levels, Bloom filters, leveled compaction                |
| Durability   | CRC32C-protected records, sync or async WAL, manifest-backed SST lifecycle              |
| Large values | Automatic blob externalization with 20-byte blob references                             |
| Compression  | Zstandard for SST and blob payloads, with per-file codec metadata                       |
| Reads        | MemTable-first lookup, SST fallback, optional SST read promotion                        |
| History      | Optional per-key version log, point-in-time read, global and per-key rollback           |
| API servers  | HTTP REST and binary TCP backends, default ports 7070 and 7071                          |
| Typed tables | `PackedTable<&T::id>` with BinPack serialization, scans, helpers, and secondary indexes |
| JVM bridge   | Kotlin/JVM wrapper over JNI using `ByteBufferL` at the public low-level API             |
| Clustering   | Standalone, mirror, and stripe replication configuration primitives                     |
| Portability  | Windows/MSVC and Linux/GCC/Clang                                                        |

---

## Quick Start

### Build

```cmake
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

Common flags:

```cmake
-DBUILD_SHARED_LIBS=ON        # Build shared library, default ON
-DBUILD_SHARED_LIBS=OFF       # Build static library
-DAKKARADB_BUILD_TESTS=ON     # Add tests/
-DAKKARADB_BUILD_JNI=ON       # Build akkaradb_jni for the JVM wrapper
```

TLS and SIMD are currently enabled by the native CMake configuration. The build links mbedTLS and adds SSE4.2/AVX2 compiler flags on supported compilers.

On Windows, build from a Visual Studio/MSVC environment so the compiler, linker, and Windows SDK are initialized.

### Install Target

```cmake
find_package(AkkaraDB REQUIRED)
target_link_libraries(my_app PRIVATE AkkaraDB::akkaradb)
```

---

## C++ Low-Level API

Use `AkkEngine` directly when you want byte-oriented control over keys, values, scans, durability, and history.

```cpp
#include "engine/AkkEngine.hpp"

#include <span>
#include <string_view>

using namespace akkaradb::engine;

std::span<const uint8_t> bytes(std::string_view s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

AkkEngineOptions opts;
opts.paths.data_dir = "data";
opts.components.version_log_enabled = true;
opts.wal.sync_mode = wal::WalSyncMode::Async;
opts.blob.threshold_bytes = 16 * 1024;
opts.runtime.sst_promote_reads = true;

auto engine = AkkEngine::open(std::move(opts));

engine->put(bytes("user:1"), bytes("Alice"));

auto value = engine->get(bytes("user:1"));
if (value) {
    // value is std::vector<uint8_t>
}

std::vector<uint8_t> out;
if (engine->get_into(bytes("user:1"), out)) {
    // hot read path without optional allocation
}

engine->remove(bytes("user:1"));
engine->force_sync();
engine->close();
```

Range scans use a caller-owned arena:

```cpp
akkaradb::core::BufferArena arena;
auto rows = engine->scan(arena, bytes("user:"), bytes("user;"));

for (auto it = rows.begin(); !(it == rows.end()); ++it) {
    auto key = it->key;
    auto value = it->value;
}
```

Version history is available when `version_log_enabled` is on:

```cpp
auto at = engine->get_at(bytes("user:1"), target_seq);
auto history = engine->history(bytes("user:1"));

engine->rollback_key(bytes("user:1"), target_seq);
engine->rollback_to(target_seq);
```

---

## C++ High-Level API

Use `AkkaraDB` and `PackedTable` when you want typed entities with automatic BinPack serialization and table-scoped keys.

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
if (users.get_into(2ULL, out)) {
    // out contains Bob
}

auto age30 = by_age.find(30U);
while (age30.has_next()) {
    auto entry = age30.next();
    auto id = entry.id;
    auto user = entry.value;
}

auto adults = users
    .query([](const User& user) { return user.age >= 18; })
    .limit(100)
    .to_vector();

auto first_bob = users.find_by<&User::email>(std::string{"bob@example.test"});

users.upsert(2ULL, [](User& user) {
    user.name = "Bobby";
});

users.remove(1ULL);
db->close();
```

Typed table keys are table-scoped. The primary key layout is an 8-byte FNV-1a table prefix followed by the encoded primary key. Secondary indexes use
`table_name + ":idx:" + field_name` as their namespace.

---

## JVM Low-Level API

The JVM wrapper exposes low-level byte access through `ByteBufferL`, not raw `ByteArray`, at the public API boundary.

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

`ByteBufferL.allocate(capacity, direct = true)` creates a little-endian direct buffer. `ByteBufferL.wrap(ByteArray)` is useful for heap-backed inputs and tests.

---

## JVM High-Level API

Use `AkkaraDB.table<T, ID>()` for Kotlin entities. The JVM high-level table mirrors the C++ table namespace and index key layout.

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

Manual query execution is available with `runQ(AkkQuery(...))`. The DSL functions such as `query { ... }`, `firstOrNull { ... }`, and `runToList { ... }` are
intended to be rewritten by the compiler plugin; without the plugin they fail at runtime by design.

---

## Startup Modes

| Mode         | WAL   | Close behavior          | Version history | Typical use                            |
|--------------|-------|-------------------------|-----------------|----------------------------------------|
| `ULTRA_FAST` | Off   | No forced flush or sync | Off             | In-memory cache, tests, temporary data |
| `FAST`       | Async | Flush/sync on close     | Off             | High-throughput ingestion              |
| `NORMAL`     | Async | Flush/sync on close     | Off             | General-purpose default                |
| `DURABLE`    | Sync  | Flush/sync on close     | On              | Audit logs and strict durability       |

Fine-grained overrides are available from both C++ and JVM:

| Override                     | C++                            | JVM                         |
|------------------------------|--------------------------------|-----------------------------|
| MemTable threshold per shard | `memtable_threshold_per_shard` | `memtableThresholdPerShard` |
| Version log                  | `version_log_enabled`          | `versionLogEnabled`         |
| SST codec                    | `sst_codec`                    | `sstCodec`                  |
| Blob codec                   | `blob_codec`                   | `blobCodec`                 |
| Blob threshold               | `blob_threshold_bytes`         | `blobThresholdBytes`        |
| Promote SST reads            | `sst_promote_reads`            | `sstPromoteReads`           |
| Bloom bits per key           | `sst_bloom_bits_per_key`       | `sstBloomBitsPerKey`        |
| Max L0 SST files             | `max_l0_sst_files`             | `maxL0SstFiles`             |

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

Read path:

```text
MemTable -> SSTManager -> BlobManager when the value is externalized
```

SST reads use Bloom filters and sparse block indexes. When `sst_promote_reads` is enabled, SST hits can be promoted back into the MemTable.

---

## Configuration Highlights

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

For the complete native configuration surface, see [SPEC.md section 14](../../SPEC.md#14-configuration-reference).

---

## API Servers

The internal engine can start HTTP and TCP API backends when `components.api_enabled` is set.

```cpp
AkkEngineOptions opts;
opts.paths.data_dir = "data";
opts.components.api_enabled = true;
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
| `GET`    | `/v1/get_at?key=<percent-encoded>&seq=<number>` |

The binary TCP protocol uses `AK5Q` request frames and `AK5S` response frames. See [SPEC.md section 11.2](../../SPEC.md#112-binary-protocol-v2) for the frame
layout.

---

## File Layout

```text
{data_dir}/
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

The native CMake file builds the benchmark and smoke executables under `build/bin`.

```powershell
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release

.\build\bin\akkaradb_akkengine_smoke_test.exe
.\build\bin\akkaradb_typed_api_smoke_test.exe
.\build\bin\akkaradb_benchmark.exe
```

Available benchmark sources live under:

```text
benchmarks/suite/
benchmarks/smoke/
benchmarks/throughput/
benchmarks/api/
```

---

## Dependencies

| Library   | Version      | Notes                                          |
|-----------|--------------|------------------------------------------------|
| Zstandard | 1.5.6        | Compression for SST and blob payloads          |
| Boost.PFR | boost-1.84.0 | Aggregate reflection for BinPack               |
| mbedTLS   | 3.6.2        | TLS support for API and replication transports |

Dependencies are fetched by CMake through `FetchContent`.

---

## Specification

The current native specification is [SPEC.md](../../SPEC.md). It documents record layouts, WAL/SST/blob/manifest formats, API framing, configuration,
concurrency, and
recovery behavior.

## Architecture

The native architecture overview is [ARCHITECTURE_en.md](ARCHITECTURE_en.md). It explains how the public APIs, `AkkEngine`, MemTable, WAL, Blob Manager, SST
Manager, Manifest, VersionLog, API servers, cluster runtime, and JNI scan path fit together.

## Native API Usage

The native low-level and high-level API guide is [API_USAGE_en.md](API_USAGE_en.md). It covers direct `AkkEngine` usage and the typed `AkkaraDB` / `PackedTable`
API.

---

## License

AkkaraDB is free software licensed under the GNU Affero General Public License v3.0.

Copyright (C) 2026 Swift Storm Studio.

See [LICENSE](../../LICENSE) for the full license text.

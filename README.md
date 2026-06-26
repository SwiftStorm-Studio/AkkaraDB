# AkkaraDB

AkkaraDB is a low-latency embedded key-value database for C++ and Kotlin/JVM applications. It gives you a native storage engine with WAL-backed durability, LSM-based persistence, typed C++ tables, and optional server backends when you need HTTP, TCP, or gRPC access.

## Read This In

| Language | README |
|---|---|
| English | [readme/en/README_en.md](readme/en/README_en.md) |
| Japanese | [readme/ja/README_ja.md](readme/ja/README_ja.md) |

## What You Can Use

Choose the entry point that matches your application:

| Use case | Entry point |
|---|---|
| Raw embedded KV engine | `AkkEngine` |
| Typed C++ tables with indexes and query helpers | `AkkaraDB` and `PackedTable<&T::id>` |
| Kotlin/JVM integration | JNI wrapper with `ByteBufferL` |
| Remote access over HTTP/TCP/gRPC | `akkara/akkserver/` backends |

## Why AkkaraDB

- Embedded-first: use it as a local native database without deploying a separate server.
- Durable by default: WAL, SST storage, manifest tracking, and optional version history are built into the engine.
- Typed C++ model: high-level tables support schema-like helpers such as secondary indexes, joins, `RowId`, immutable persisted fields, and foreign-key actions.
- JVM bridge: Kotlin/JVM code can call the same native engine through JNI.
- Optional server layer: add HTTP, TCP, or gRPC only when your deployment actually needs it.

## Quick Start

Build the native library:

```cmake
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

Build the JNI bridge when you need the JVM wrapper:

```cmake
cmake -B build-jni -DCMAKE_BUILD_TYPE=Release -DAKKARADB_BUILD_JNI=ON
cmake --build build-jni --config Release
```

Minimal typed C++ example:

```cpp
struct User {
    uint64_t id;
    std::string email;
    std::string name;
};

AKKARADB_QUERYABLE(User, id, email, name)

auto db = akkaradb::AkkaraDB::open("data", akkaradb::StartupMode::FAST);
auto users = db->table<&User::id>("users");

users.index<&User::email>();
users.put({1, "alice@example.test", "Alice"});

auto alice = users.get(1ULL);
```

Minimal low-level engine example:

```cpp
#include <string_view>

std::span<const uint8_t> bytes(std::string_view s) {
    return {
        reinterpret_cast<const uint8_t*>(s.data()),
        s.size()
    };
}

akkaradb::engine::AkkEngineOptions opts;
opts.paths.dataDir = "data";

auto engine = akkaradb::engine::AkkEngine::open(std::move(opts));
engine->put(bytes("user:1"), bytes("Alice"));
```

## Documentation

Start here for the API surface:

- [readme/en/API_USAGE_en.md](readme/en/API_USAGE_en.md)
- [readme/ja/API_USAGE_ja.md](readme/ja/API_USAGE_ja.md)

System and format details:

- [readme/en/ARCHITECTURE_en.md](readme/en/ARCHITECTURE_en.md)
- [readme/ja/ARCHITECTURE_ja.md](readme/ja/ARCHITECTURE_ja.md)
- [SPEC.md](SPEC.md)

Benchmark and validation documents:

- [readme/en/BENCHMARK.md](readme/en/BENCHMARK.md)
- `benchmarks/`

## Repository Map

```text
akkara/              Native engine, typed API, JNI, and server backends
benchmarks/          Smoke tests and throughput benchmarks
readme/en/           English documentation
readme/ja/           Japanese documentation
SPEC.md              Native technical specification
```

## License

The native core of AkkaraDB is licensed under the Mozilla Public License 2.0. See [LICENSE](LICENSE).

`akkara/akkserver/` is licensed separately under the GNU Affero General Public License v3.0. See [LICENSE_SERVER](LICENSE_SERVER).

# AkkaraDB

AkkaraDB is a low-latency embedded key-value engine written in C++23. It combines a WAL-backed LSM storage engine, typed C++ tables, a Kotlin/JVM JNI wrapper, optional HTTP/TCP API servers, and the foundation for replicated deployments.

## Read This In

| Language | README |
|---|---|
| English | [readme/en/README_en.md](readme/en/README_en.md) |
| Japanese | [readme/ja/README_ja.md](readme/ja/README_ja.md) |

## What Is Included

| Area | Summary |
|---|---|
| Native engine | `AkkEngine` provides raw byte-oriented put/get/remove/scan APIs with WAL, SST, blob storage, and optional version history. |
| C++ typed API | `AkkaraDB` and `PackedTable<&T::id>` provide BinPack-backed typed tables, secondary indexes, scans, and query helpers. |
| JVM bridge | The Kotlin/JVM layer talks to native code through JNI and exposes `ByteBufferL` for low-level byte access. |
| API servers | The native engine can expose HTTP REST and binary TCP endpoints for basic key/value operations. |
| Architecture | [English](readme/en/ARCHITECTURE_en.md) and [Japanese](readme/ja/ARCHITECTURE_ja.md) architecture notes explain component responsibilities and data flow. |
| Native API usage | [English](readme/en/API_USAGE_en.md) and [Japanese](readme/ja/API_USAGE_ja.md) guides show how to use the low-level `AkkEngine` API and the high-level typed table API. |
| Specification | [SPEC.md](SPEC.md) documents storage formats, API framing, configuration, threading, recovery, and JNI query payloads. |

## Quick Build

```cmake
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

Build the JNI bridge when using the JVM wrapper:

```cmake
cmake -B build-jni -DCMAKE_BUILD_TYPE=Release -DAKKARADB_BUILD_JNI=ON
cmake --build build-jni --config Release
```

## Repository Map

```text
akkara/              Native high-level AkkaraDB and low-level AkkEngine sources
benchmarks/          Smoke tests and throughput benchmarks
readme/en/           English documentation
readme/ja/           Japanese documentation
SPEC.md              Native technical specification
```

## License

AkkaraDB is licensed under the GNU Affero General Public License v3.0. See [LICENSE](LICENSE).

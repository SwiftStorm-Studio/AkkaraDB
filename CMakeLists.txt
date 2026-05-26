cmake_minimum_required(VERSION 4.1)

project(AkkaraDB
        DESCRIPTION "Low-latency, crash-safe JVM KV store with WAL & stripe parity"
        VERSION 2026.1.1
        LANGUAGES CXX
)

# ==================== Build Options ====================
option(BUILD_SHARED_LIBS "Build shared libraries (.so/.dll)" ON)
option(AKKARADB_BUILD_TESTS "Build unit tests" OFF)
option(AKKARADB_BUILD_JNI "Build JNI bridge for JVM integration" OFF)
set(AKKARADB_REVISION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/VERSION_REVISION" CACHE FILEPATH "AkkaraDB release revision counter file")
set(AKKARADB_DETAIL_VERSION_BASE "126.1.1" CACHE STRING "AkkaraDB detailed release version prefix")
set(AKKARADB_PRERELEASE "rc.1" CACHE STRING "AkkaraDB prerelease label used in dist artifact names")
set(AKKARADB_COMPAT_LINE "126.1" CACHE STRING "AkkaraDB native/JNI compatibility line used in library file names")
set(AKKARADB_RELEASE_VERSION "${AKKARADB_DETAIL_VERSION_BASE}-${AKKARADB_PRERELEASE}")

# ==================== C++ Standard ====================
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ==================== Directory Configuration ====================
set(AKKARA_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/akkara)
set(AKKARADB_DIR ${AKKARA_ROOT}/akkaradb)
set(AKKENGINE_DIR ${AKKARA_ROOT}/akkengine)

set(AKKARADB_INCLUDE_DIR ${AKKARADB_DIR}/include)
set(AKKARADB_SRC_DIR ${AKKARADB_DIR}/src)

set(AKKENGINE_INCLUDE_DIR ${AKKENGINE_DIR}/include)
set(AKKENGINE_SRC_DIR ${AKKENGINE_DIR}/src)

set(AKKARADB_RUNTIME_OUTPUT_DIR "${CMAKE_BINARY_DIR}/bin")
set(AKKARADB_TEST_OUTPUT_DIR "${CMAKE_BINARY_DIR}/test")
file(MAKE_DIRECTORY "${AKKARADB_RUNTIME_OUTPUT_DIR}")

string(TOLOWER "${CMAKE_SYSTEM_NAME}" AKKARADB_DIST_OS)
if (AKKARADB_DIST_OS STREQUAL "darwin")
    set(AKKARADB_DIST_OS "macos")
endif ()

string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" AKKARADB_DIST_ARCH)
if (AKKARADB_DIST_ARCH MATCHES "^(amd64|x86_64)$")
    set(AKKARADB_DIST_ARCH "x86_64")
elseif (AKKARADB_DIST_ARCH MATCHES "^(arm64|aarch64)$")
    set(AKKARADB_DIST_ARCH "aarch64")
endif ()

set(AKKARADB_DIST_PLATFORM "${AKKARADB_DIST_OS}-${AKKARADB_DIST_ARCH}")
set(AKKARADB_NATIVE_OUTPUT_NAME "akkaradb-native-${AKKARADB_RELEASE_VERSION}")
set(AKKARADB_JNI_OUTPUT_NAME "akkaradb-jni-${AKKARADB_RELEASE_VERSION}")
if (WIN32)
    set(AKKARADB_NATIVE_DIST_LIBRARY_NAME "akkaradb-native-${AKKARADB_RELEASE_VERSION}.dll")
elseif (APPLE)
    set(AKKARADB_NATIVE_DIST_LIBRARY_NAME "libakkaradb-native-${AKKARADB_RELEASE_VERSION}.dylib")
else ()
    set(AKKARADB_NATIVE_DIST_LIBRARY_NAME "libakkaradb-native-${AKKARADB_RELEASE_VERSION}.so")
endif ()
set(AKKARADB_NATIVE_DIST_DIR "${CMAKE_CURRENT_SOURCE_DIR}/dist/native/${AKKARADB_RELEASE_VERSION}")
set(AKKARADB_NATIVE_PACKAGE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/dist/native")
set(AKKARADB_RELEASE_DIST_DIR "${CMAKE_CURRENT_SOURCE_DIR}/dist/${AKKARADB_RELEASE_VERSION}")
set(AKKARADB_SDK_PACKAGE_NAME "akkaradb-sdk-${AKKARADB_RELEASE_VERSION}-${AKKARADB_DIST_PLATFORM}")
set(AKKARADB_SDK_EXTRACT_DIR "${CMAKE_BINARY_DIR}/sdk-package/${AKKARADB_SDK_PACKAGE_NAME}")
set(AKKARADB_SDK_ZIP "${AKKARADB_RELEASE_DIST_DIR}/${AKKARADB_SDK_PACKAGE_NAME}.zip")
file(MAKE_DIRECTORY "${AKKARADB_NATIVE_DIST_DIR}")
file(MAKE_DIRECTORY "${AKKARADB_RELEASE_DIST_DIR}")

# ==================== Compiler Settings ====================
if (MSVC)
    add_compile_options(/W4 /permissive- /utf-8)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
    if (AKKARADB_BUILD_JNI)
        set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>" CACHE STRING "" FORCE)
    endif ()
else ()
    add_compile_options(-Wall -Wextra -pedantic)
endif ()

# ==================== Windows-specific definitions ====================
if (WIN32)
    add_compile_definitions(NOMINMAX WIN32_LEAN_AND_MEAN)
endif ()

# ==================== Dependencies ====================
include(FetchContent)

# ── Zstd compression ──────────────────────────────────────────────────────────
FetchContent_Declare(
        zstd
        GIT_REPOSITORY https://github.com/facebook/zstd.git
        GIT_TAG v1.5.6
        SOURCE_SUBDIR build/cmake
)
set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_CONTRIB OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)

set(CMAKE_WARN_DEPRECATED OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(zstd)
set(BUILD_SHARED_LIBS ON CACHE BOOL "" FORCE)
set(CMAKE_WARN_DEPRECATED ON CACHE BOOL "" FORCE)

# ── Boost.PFR (header-only) ───────────────────────────────────────────────────
FetchContent_Declare(
        boost_pfr
        GIT_REPOSITORY https://github.com/boostorg/pfr.git
        GIT_TAG boost-1.84.0
)
FetchContent_MakeAvailable(boost_pfr)

# ── mbedTLS (always enabled) ──────────────────────────────────────────────────
FetchContent_Declare(
        mbedtls
        GIT_REPOSITORY https://github.com/Mbed-TLS/mbedtls.git
        GIT_TAG v3.6.2
)
set(ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(ENABLE_PROGRAMS OFF CACHE BOOL "" FORCE)
set(MBEDTLS_FATAL_WARNINGS OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(mbedtls)
set(BUILD_SHARED_LIBS ON CACHE BOOL "" FORCE)

# ==================== Main Library ====================
add_library(akkaradb)

if (MSVC)
    target_compile_options(akkaradb PRIVATE /WX)
else ()
    target_compile_options(akkaradb PRIVATE -Werror)
endif ()
# ====================== Public Headers ======================
file(GLOB_RECURSE PUBLIC_HEADERS CONFIGURE_DEPENDS
        "${AKKARADB_INCLUDE_DIR}/akkaradb/*.hpp"
        "${AKKARADB_INCLUDE_DIR}/akkaradb/*.h"
)

target_sources(akkaradb PUBLIC FILE_SET HEADERS
        BASE_DIRS ${AKKARADB_INCLUDE_DIR}
        FILES ${PUBLIC_HEADERS}
)

# ====================== AkkEngine Headers ======================
file(GLOB_RECURSE AKKENGINE_HEADERS CONFIGURE_DEPENDS
        "${AKKENGINE_INCLUDE_DIR}/*.hpp"
        "${AKKENGINE_INCLUDE_DIR}/*.h"
)

target_sources(akkaradb PUBLIC FILE_SET akkengine_headers TYPE HEADERS
        BASE_DIRS ${AKKENGINE_INCLUDE_DIR}
        FILES ${AKKENGINE_HEADERS}
)

# ====================== Source Files ======================
file(GLOB_RECURSE AKKARADB_SOURCES CONFIGURE_DEPENDS
        "${AKKARADB_SRC_DIR}/*.cpp"
)

file(GLOB_RECURSE AKKENGINE_SOURCES CONFIGURE_DEPENDS
        "${AKKENGINE_SRC_DIR}/*.cpp"
)

target_sources(akkaradb PRIVATE
        ${AKKARADB_SOURCES}
        ${AKKENGINE_SOURCES}
)

add_custom_target(akkaradb_increment_revision
        COMMAND ${CMAKE_COMMAND}
        -DREVISION_FILE=${AKKARADB_REVISION_FILE}
        -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/IncrementRevision.cmake
        COMMENT "Incrementing AkkaraDB build revision"
        VERBATIM
)
add_dependencies(akkaradb akkaradb_increment_revision)

# SIMD-specialized source files are compiled with per-file options so the
# default binary remains portable and runtime dispatch can pick the best path.
set(AKKARADB_CRC32C_AVX2_SOURCE "${AKKENGINE_SRC_DIR}/cpu/crc32c/CRC32CX86AVX2.cpp")
set(AKKARADB_CRC32C_AVX512_SOURCE "${AKKENGINE_SRC_DIR}/cpu/crc32c/CRC32CX86AVX512.cpp")

if (AKKARADB_DIST_ARCH STREQUAL "x86_64")
    if (MSVC)
        set_source_files_properties(${AKKARADB_CRC32C_AVX2_SOURCE}
                PROPERTIES COMPILE_OPTIONS "/arch:AVX2")
        set_source_files_properties(${AKKARADB_CRC32C_AVX512_SOURCE}
                PROPERTIES COMPILE_OPTIONS "/arch:AVX512")
    elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        set_source_files_properties(${AKKARADB_CRC32C_AVX2_SOURCE}
                PROPERTIES COMPILE_OPTIONS "-mavx2")
        set_source_files_properties(${AKKARADB_CRC32C_AVX512_SOURCE}
                PROPERTIES COMPILE_OPTIONS "-mavx512f;-mvpclmulqdq")
    endif ()
endif ()

# ==================== Linking ====================
target_link_libraries(akkaradb PRIVATE libzstd_static mbedtls mbedcrypto mbedx509)

# TLS always enabled
target_compile_definitions(akkaradb PRIVATE AKKARADB_TLS_ENABLED)

if(WIN32)
    target_link_libraries(akkaradb PRIVATE ws2_32)
endif()

# ==================== Include Directories ====================
target_include_directories(akkaradb
        PUBLIC
        $<BUILD_INTERFACE:${AKKARADB_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${AKKENGINE_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${boost_pfr_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

set_target_properties(akkaradb PROPERTIES
        LINKER_LANGUAGE CXX
        OUTPUT_NAME "${AKKARADB_NATIVE_OUTPUT_NAME}"
)

# ==================== Shared / Static Configuration ====================
if (BUILD_SHARED_LIBS)
    target_compile_definitions(akkaradb PRIVATE AKKARADB_BUILD_SHARED)
    set_target_properties(akkaradb PROPERTIES
            CXX_VISIBILITY_PRESET hidden
            VISIBILITY_INLINES_HIDDEN ON
            VERSION ${PROJECT_VERSION}
            SOVERSION ${PROJECT_VERSION_MAJOR}
    )
    if (WIN32)
        set_target_properties(akkaradb PROPERTIES
                WINDOWS_EXPORT_ALL_SYMBOLS ON
                ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
                RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
        )
    endif ()
    message(STATUS "AkkaraDB: building as SHARED library (.dll / .so)")
else ()
    target_compile_definitions(akkaradb PUBLIC AKKARADB_STATIC)
    message(STATUS "AkkaraDB: building as STATIC library (.lib / .a)")
endif ()

# ==================== Installation ====================
install(TARGETS akkaradb
        EXPORT AkkaraDBTargets
        LIBRARY DESTINATION lib COMPONENT SDK
        ARCHIVE DESTINATION lib COMPONENT SDK
        RUNTIME DESTINATION bin COMPONENT SDK
        FILE_SET HEADERS DESTINATION include COMPONENT SDK
        FILE_SET akkengine_headers DESTINATION include COMPONENT SDK
)

install(EXPORT AkkaraDBTargets
        FILE AkkaraDBTargets.cmake
        NAMESPACE AkkaraDB::
        DESTINATION lib/cmake/AkkaraDB
        COMPONENT SDK
)

# ── find_package() support ────────────────────────────────────────────────────
include(CMakePackageConfigHelpers)
configure_package_config_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/AkkaraDBConfig.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/AkkaraDBConfig.cmake"
        INSTALL_DESTINATION lib/cmake/AkkaraDB
)

write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/AkkaraDBConfigVersion.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
)

install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/AkkaraDBConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/AkkaraDBConfigVersion.cmake"
        DESTINATION lib/cmake/AkkaraDB
        COMPONENT SDK
)

install(FILES
        "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE"
        DESTINATION .
        COMPONENT SDK
)

# ==================== Tests ====================
if (AKKARADB_BUILD_TESTS)
    enable_testing()
    file(MAKE_DIRECTORY "${AKKARADB_TEST_OUTPUT_DIR}")
endif ()

# ==================== Benchmarks ====================
message(STATUS "Building AkkaraDB benchmarks")

add_executable(akkaradb_benchmark
        benchmarks/suite/benchmark.cpp
)

if (MSVC)
    target_compile_options(akkaradb_benchmark PRIVATE /WX-)
else ()
    get_target_property(CURRENT_OPTS akkaradb_benchmark COMPILE_OPTIONS)
    if (CURRENT_OPTS)
        list(REMOVE_ITEM CURRENT_OPTS "-Werror")
        set_target_properties(akkaradb_benchmark PROPERTIES COMPILE_OPTIONS "${CURRENT_OPTS}")
    endif ()
endif ()

target_link_libraries(akkaradb_benchmark PRIVATE akkaradb)

target_include_directories(akkaradb_benchmark
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_benchmark PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

install(TARGETS akkaradb_benchmark RUNTIME DESTINATION bin)

# ==================== API Server Smoke Test ====================
add_executable(akkaradb_api_server_smoke_test
        benchmarks/api/api_server_smoke_test.cpp
)

if(MSVC)
    target_compile_options(akkaradb_api_server_smoke_test PRIVATE /WX-)
endif()

target_link_libraries(akkaradb_api_server_smoke_test PRIVATE akkaradb)

target_include_directories(akkaradb_api_server_smoke_test
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_api_server_smoke_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# ==================== API Server Interactive Runner ====================
add_executable(akkaradb_api_server_test
        benchmarks/api/api_server_test.cpp
)

if(MSVC)
    target_compile_options(akkaradb_api_server_test PRIVATE /WX-)
endif()

target_link_libraries(akkaradb_api_server_test PRIVATE akkaradb)

target_include_directories(akkaradb_api_server_test
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_api_server_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# ==================== AkkEngine Smoke Test ====================
add_executable(akkaradb_akkengine_smoke_test
        benchmarks/smoke/akkengine_smoke_test.cpp
)

if(MSVC)
    target_compile_options(akkaradb_akkengine_smoke_test PRIVATE /WX-)
endif()

target_link_libraries(akkaradb_akkengine_smoke_test PRIVATE akkaradb)

target_include_directories(akkaradb_akkengine_smoke_test
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_akkengine_smoke_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# ==================== Typed API Smoke Test ====================
add_executable(akkaradb_typed_api_smoke_test
        benchmarks/smoke/typed_api_smoke_test.cpp
)

if(MSVC)
    target_compile_options(akkaradb_typed_api_smoke_test PRIVATE /WX-)
endif()

target_link_libraries(akkaradb_typed_api_smoke_test PRIVATE akkaradb)

target_include_directories(akkaradb_typed_api_smoke_test
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_typed_api_smoke_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# ==================== MemTable Smoke Test ====================
add_executable(akkaradb_memtable_smoke_test
        benchmarks/smoke/memtable_smoke_test.cpp
)

if(MSVC)
    target_compile_options(akkaradb_memtable_smoke_test PRIVATE /WX-)
endif()

target_link_libraries(akkaradb_memtable_smoke_test PRIVATE akkaradb)

target_include_directories(akkaradb_memtable_smoke_test
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_memtable_smoke_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# ==================== MemTable Throughput Benchmark ====================
add_executable(akkaradb_memtable_throughput_benchmark
        benchmarks/throughput/memtable_throughput_benchmark.cpp
)

if(MSVC)
    target_compile_options(akkaradb_memtable_throughput_benchmark PRIVATE /WX-)
endif()

target_link_libraries(akkaradb_memtable_throughput_benchmark PRIVATE akkaradb)

target_include_directories(akkaradb_memtable_throughput_benchmark
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_memtable_throughput_benchmark PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# ==================== WAL Throughput Benchmark ====================
add_executable(akkaradb_wal_throughput_benchmark
        benchmarks/throughput/wal_throughput_benchmark.cpp
)

if(MSVC)
    target_compile_options(akkaradb_wal_throughput_benchmark PRIVATE /WX-)
endif()

target_link_libraries(akkaradb_wal_throughput_benchmark PRIVATE akkaradb)

target_include_directories(akkaradb_wal_throughput_benchmark
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_wal_throughput_benchmark PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# ==================== SSTable Throughput Benchmark ====================
add_executable(akkaradb_sstable_throughput_benchmark
        benchmarks/throughput/sstable_throughput_benchmark.cpp
)

if(MSVC)
    target_compile_options(akkaradb_sstable_throughput_benchmark PRIVATE /WX-)
endif()

target_link_libraries(akkaradb_sstable_throughput_benchmark PRIVATE akkaradb)

target_include_directories(akkaradb_sstable_throughput_benchmark
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_sstable_throughput_benchmark PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# ==================== SSTable Bloom Negative Lookup Benchmark ====================
add_executable(akkaradb_sstable_bloom_negative_lookup_benchmark
        benchmarks/throughput/sstable_bloom_negative_lookup_benchmark.cpp
)

if(MSVC)
    target_compile_options(akkaradb_sstable_bloom_negative_lookup_benchmark PRIVATE /WX-)
endif()

target_link_libraries(akkaradb_sstable_bloom_negative_lookup_benchmark PRIVATE akkaradb)

target_include_directories(akkaradb_sstable_bloom_negative_lookup_benchmark
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_sstable_bloom_negative_lookup_benchmark PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# ==================== VersionLog Smoke Test ====================
add_executable(akkaradb_versionlog_smoke_test
        benchmarks/smoke/versionlog_smoke_test.cpp
)

if(MSVC)
    target_compile_options(akkaradb_versionlog_smoke_test PRIVATE /WX-)
endif()

target_link_libraries(akkaradb_versionlog_smoke_test PRIVATE akkaradb)

target_include_directories(akkaradb_versionlog_smoke_test
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_versionlog_smoke_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# ==================== Manifest Smoke Test ====================
add_executable(akkaradb_manifest_smoke_test
        benchmarks/smoke/manifest_smoke_test.cpp
)

if(MSVC)
    target_compile_options(akkaradb_manifest_smoke_test PRIVATE /WX-)
endif()

target_link_libraries(akkaradb_manifest_smoke_test PRIVATE akkaradb)

target_include_directories(akkaradb_manifest_smoke_test
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_manifest_smoke_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# ==================== Cluster Smoke Test ====================
add_executable(akkaradb_cluster_smoke_test
        benchmarks/smoke/cluster_smoke_test.cpp
)

if(MSVC)
    target_compile_options(akkaradb_cluster_smoke_test PRIVATE /WX-)
endif()

target_link_libraries(akkaradb_cluster_smoke_test PRIVATE akkaradb)

target_include_directories(akkaradb_cluster_smoke_test
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_cluster_smoke_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# ==================== Blob Smoke Test ====================
add_executable(akkaradb_blob_smoke_test
        benchmarks/smoke/blob_smoke_test.cpp
)

if(MSVC)
    target_compile_options(akkaradb_blob_smoke_test PRIVATE /WX-)
endif()

target_link_libraries(akkaradb_blob_smoke_test PRIVATE akkaradb)

target_include_directories(akkaradb_blob_smoke_test
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_blob_smoke_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# ==================== SSTable Smoke Test ====================
add_executable(akkaradb_sstable_smoke_test
        benchmarks/smoke/sstable_smoke_test.cpp
)

if(MSVC)
    target_compile_options(akkaradb_sstable_smoke_test PRIVATE /WX-)
endif()

target_link_libraries(akkaradb_sstable_smoke_test PRIVATE akkaradb)

target_include_directories(akkaradb_sstable_smoke_test
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_sstable_smoke_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# ==================== WAL Smoke Test ====================
add_executable(akkaradb_wal_smoke_test
        benchmarks/smoke/wal_smoke_test.cpp
)

if(MSVC)
    target_compile_options(akkaradb_wal_smoke_test PRIVATE /WX-)
endif()

target_link_libraries(akkaradb_wal_smoke_test PRIVATE akkaradb)

target_include_directories(akkaradb_wal_smoke_test
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)

set_target_properties(akkaradb_wal_smoke_test PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

if (WIN32 AND BUILD_SHARED_LIBS)
    add_custom_command(TARGET akkaradb_benchmark POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:akkaradb>
            $<TARGET_FILE_DIR:akkaradb_benchmark>
            COMMENT "Copying akkaradb.dll to benchmark executable directory"
    )
endif()

add_custom_command(TARGET akkaradb POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory "${AKKARADB_NATIVE_DIST_DIR}"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        $<TARGET_FILE:akkaradb>
        "${AKKARADB_NATIVE_DIST_DIR}/${AKKARADB_NATIVE_DIST_LIBRARY_NAME}"
        COMMENT "Copying akkaradb native library to dist/native/${AKKARADB_RELEASE_VERSION}"
)

add_custom_command(TARGET akkaradb POST_BUILD
        COMMAND ${CMAKE_COMMAND}
        -DBUILD_DIR="${CMAKE_BINARY_DIR}"
        -DINSTALL_CONFIG=$<CONFIG>
        -DRELEASE_DIST_DIR="${AKKARADB_RELEASE_DIST_DIR}"
        -DEXTRACT_DIR="${AKKARADB_SDK_EXTRACT_DIR}"
        -DPACKAGE_NAME="${AKKARADB_SDK_PACKAGE_NAME}"
        -DZIP_PATH="${AKKARADB_SDK_ZIP}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/PackageSdkDist.cmake"
        COMMENT "Packaging AkkaraDB SDK: dist/${AKKARADB_RELEASE_VERSION}/${AKKARADB_SDK_PACKAGE_NAME}"
)

set(AKKARADB_SMOKE_TEST_TARGETS
        akkaradb_api_server_smoke_test
        akkaradb_api_server_test
        akkaradb_akkengine_smoke_test
        akkaradb_typed_api_smoke_test
        akkaradb_memtable_smoke_test
        akkaradb_versionlog_smoke_test
        akkaradb_manifest_smoke_test
        akkaradb_cluster_smoke_test
        akkaradb_blob_smoke_test
        akkaradb_sstable_smoke_test
        akkaradb_wal_smoke_test
)

if (AKKARADB_BUILD_TESTS)
    add_custom_target(akkaradb_tests DEPENDS ${AKKARADB_SMOKE_TEST_TARGETS})

    foreach (AKKARADB_TEST_TARGET IN LISTS AKKARADB_SMOKE_TEST_TARGETS)
        set_target_properties(${AKKARADB_TEST_TARGET} PROPERTIES
                RUNTIME_OUTPUT_DIRECTORY "${AKKARADB_TEST_OUTPUT_DIR}"
        )
        add_test(NAME ${AKKARADB_TEST_TARGET} COMMAND $<TARGET_FILE:${AKKARADB_TEST_TARGET}>)

        if (WIN32 AND BUILD_SHARED_LIBS)
            add_custom_command(TARGET ${AKKARADB_TEST_TARGET} POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E copy_if_different
                    $<TARGET_FILE:akkaradb>
                    $<TARGET_FILE_DIR:${AKKARADB_TEST_TARGET}>
                    COMMENT "Copying akkaradb.dll to ${AKKARADB_TEST_TARGET} output directory"
            )
        endif ()
    endforeach ()
endif ()

# ==================== JNI Bridge ====================
if(AKKARADB_BUILD_JNI)
    find_package(JNI REQUIRED)
    add_library(akkaradb_jni SHARED
            ${AKKARA_ROOT}/jni/AkkaraJni.cpp
            ${AKKARADB_SOURCES}
            ${AKKENGINE_SOURCES}
    )
    add_dependencies(akkaradb_jni akkaradb)

    target_link_libraries(akkaradb_jni PRIVATE libzstd_static mbedtls mbedcrypto mbedx509)
    target_include_directories(akkaradb_jni PRIVATE
            ${JNI_INCLUDE_DIRS}
            ${AKKENGINE_INCLUDE_DIR}
            ${AKKARADB_INCLUDE_DIR}
            ${boost_pfr_SOURCE_DIR}/include
    )
    target_compile_definitions(akkaradb_jni PRIVATE AKKARADB_STATIC AKKARADB_TLS_ENABLED)

    set_target_properties(akkaradb_jni PROPERTIES
            CXX_STANDARD 23
            CXX_STANDARD_REQUIRED ON
            OUTPUT_NAME "${AKKARADB_JNI_OUTPUT_NAME}"
    )

    if(WIN32)
        target_link_libraries(akkaradb_jni PRIVATE ws2_32)
    endif()

    if(WIN32)
        set_target_properties(akkaradb_jni PROPERTIES
                RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
        )
    endif()

    add_custom_command(TARGET akkaradb_jni POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E make_directory "${AKKARADB_NATIVE_DIST_DIR}"
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:akkaradb_jni>
            "${AKKARADB_NATIVE_DIST_DIR}/$<TARGET_FILE_NAME:akkaradb_jni>"
            COMMAND ${CMAKE_COMMAND}
            -DDIST_DIR="${AKKARADB_NATIVE_DIST_DIR}"
            -DPACKAGE_DIR="${AKKARADB_NATIVE_PACKAGE_DIR}"
            -DPLATFORM="${AKKARADB_DIST_PLATFORM}"
            -DDETAIL_VERSION_BASE="${AKKARADB_DETAIL_VERSION_BASE}"
            -DPRERELEASE="${AKKARADB_PRERELEASE}"
            -DNATIVE_LIBRARY_NAME="${AKKARADB_NATIVE_DIST_LIBRARY_NAME}"
            -DJNI_LIBRARY_NAME=$<TARGET_FILE_NAME:akkaradb_jni>
            -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/PackageNativeDist.cmake"
            COMMENT "Copying akkaradb JNI library to dist/native/${AKKARADB_RELEASE_VERSION}"
    )

    message(STATUS "JNI bridge enabled  Etarget: akkaradb_jni")
endif()

message(STATUS "AkkaraDB CMake configuration completed. TLS is enabled; SIMD paths are selected at runtime.")

cmake_minimum_required(VERSION 4.1)

project(AkkaraDB
        DESCRIPTION "Low-latency, crash-safe JVM KV store with WAL & stripe parity"
        VERSION 0.6.0
        LANGUAGES CXX
)

# ==================== Build Options ====================
option(BUILD_SHARED_LIBS "Build shared libraries (.so/.dll)" ON)
option(AKKARADB_BUILD_TESTS "Build unit tests" OFF)
option(AKKARADB_BUILD_JNI "Build JNI bridge for JVM integration" OFF)

# ==================== C++ Standard ====================
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ==================== Directory Configuration ====================
set(AKKARA_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/akkara)
set(AKKARADB_DIR ${AKKARA_ROOT}/akkaradb)
set(INTERNAL_DIR ${AKKARA_ROOT}/internal)

set(AKKARADB_INCLUDE_DIR ${AKKARADB_DIR}/include)
set(AKKARADB_SRC_DIR ${AKKARADB_DIR}/src)

set(INTERNAL_INCLUDE_DIR ${INTERNAL_DIR}/include)
set(INTERNAL_SRC_DIR ${INTERNAL_DIR}/src)

# ==================== Compiler Settings ====================
if (MSVC)
    add_compile_options(/W4 /permissive- /utf-8)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
else ()
    add_compile_options(-Wall -Wextra -pedantic)
endif ()

# Always enable SIMD optimizations (SSE4.2 + AVX2)
if(MSVC)
    add_compile_options(/arch:AVX2)
else()
    add_compile_options(-msse4.2 -mavx2)
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

# ====================== Internal Headers ======================
file(GLOB_RECURSE INTERNAL_HEADERS CONFIGURE_DEPENDS
        "${INTERNAL_INCLUDE_DIR}/*.hpp"
        "${INTERNAL_INCLUDE_DIR}/*.h"
)

target_sources(akkaradb PUBLIC FILE_SET internal_headers TYPE HEADERS
        BASE_DIRS ${INTERNAL_INCLUDE_DIR}
        FILES ${INTERNAL_HEADERS}
)

# ====================== Source Files ======================
file(GLOB_RECURSE AKKARADB_SOURCES CONFIGURE_DEPENDS
        "${AKKARADB_SRC_DIR}/*.cpp"
)

file(GLOB_RECURSE INTERNAL_SOURCES CONFIGURE_DEPENDS
        "${INTERNAL_SRC_DIR}/*.cpp"
)

target_sources(akkaradb PRIVATE
        ${AKKARADB_SOURCES}
        ${INTERNAL_SOURCES}
)

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
        $<BUILD_INTERFACE:${INTERNAL_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${boost_pfr_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
        $<INSTALL_INTERFACE:include/internal>
)

set_target_properties(akkaradb PROPERTIES LINKER_LANGUAGE CXX)

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
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        FILE_SET HEADERS DESTINATION include
        FILE_SET internal_headers DESTINATION include/internal
)

install(EXPORT AkkaraDBTargets
        FILE AkkaraDBTargets.cmake
        NAMESPACE AkkaraDB::
        DESTINATION lib/cmake/AkkaraDB
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
)

# ==================== Tests ====================
if (AKKARADB_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
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
        ${INTERNAL_INCLUDE_DIR}
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
        ${INTERNAL_INCLUDE_DIR}
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
        ${INTERNAL_INCLUDE_DIR}
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
        ${INTERNAL_INCLUDE_DIR}
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
        ${INTERNAL_INCLUDE_DIR}
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
        ${INTERNAL_INCLUDE_DIR}
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
        ${INTERNAL_INCLUDE_DIR}
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
        ${INTERNAL_INCLUDE_DIR}
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
        ${INTERNAL_INCLUDE_DIR}
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
        ${INTERNAL_INCLUDE_DIR}
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
        ${INTERNAL_INCLUDE_DIR}
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
        ${INTERNAL_INCLUDE_DIR}
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
        ${INTERNAL_INCLUDE_DIR}
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
        ${INTERNAL_INCLUDE_DIR}
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
        ${INTERNAL_INCLUDE_DIR}
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
        ${INTERNAL_INCLUDE_DIR}
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

# ==================== JNI Bridge ====================
if(AKKARADB_BUILD_JNI)
    find_package(JNI REQUIRED)
    add_library(akkaradb_jni SHARED
            ${AKKARA_ROOT}/jni/AkkaraJni.cpp
    )

    target_link_libraries(akkaradb_jni PRIVATE akkaradb)
    target_include_directories(akkaradb_jni PRIVATE
            ${JNI_INCLUDE_DIRS}
            ${INTERNAL_INCLUDE_DIR}
            ${AKKARADB_INCLUDE_DIR}
    )

    set_target_properties(akkaradb_jni PROPERTIES
            CXX_STANDARD 23
            CXX_STANDARD_REQUIRED ON
    )

    if(WIN32)
        set_target_properties(akkaradb_jni PROPERTIES
                RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
        )
    endif()

    message(STATUS "JNI bridge enabled — target: akkaradb_jni")
endif()

message(STATUS "AkkaraDB CMake configuration completed. TLS and SIMD are always enabled.")

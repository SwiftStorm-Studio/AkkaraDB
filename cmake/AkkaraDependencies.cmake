# ==================== Dependencies ====================
include(FetchContent)

FetchContent_Declare(
        zstd
        GIT_REPOSITORY https://github.com/facebook/zstd.git
        GIT_TAG v1.5.7
        SOURCE_SUBDIR build/cmake
)
set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_CONTRIB OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)

set(CMAKE_WARN_DEPRECATED OFF CACHE BOOL "" FORCE)
set(AKKARADB_SAVED_BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS})
set(BUILD_SHARED_LIBS OFF)
FetchContent_MakeAvailable(zstd)
set(BUILD_SHARED_LIBS ${AKKARADB_SAVED_BUILD_SHARED_LIBS})
set(CMAKE_WARN_DEPRECATED ON CACHE BOOL "" FORCE)

FetchContent_Declare(
        boost_pfr
        GIT_REPOSITORY https://github.com/boostorg/pfr.git
        GIT_TAG boost-1.91.0
)
FetchContent_MakeAvailable(boost_pfr)

FetchContent_Declare(
        monocypher
        GIT_REPOSITORY https://github.com/LoupVaillant/Monocypher.git
        GIT_TAG 4.0.3
)
FetchContent_MakeAvailable(monocypher)

if (NOT TARGET akkaradb_monocypher)
    set_source_files_properties(${monocypher_SOURCE_DIR}/src/monocypher.c PROPERTIES LANGUAGE CXX)
    add_library(akkaradb_monocypher STATIC
            ${monocypher_SOURCE_DIR}/src/monocypher.c
    )
    target_include_directories(akkaradb_monocypher PUBLIC
            $<BUILD_INTERFACE:${monocypher_SOURCE_DIR}/src>
            $<INSTALL_INTERFACE:include/akkaradb/third_party/monocypher>
    )
endif ()

FetchContent_Declare(
        mbedtls
        # Use the official release archive. GitHub's auto-generated source archives
        # omit the bundled TF-PSA-Crypto sources and can't be configured for 4.1.0.
        URL https://github.com/Mbed-TLS/mbedtls/releases/download/mbedtls-4.1.0/mbedtls-4.1.0.tar.bz2
        URL_HASH SHA256=377a09cf8eb81b5fb2707045e5522d5489d3309fed5006c9874e60558fc81d10
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(ENABLE_PROGRAMS OFF CACHE BOOL "" FORCE)
set(MBEDTLS_FATAL_WARNINGS OFF CACHE BOOL "" FORCE)
set(AKKARADB_SAVED_BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS})
set(BUILD_SHARED_LIBS OFF)
FetchContent_MakeAvailable(mbedtls)
set(BUILD_SHARED_LIBS ${AKKARADB_SAVED_BUILD_SHARED_LIBS})

cmake_minimum_required(VERSION 4.1)

project(AkkaraDB
        DESCRIPTION "Low-latency, crash-safe JVM KV store with WAL & stripe parity"
        VERSION 2026.1.1
        LANGUAGES CXX
)

include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/settings/CMakeLists.txt")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/AkkaraDependencies.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/AkkaraTargets.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/AkkaraBenchmarksAndTests.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/AkkaraPackaging.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/AkkaraJni.cmake")

message(STATUS "AkkaraDB CMake configuration completed. TLS is enabled; SIMD paths are selected at runtime.")

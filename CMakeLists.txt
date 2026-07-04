cmake_minimum_required(VERSION 4.1)

set(AKKARADB_VERSION_PROPERTIES_FILE "${CMAKE_CURRENT_SOURCE_DIR}/version.properties" CACHE FILEPATH "AkkaraDB root version properties file")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/AkkaraVersionProperties.cmake")
akkaradb_load_version_properties("${AKKARADB_VERSION_PROPERTIES_FILE}")

foreach (AKKARADB_REQUIRED_VERSION_PROPERTY IN ITEMS
        AKKARADB_PROJECT_VERSION
        AKKARADB_DETAIL_VERSION_BASE
        AKKARADB_COMPAT_LINE
        AKKARADB_REVISION_FILE
)
    if (NOT DEFINED ${AKKARADB_REQUIRED_VERSION_PROPERTY} OR "${${AKKARADB_REQUIRED_VERSION_PROPERTY}}" STREQUAL "")
        message(FATAL_ERROR "Missing required AkkaraDB version property: ${AKKARADB_REQUIRED_VERSION_PROPERTY}")
    endif ()
endforeach ()
if (NOT DEFINED AKKARADB_PRERELEASE)
    message(FATAL_ERROR "Missing required AkkaraDB version property: AKKARADB_PRERELEASE")
endif ()

project(AkkaraDB
        DESCRIPTION "The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database"
        VERSION ${AKKARADB_PROJECT_VERSION}
        LANGUAGES CXX
)

include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/settings/CMakeLists.txt")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/AkkaraDependencies.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/AkkaraTargets.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/AkkaraBenchmarksAndTests.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/AkkaraPackaging.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/AkkaraJni.cmake")

message(STATUS "AkkaraDB CMake configuration completed. TLS is enabled; SIMD paths are selected at runtime.")

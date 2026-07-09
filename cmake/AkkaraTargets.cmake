# ==================== Main Library ====================
add_library(akkaradb ${AKKARADB_LIBRARY_KIND})

if (AKKARADB_WARNINGS_AS_ERRORS)
    if (MSVC)
        target_compile_options(akkaradb PRIVATE /WX)
    else ()
        target_compile_options(akkaradb PRIVATE -Werror)
    endif ()
endif ()

file(GLOB_RECURSE PUBLIC_HEADERS CONFIGURE_DEPENDS
        "${AKKARADB_INCLUDE_DIR}/akkaradb/*.hpp"
        "${AKKARADB_INCLUDE_DIR}/akkaradb/*.h"
)
target_sources(akkaradb PUBLIC FILE_SET HEADERS
        BASE_DIRS ${AKKARADB_INCLUDE_DIR}
        FILES ${PUBLIC_HEADERS}
)

file(GLOB_RECURSE AKKENGINE_HEADERS CONFIGURE_DEPENDS
        "${AKKENGINE_INCLUDE_DIR}/*.hpp"
        "${AKKENGINE_INCLUDE_DIR}/*.h"
)
file(GLOB_RECURSE AKKSERVER_HEADERS CONFIGURE_DEPENDS
        "${AKKSERVER_INCLUDE_DIR}/*.hpp"
        "${AKKSERVER_INCLUDE_DIR}/*.h"
)
target_sources(akkaradb PUBLIC FILE_SET akkengine_headers TYPE HEADERS
        BASE_DIRS ${AKKENGINE_INCLUDE_DIR} ${AKKSERVER_INCLUDE_DIR}
        FILES ${AKKENGINE_HEADERS} ${AKKSERVER_HEADERS}
)

file(GLOB_RECURSE AKKARADB_SOURCES CONFIGURE_DEPENDS
        "${AKKARADB_SRC_DIR}/*.cpp"
)
file(GLOB_RECURSE AKKENGINE_SOURCES CONFIGURE_DEPENDS
        "${AKKENGINE_SRC_DIR}/*.cpp"
)
file(GLOB_RECURSE AKKSERVER_SOURCES CONFIGURE_DEPENDS
        "${AKKSERVER_SRC_DIR}/*.cpp"
)

set(AKKENGINE_API_CORE_SOURCES
        "${AKKSERVER_SRC_DIR}/server/AkkApiServer.cpp"
)
set(AKKENGINE_API_PROVIDER_SOURCES
        "${AKKSERVER_SRC_DIR}/server/ApiServerProvider.cpp"
        "${AKKSERVER_SRC_DIR}/server/AkkApiTransportProvider.cpp"
)
set(AKKENGINE_API_HTTP_SOURCES
        "${AKKSERVER_SRC_DIR}/http/HttpApiServer.cpp"
)
set(AKKENGINE_API_TCP_SOURCES
        "${AKKSERVER_SRC_DIR}/tcp/TcpApiServer.cpp"
        "${AKKSERVER_SRC_DIR}/tcp/ApiFraming.cpp"
)
set(AKKENGINE_CLUSTER_RUNTIME_SOURCES
        "${AKKENGINE_SRC_DIR}/engine/cluster/ClusterRuntime.cpp"
        "${AKKENGINE_SRC_DIR}/engine/cluster/ClusterManager.cpp"
        "${AKKENGINE_SRC_DIR}/engine/cluster/ClusterRouter.cpp"
        "${AKKENGINE_SRC_DIR}/engine/cluster/ReplicationClient.cpp"
        "${AKKENGINE_SRC_DIR}/engine/cluster/ReplicationServer.cpp"
        "${AKKENGINE_SRC_DIR}/engine/cluster/ReplFraming.cpp"
)

list(REMOVE_ITEM AKKENGINE_SOURCES ${AKKSERVER_SOURCES})
list(REMOVE_ITEM AKKENGINE_SOURCES ${AKKENGINE_CLUSTER_RUNTIME_SOURCES})

target_sources(akkaradb PRIVATE
        ${AKKARADB_SOURCES}
        ${AKKENGINE_SOURCES}
        ${AKKENGINE_API_PROVIDER_SOURCES}
)

add_custom_target(akkaradb_increment_revision
        COMMAND ${CMAKE_COMMAND}
        -DREVISION_FILE=${AKKARADB_REVISION_FILE}
        -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/IncrementRevision.cmake
        COMMENT "Incrementing AkkaraDB build revision"
        VERBATIM
)
add_dependencies(akkaradb akkaradb_increment_revision)

set(AKKARADB_CRC32C_SSE42_SOURCE "${AKKENGINE_SRC_DIR}/cpu/crc32c/CRC32CX86SSE42.cpp")
set(AKKARADB_CRC32C_AVX2_SOURCE "${AKKENGINE_SRC_DIR}/cpu/crc32c/CRC32CX86AVX2.cpp")
set(AKKARADB_CRC32C_AVX512_SOURCE "${AKKENGINE_SRC_DIR}/cpu/crc32c/CRC32CX86AVX512.cpp")
if (AKKARADB_DIST_ARCH STREQUAL "x86_64")
    if (MSVC)
        if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
            set_source_files_properties(${AKKARADB_CRC32C_SSE42_SOURCE}
                    PROPERTIES COMPILE_OPTIONS "/clang:-msse4.2")
        endif ()
        set_source_files_properties(${AKKARADB_CRC32C_AVX2_SOURCE}
                PROPERTIES COMPILE_OPTIONS "/arch:AVX2")
        set_source_files_properties(${AKKARADB_CRC32C_AVX512_SOURCE}
                PROPERTIES COMPILE_OPTIONS "/arch:AVX512")
    elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        set_source_files_properties(${AKKARADB_CRC32C_SSE42_SOURCE}
                PROPERTIES COMPILE_OPTIONS "-msse4.2")
        set_source_files_properties(${AKKARADB_CRC32C_AVX2_SOURCE}
                PROPERTIES COMPILE_OPTIONS "-mavx2;-mpclmul;-msse4.2")
        set_source_files_properties(${AKKARADB_CRC32C_AVX512_SOURCE}
                PROPERTIES COMPILE_OPTIONS "-mavx512f;-mavx512vl;-mvpclmulqdq;-mpclmul;-msse4.2")
    endif ()
endif ()

target_link_libraries(akkaradb PRIVATE libzstd_static akkaradb_monocypher mbedtls tfpsacrypto mbedx509)
target_compile_definitions(akkaradb PRIVATE
        AKKARADB_TLS_ENABLED
        AKKARADB_API_SERVER_LIBRARY_FILENAME="${AKKARADB_API_DIST_LIBRARY_NAME}"
        AKKARADB_API_HTTP_LIBRARY_FILENAME="${AKKARADB_API_HTTP_DIST_LIBRARY_NAME}"
        AKKARADB_API_TCP_LIBRARY_FILENAME="${AKKARADB_API_TCP_DIST_LIBRARY_NAME}"
        AKKARADB_API_GRPC_LIBRARY_FILENAME="${AKKARADB_API_GRPC_DIST_LIBRARY_NAME}"
        AKKARADB_CLUSTER_LIBRARY_FILENAME="${AKKARADB_CLUSTER_DIST_LIBRARY_NAME}"
)
if(WIN32)
    target_link_libraries(akkaradb PRIVATE ws2_32 bcrypt)
endif()
if(CMAKE_DL_LIBS)
    target_link_libraries(akkaradb PRIVATE ${CMAKE_DL_LIBS})
endif()
target_include_directories(akkaradb
        PUBLIC
        $<BUILD_INTERFACE:${AKKARADB_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${AKKENGINE_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${AKKSERVER_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${boost_pfr_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${mbedtls_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${mbedtls_SOURCE_DIR}/tf-psa-crypto/include>
        $<INSTALL_INTERFACE:include>
)

# ==================== API Server Targets ====================
set(AKKARADB_BUILT_API_BACKEND_TARGETS "")
if (AKKARADB_BUILD_API_SERVERS)
    add_library(akkaradb_api SHARED)
    target_sources(akkaradb_api PRIVATE ${AKKENGINE_API_CORE_SOURCES})
    target_include_directories(akkaradb_api PRIVATE
            ${AKKARADB_INCLUDE_DIR}
            ${AKKENGINE_INCLUDE_DIR}
            ${AKKSERVER_INCLUDE_DIR}
            ${boost_pfr_SOURCE_DIR}/include
    )
    target_link_libraries(akkaradb_api PRIVATE akkaradb)
    target_compile_definitions(akkaradb_api PRIVATE
            AKKARADB_API_SERVER_BUILD_SHARED
            AKKARADB_TLS_ENABLED
    )
    if (AKKARADB_BUILD_API_HTTP)
        target_compile_definitions(akkaradb_api PRIVATE AKKARADB_API_HAS_HTTP_BACKEND)
    endif ()
    if (AKKARADB_BUILD_API_TCP)
        target_compile_definitions(akkaradb_api PRIVATE AKKARADB_API_HAS_TCP_BACKEND)
    endif ()
    if (AKKARADB_WARNINGS_AS_ERRORS)

        if (MSVC)

            target_compile_options(akkaradb_api PRIVATE /WX)

        else ()

            target_compile_options(akkaradb_api PRIVATE -Werror)

        endif ()

    endif ()
    if(WIN32)
        target_link_libraries(akkaradb_api PRIVATE ws2_32 bcrypt)
    endif()

    if (AKKARADB_BUILD_API_HTTP)
        add_library(akkaradb_api_http SHARED)
        target_sources(akkaradb_api_http PRIVATE ${AKKENGINE_API_HTTP_SOURCES})
        target_include_directories(akkaradb_api_http PRIVATE
                ${AKKARADB_INCLUDE_DIR}
                ${AKKENGINE_INCLUDE_DIR}
                ${AKKSERVER_INCLUDE_DIR}
                ${boost_pfr_SOURCE_DIR}/include
        )
        target_link_libraries(akkaradb_api_http PRIVATE akkaradb)
        target_compile_definitions(akkaradb_api_http PRIVATE
                AKKARADB_API_SERVER_BUILD_SHARED
                AKKARADB_TLS_ENABLED
        )
        if (AKKARADB_WARNINGS_AS_ERRORS)
            if (AKKARADB_WARNINGS_AS_ERRORS)

                if (MSVC)

                    target_compile_options(akkaradb_api_http PRIVATE /WX)

                else ()

                    target_compile_options(akkaradb_api_http PRIVATE -Werror)

                endif ()

            endif ()
        endif ()
        if(WIN32)
            target_link_libraries(akkaradb_api_http PRIVATE ws2_32 bcrypt)
        endif()
        list(APPEND AKKARADB_BUILT_API_BACKEND_TARGETS akkaradb_api_http)
    endif ()

    if (AKKARADB_BUILD_API_TCP)
        add_library(akkaradb_api_tcp SHARED)
        target_sources(akkaradb_api_tcp PRIVATE ${AKKENGINE_API_TCP_SOURCES})
        target_include_directories(akkaradb_api_tcp PRIVATE
                ${AKKARADB_INCLUDE_DIR}
                ${AKKENGINE_INCLUDE_DIR}
                ${AKKSERVER_INCLUDE_DIR}
                ${boost_pfr_SOURCE_DIR}/include
        )
        target_link_libraries(akkaradb_api_tcp PRIVATE akkaradb)
        target_compile_definitions(akkaradb_api_tcp PRIVATE
                AKKARADB_API_SERVER_BUILD_SHARED
                AKKARADB_TLS_ENABLED
        )
        if (AKKARADB_WARNINGS_AS_ERRORS)
            if (AKKARADB_WARNINGS_AS_ERRORS)

                if (MSVC)

                    target_compile_options(akkaradb_api_tcp PRIVATE /WX)

                else ()

                    target_compile_options(akkaradb_api_tcp PRIVATE -Werror)

                endif ()

            endif ()
        endif ()
        if(WIN32)
            target_link_libraries(akkaradb_api_tcp PRIVATE ws2_32 bcrypt)
        endif()
        list(APPEND AKKARADB_BUILT_API_BACKEND_TARGETS akkaradb_api_tcp)
    endif ()

    if (AKKARADB_BUILD_API_GRPC)
        add_library(akkaradb_api_grpc SHARED)
        target_include_directories(akkaradb_api_grpc PRIVATE
                ${AKKARADB_INCLUDE_DIR}
                ${AKKENGINE_INCLUDE_DIR}
                ${AKKSERVER_INCLUDE_DIR}
                ${boost_pfr_SOURCE_DIR}/include
        )
        target_link_libraries(akkaradb_api_grpc PRIVATE akkaradb)
        target_compile_definitions(akkaradb_api_grpc PRIVATE
                AKKARADB_API_SERVER_BUILD_SHARED
                AKKARADB_TLS_ENABLED
        )
        if (AKKARADB_WARNINGS_AS_ERRORS)
            if (AKKARADB_WARNINGS_AS_ERRORS)

                if (MSVC)

                    target_compile_options(akkaradb_api_grpc PRIVATE /WX)

                else ()

                    target_compile_options(akkaradb_api_grpc PRIVATE -Werror)

                endif ()

            endif ()
        endif ()
        if(WIN32)
            target_link_libraries(akkaradb_api_grpc PRIVATE ws2_32 bcrypt)
        endif()
        list(APPEND AKKARADB_BUILT_API_BACKEND_TARGETS akkaradb_api_grpc)
    endif ()
endif ()

add_library(akkaradb_cluster SHARED)
target_sources(akkaradb_cluster PRIVATE ${AKKENGINE_CLUSTER_RUNTIME_SOURCES})
target_include_directories(akkaradb_cluster PRIVATE
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
        ${boost_pfr_SOURCE_DIR}/include
)
target_link_libraries(akkaradb_cluster PRIVATE akkaradb)
target_compile_definitions(akkaradb_cluster PRIVATE
        AKKARADB_CLUSTER_RUNTIME_BUILD_SHARED
        AKKARADB_TLS_ENABLED
)
if (AKKARADB_WARNINGS_AS_ERRORS)
    if (AKKARADB_WARNINGS_AS_ERRORS)

        if (MSVC)

            target_compile_options(akkaradb_cluster PRIVATE /WX)

        else ()

            target_compile_options(akkaradb_cluster PRIVATE -Werror)

        endif ()

    endif ()
endif ()
if(WIN32)
    target_link_libraries(akkaradb_cluster PRIVATE ws2_32 bcrypt)
endif()

if (TARGET akkaradb_api)
    add_dependencies(akkaradb_api
            ${AKKARADB_BUILT_API_BACKEND_TARGETS}
            akkaradb_cluster
    )
endif ()

if (TARGET akkaradb_api_grpc)
    find_package(Protobuf CONFIG QUIET)
    find_package(gRPC CONFIG QUIET)

    set(AKKARADB_FETCHED_GRPC OFF)
    if (NOT Protobuf_FOUND OR NOT gRPC_FOUND)
        message(STATUS "AkkaraDB: Protobuf/gRPC packages not found; fetching gRPC")
        set(AKKARADB_FETCHED_GRPC ON)
        set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
        set(gRPC_BUILD_CODEGEN ON CACHE BOOL "" FORCE)
        set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF CACHE BOOL "" FORCE)
        set(gRPC_BUILD_GRPC_NODE_PLUGIN OFF CACHE BOOL "" FORCE)
        set(gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF CACHE BOOL "" FORCE)
        set(gRPC_BUILD_GRPC_PHP_PLUGIN OFF CACHE BOOL "" FORCE)
        set(gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF CACHE BOOL "" FORCE)
        set(gRPC_BUILD_GRPC_RUBY_PLUGIN OFF CACHE BOOL "" FORCE)
        set(gRPC_BUILD_GRPCPP_OTEL_PLUGIN OFF CACHE BOOL "" FORCE)
        set(gRPC_INSTALL OFF CACHE BOOL "" FORCE)
        set(gRPC_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
        set(gRPC_ABSL_PROVIDER "module" CACHE STRING "" FORCE)
        set(gRPC_CARES_PROVIDER "module" CACHE STRING "" FORCE)
        set(gRPC_PROTOBUF_PROVIDER "module" CACHE STRING "" FORCE)
        set(gRPC_RE2_PROVIDER "module" CACHE STRING "" FORCE)
        set(gRPC_SSL_PROVIDER "module" CACHE STRING "" FORCE)
        set(gRPC_ZLIB_PROVIDER "module" CACHE STRING "" FORCE)
        set(OPENSSL_NO_ASM ON CACHE BOOL "" FORCE)
        set(protobuf_INSTALL OFF CACHE BOOL "" FORCE)
        set(utf8_range_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
        set(utf8_range_ENABLE_TESTS OFF CACHE BOOL "" FORCE)
        set(CMAKE_POLICY_VERSION_MINIMUM 3.5 CACHE STRING "" FORCE)
        set(AKKARADB_SAVED_BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS})
        get_directory_property(AKKARADB_SAVED_COMPILE_OPTIONS COMPILE_OPTIONS)
        set(BUILD_SHARED_LIBS OFF)
        set_directory_properties(PROPERTIES COMPILE_OPTIONS "")
        FetchContent_Declare(
                grpc
                GIT_REPOSITORY https://github.com/grpc/grpc.git
                GIT_TAG v1.81.1
                GIT_SHALLOW TRUE
                GIT_SUBMODULES
                    third_party/abseil-cpp
                    third_party/boringssl-with-bazel
                    third_party/cares/cares
                    third_party/protobuf
                    third_party/re2
                    third_party/zlib
                GIT_SUBMODULES_RECURSE FALSE
        )
        FetchContent_MakeAvailable(grpc)
        set_directory_properties(PROPERTIES COMPILE_OPTIONS "${AKKARADB_SAVED_COMPILE_OPTIONS}")
        set(BUILD_SHARED_LIBS ${AKKARADB_SAVED_BUILD_SHARED_LIBS})
        foreach (AKKARADB_GRPC_THIRD_PARTY_TARGET IN ITEMS
                bssl
                crypto
                decrepit
                fipsmodule
                ssl
        )
            if (TARGET ${AKKARADB_GRPC_THIRD_PARTY_TARGET})
                target_compile_options(${AKKARADB_GRPC_THIRD_PARTY_TARGET} PRIVATE
                        $<$<COMPILE_LANG_AND_ID:C,Clang>:-Wno-unused-parameter>
                        $<$<COMPILE_LANG_AND_ID:CXX,Clang>:-Wno-unused-parameter>
                        $<$<COMPILE_LANG_AND_ID:C,Clang>:-Wno-error=unused-parameter>
                        $<$<COMPILE_LANG_AND_ID:CXX,Clang>:-Wno-error=unused-parameter>
                )
            endif ()
        endforeach ()
    endif ()

    set(AKKARADB_GRPCPP_TARGET "")
    if (AKKARADB_FETCHED_GRPC AND TARGET grpc++)
        set(AKKARADB_GRPCPP_TARGET grpc++)
    elseif (TARGET gRPC::grpc++)
        set(AKKARADB_GRPCPP_TARGET gRPC::grpc++)
    elseif (TARGET grpc++)
        set(AKKARADB_GRPCPP_TARGET grpc++)
    endif ()

    set(AKKARADB_PROTOBUF_TARGET "")
    if (AKKARADB_FETCHED_GRPC AND TARGET libprotobuf)
        set(AKKARADB_PROTOBUF_TARGET libprotobuf)
    elseif (TARGET protobuf::libprotobuf)
        set(AKKARADB_PROTOBUF_TARGET protobuf::libprotobuf)
    elseif (TARGET libprotobuf)
        set(AKKARADB_PROTOBUF_TARGET libprotobuf)
    endif ()

    set(AKKARADB_PROTOC_TARGET "")
    if (AKKARADB_FETCHED_GRPC AND TARGET protoc)
        set(AKKARADB_PROTOC_TARGET protoc)
    elseif (TARGET protobuf::protoc)
        set(AKKARADB_PROTOC_TARGET protobuf::protoc)
    elseif (TARGET protoc)
        set(AKKARADB_PROTOC_TARGET protoc)
    endif ()

    set(AKKARADB_GRPC_CPP_PLUGIN_TARGET "")
    if (AKKARADB_FETCHED_GRPC AND TARGET grpc_cpp_plugin)
        set(AKKARADB_GRPC_CPP_PLUGIN_TARGET grpc_cpp_plugin)
    elseif (TARGET gRPC::grpc_cpp_plugin)
        set(AKKARADB_GRPC_CPP_PLUGIN_TARGET gRPC::grpc_cpp_plugin)
    elseif (TARGET grpc_cpp_plugin)
        set(AKKARADB_GRPC_CPP_PLUGIN_TARGET grpc_cpp_plugin)
    endif ()

    if (AKKARADB_GRPCPP_TARGET AND AKKARADB_PROTOBUF_TARGET AND AKKARADB_PROTOC_TARGET AND AKKARADB_GRPC_CPP_PLUGIN_TARGET)
        set(AKKARADB_GRPC_PROTO "${AKKSERVER_PROTO_DIR}/akkaradb_grpc.proto")
        set(AKKARADB_GRPC_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated/akkserver_grpc")
        file(MAKE_DIRECTORY "${AKKARADB_GRPC_GENERATED_DIR}")

        set(AKKARADB_GRPC_GENERATED_SOURCES
                "${AKKARADB_GRPC_GENERATED_DIR}/akkaradb_grpc.pb.cc"
                "${AKKARADB_GRPC_GENERATED_DIR}/akkaradb_grpc.grpc.pb.cc"
        )
        set(AKKARADB_GRPC_GENERATED_HEADERS
                "${AKKARADB_GRPC_GENERATED_DIR}/akkaradb_grpc.pb.h"
                "${AKKARADB_GRPC_GENERATED_DIR}/akkaradb_grpc.grpc.pb.h"
        )

        add_custom_command(
                OUTPUT ${AKKARADB_GRPC_GENERATED_SOURCES} ${AKKARADB_GRPC_GENERATED_HEADERS}
                COMMAND $<TARGET_FILE:${AKKARADB_PROTOC_TARGET}>
                ARGS
                --cpp_out "${AKKARADB_GRPC_GENERATED_DIR}"
                --grpc_out "${AKKARADB_GRPC_GENERATED_DIR}"
                --plugin=protoc-gen-grpc=$<TARGET_FILE:${AKKARADB_GRPC_CPP_PLUGIN_TARGET}>
                -I "${AKKSERVER_PROTO_DIR}"
                "${AKKARADB_GRPC_PROTO}"
                DEPENDS "${AKKARADB_GRPC_PROTO}" ${AKKARADB_PROTOC_TARGET} ${AKKARADB_GRPC_CPP_PLUGIN_TARGET}
                VERBATIM
        )

        add_custom_target(akkaradb_grpc_codegen
                DEPENDS ${AKKARADB_GRPC_GENERATED_SOURCES} ${AKKARADB_GRPC_GENERATED_HEADERS}
        )

        target_sources(akkaradb_api_grpc PRIVATE
                "${AKKSERVER_SRC_DIR}/grpc/AkkaraGRPCServer.cpp"
                ${AKKARADB_GRPC_GENERATED_SOURCES}
                ${AKKARADB_GRPC_GENERATED_HEADERS}
        )
        add_dependencies(akkaradb_api_grpc akkaradb_grpc_codegen)
        if (MSVC)
            set_source_files_properties(
                    "${AKKSERVER_SRC_DIR}/grpc/AkkaraGRPCServer.cpp"
                    ${AKKARADB_GRPC_GENERATED_SOURCES}
                    PROPERTIES COMPILE_OPTIONS "/WX-"
            )
        endif ()
        target_include_directories(akkaradb_api_grpc PRIVATE ${AKKARADB_GRPC_GENERATED_DIR})
        target_link_libraries(akkaradb_api_grpc PRIVATE ${AKKARADB_GRPCPP_TARGET} ${AKKARADB_PROTOBUF_TARGET})
        target_compile_definitions(akkaradb_api_grpc PRIVATE
                AKKARADB_GRPC_ENABLED
                _SILENCE_ALL_CXX23_DEPRECATION_WARNINGS
                _SILENCE_CXX23_ALIGNED_STORAGE_DEPRECATION_WARNING
        )
        target_compile_definitions(akkaradb_api PRIVATE AKKARADB_API_HAS_GRPC_BACKEND)
        message(STATUS "AkkaraDB: gRPC backend enabled")
    else ()
        message(FATAL_ERROR
                "AkkaraDB: gRPC API backend is enabled, but required gRPC/Protobuf targets "
                "were not available after package lookup and FetchContent")
    endif ()
endif ()

set_target_properties(akkaradb PROPERTIES
        LINKER_LANGUAGE CXX
        OUTPUT_NAME "${AKKARADB_NATIVE_OUTPUT_NAME}"
)
if (TARGET akkaradb_api)
    set_target_properties(akkaradb_api PROPERTIES
            LINKER_LANGUAGE CXX
            OUTPUT_NAME "${AKKARADB_API_OUTPUT_NAME}"
            CXX_VISIBILITY_PRESET hidden
            VISIBILITY_INLINES_HIDDEN ON
            VERSION ${PROJECT_VERSION}
            SOVERSION ${PROJECT_VERSION_MAJOR}
    )
endif ()
if (TARGET akkaradb_api_http)
    set_target_properties(akkaradb_api_http PROPERTIES
            LINKER_LANGUAGE CXX
            OUTPUT_NAME "${AKKARADB_API_HTTP_OUTPUT_NAME}"
            CXX_VISIBILITY_PRESET hidden
            VISIBILITY_INLINES_HIDDEN ON
            VERSION ${PROJECT_VERSION}
            SOVERSION ${PROJECT_VERSION_MAJOR}
    )
endif ()
if (TARGET akkaradb_api_tcp)
    set_target_properties(akkaradb_api_tcp PROPERTIES
            LINKER_LANGUAGE CXX
            OUTPUT_NAME "${AKKARADB_API_TCP_OUTPUT_NAME}"
            CXX_VISIBILITY_PRESET hidden
            VISIBILITY_INLINES_HIDDEN ON
            VERSION ${PROJECT_VERSION}
            SOVERSION ${PROJECT_VERSION_MAJOR}
    )
endif ()
if (TARGET akkaradb_api_grpc)
    set_target_properties(akkaradb_api_grpc PROPERTIES
            LINKER_LANGUAGE CXX
            OUTPUT_NAME "${AKKARADB_API_GRPC_OUTPUT_NAME}"
            CXX_VISIBILITY_PRESET hidden
            VISIBILITY_INLINES_HIDDEN ON
            VERSION ${PROJECT_VERSION}
            SOVERSION ${PROJECT_VERSION_MAJOR}
    )
endif ()
set_target_properties(akkaradb_cluster PROPERTIES
        LINKER_LANGUAGE CXX
        OUTPUT_NAME "${AKKARADB_CLUSTER_OUTPUT_NAME}"
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN ON
        VERSION ${PROJECT_VERSION}
        SOVERSION ${PROJECT_VERSION_MAJOR}
)

set(AKKARADB_SHARED_PLUGIN_TARGETS akkaradb_cluster)
set(AKKARADB_INSTALL_PLUGIN_TARGETS akkaradb_cluster)
set(AKKARADB_SDK_PACKAGE_TARGET akkaradb_cluster)
if (TARGET akkaradb_api)
    list(PREPEND AKKARADB_INSTALL_PLUGIN_TARGETS akkaradb_api)
    list(APPEND AKKARADB_SHARED_PLUGIN_TARGETS ${AKKARADB_BUILT_API_BACKEND_TARGETS})
    list(APPEND AKKARADB_INSTALL_PLUGIN_TARGETS ${AKKARADB_BUILT_API_BACKEND_TARGETS})
    set(AKKARADB_SDK_PACKAGE_TARGET akkaradb_api)
endif ()

if (WIN32)
    if (TARGET akkaradb_api)
        set_target_properties(akkaradb_api PROPERTIES
                WINDOWS_EXPORT_ALL_SYMBOLS OFF
                ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
                RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
        )
    endif ()
    foreach (AKKARADB_OPTIONAL_BACKEND_TARGET IN LISTS AKKARADB_SHARED_PLUGIN_TARGETS)
        set_target_properties(${AKKARADB_OPTIONAL_BACKEND_TARGET} PROPERTIES
                WINDOWS_EXPORT_ALL_SYMBOLS OFF
                ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
                RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
        )
    endforeach ()
endif ()

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
                WINDOWS_EXPORT_ALL_SYMBOLS OFF
                ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
                RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
        )
    endif ()
    message(STATUS "AkkaraDB: building as SHARED library (.dll / .so)")
else ()
    target_compile_definitions(akkaradb PUBLIC AKKARADB_STATIC)
    message(STATUS "AkkaraDB: building as STATIC library (.lib / .a)")
endif ()

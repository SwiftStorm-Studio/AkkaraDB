# ==================== Tests ====================
if (AKKARADB_BUILD_TESTS)
    enable_testing()
    file(MAKE_DIRECTORY "${AKKARADB_TEST_OUTPUT_DIR}")
endif ()

# ==================== Benchmarks ====================
message(STATUS "Building AkkaraDB benchmarks")

set(AKKARADB_DISKSPD_URL "https://github.com/microsoft/diskspd/releases/latest/download/DiskSpd.zip")
if (CMAKE_SYSTEM_PROCESSOR MATCHES "^[Aa][Rr][Mm]64$")
    set(AKKARADB_DISKSPD_ARCH_SUBDIR "arm64")
elseif (CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(AKKARADB_DISKSPD_ARCH_SUBDIR "amd64")
else ()
    set(AKKARADB_DISKSPD_ARCH_SUBDIR "x86")
endif ()
set(AKKARADB_DISKSPD_WORK_DIR "${CMAKE_BINARY_DIR}/_deps/diskspd")

add_custom_target(akkaradb_prepare_diskspd
        COMMAND ${CMAKE_COMMAND}
        -DDISKSPD_URL=${AKKARADB_DISKSPD_URL}
        -DDISKSPD_ARCH_SUBDIR=${AKKARADB_DISKSPD_ARCH_SUBDIR}
        -DDISKSPD_WORK_DIR=${AKKARADB_DISKSPD_WORK_DIR}
        -DDISKSPD_OUTPUT_DIR=${CMAKE_BINARY_DIR}/bin
        -DDISKSPD_DIST_DIR=${AKKARADB_NATIVE_DIST_DIR}
        -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/EnsureDiskSpd.cmake"
        COMMENT "Preparing DiskSpd benchmark tool"
)

add_executable(akkaradb_benchmark
        benchmarks/suite/benchmark.cpp
)
add_executable(akkaradb_parallel_memtable_visibility_rate_benchmark
        benchmarks/throughput/parallel_memtable_visibility_rate_benchmark.cpp
)
add_executable(akkaradb_vlog_tool
        benchmarks/tools/akkaradb_vlog_tool.cpp
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
target_link_libraries(akkaradb_parallel_memtable_visibility_rate_benchmark PRIVATE akkaradb)
target_link_libraries(akkaradb_vlog_tool PRIVATE akkaradb)
target_include_directories(akkaradb_benchmark PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)
target_include_directories(akkaradb_parallel_memtable_visibility_rate_benchmark PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)
target_include_directories(akkaradb_vlog_tool PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)
set_target_properties(akkaradb_benchmark PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
set_target_properties(akkaradb_parallel_memtable_visibility_rate_benchmark PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
set_target_properties(akkaradb_vlog_tool PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

set(AKKARADB_API_SMOKE_TEST_TARGETS "")
if (TARGET akkaradb_api AND TARGET akkaradb_api_tcp)
    add_executable(akkaradb_api_server_smoke_test
            benchmarks/api/api_server_smoke_test.cpp
    )
    add_executable(akkaradb_api_server_test
            benchmarks/api/api_server_test.cpp
    )
    add_executable(akkaradb_tcp_api_throughput_benchmark
            benchmarks/api/tcp_api_throughput_benchmark.cpp
    )
    foreach (AKKARADB_API_EXE IN ITEMS
        akkaradb_api_server_smoke_test
        akkaradb_api_server_test
        akkaradb_tcp_api_throughput_benchmark
    )
        if(MSVC)
            target_compile_options(${AKKARADB_API_EXE} PRIVATE /WX-)
        endif()
        target_include_directories(${AKKARADB_API_EXE} PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
                ${AKKARADB_INCLUDE_DIR}
                ${AKKENGINE_INCLUDE_DIR}
        )
        set_target_properties(${AKKARADB_API_EXE} PROPERTIES
                RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
        )
    endforeach ()
    target_link_libraries(akkaradb_api_server_smoke_test PRIVATE akkaradb akkaradb_api akkaradb_api_tcp)
    target_link_libraries(akkaradb_api_server_test PRIVATE akkaradb akkaradb_api akkaradb_api_tcp)
    target_link_libraries(akkaradb_tcp_api_throughput_benchmark PRIVATE akkaradb akkaradb_api akkaradb_api_tcp)
    if (TARGET akkaradb_api_grpc AND DEFINED AKKARADB_GRPC_GENERATED_SOURCES AND DEFINED AKKARADB_GRPC_GENERATED_DIR)
        target_sources(akkaradb_api_server_smoke_test PRIVATE ${AKKARADB_GRPC_GENERATED_SOURCES})
        target_include_directories(akkaradb_api_server_smoke_test PRIVATE ${AKKARADB_GRPC_GENERATED_DIR})
        target_link_libraries(akkaradb_api_server_smoke_test PRIVATE ${AKKARADB_GRPCPP_TARGET} ${AKKARADB_PROTOBUF_TARGET})
        target_compile_definitions(akkaradb_api_server_smoke_test PRIVATE AKKARADB_TEST_HAS_GRPC)
        add_dependencies(akkaradb_api_server_smoke_test akkaradb_grpc_codegen)
        if (MSVC)
            set_source_files_properties(${AKKARADB_GRPC_GENERATED_SOURCES} PROPERTIES COMPILE_OPTIONS "/WX-")
        endif()
    endif ()
    if(WIN32)
        target_link_libraries(akkaradb_tcp_api_throughput_benchmark PRIVATE ws2_32)
    endif()
    list(APPEND AKKARADB_API_SMOKE_TEST_TARGETS akkaradb_api_server_smoke_test)
endif ()

set(AKKARADB_COMMON_TEST_TARGETS
        akkaradb_memtable_throughput_benchmark|benchmarks/throughput/memtable_throughput_benchmark.cpp
        akkaradb_akkengine_bptree_put_benchmark|benchmarks/throughput/akkengine_bptree_put_benchmark.cpp
        akkaradb_wal_throughput_benchmark|benchmarks/throughput/wal_throughput_benchmark.cpp
        akkaradb_sstable_throughput_benchmark|benchmarks/throughput/sstable_throughput_benchmark.cpp
        akkaradb_sstable_bloom_negative_lookup_benchmark|benchmarks/throughput/sstable_bloom_negative_lookup_benchmark.cpp
        akkaradb_bptree_mutable_concurrency_stress_test|benchmarks/smoke/bptree_mutable_concurrency_stress_test.cpp
        akkaradb_cluster_smoke_test|benchmarks/smoke/cluster_smoke_test.cpp
        akkaradb_memtable_lifecycle_smoke_test|benchmarks/smoke/memtable_lifecycle_smoke_test.cpp
        akkaradb_parallel_memtable_visibility_smoke_test|benchmarks/smoke/parallel_memtable_visibility_smoke_test.cpp
        akkaradb_query_planner_smoke_test|benchmarks/smoke/query_planner_smoke_test.cpp
        akkaradb_version_log_admission_visibility_smoke_test|benchmarks/smoke/version_log_admission_visibility_smoke_test.cpp
        akkaradb_version_log_fault_injection_smoke_test|benchmarks/smoke/version_log_fault_injection_smoke_test.cpp
        akkaradb_version_log_recovery_concurrency_smoke_test|benchmarks/smoke/version_log_recovery_concurrency_smoke_test.cpp
        akkaradb_version_log_tool_smoke_test|benchmarks/smoke/version_log_tool_smoke_test.cpp
        akkaradb_engine_recovery_smoke_test|benchmarks/smoke/engine_recovery_smoke_test.cpp
        akkaradb_wal_async_failure_smoke_test|benchmarks/smoke/wal_async_failure_smoke_test.cpp
        akkaradb_sst_snapshot_visibility_smoke_test|benchmarks/smoke/sst_snapshot_visibility_smoke_test.cpp
)

foreach (AKKARADB_TARGET_SPEC IN LISTS AKKARADB_COMMON_TEST_TARGETS)
    string(REPLACE "|" ";" AKKARADB_TARGET_PARTS "${AKKARADB_TARGET_SPEC}")
    list(GET AKKARADB_TARGET_PARTS 0 AKKARADB_TARGET_NAME)
    list(GET AKKARADB_TARGET_PARTS 1 AKKARADB_TARGET_SOURCE)
    add_executable(${AKKARADB_TARGET_NAME} ${AKKARADB_TARGET_SOURCE})
    if(MSVC)
        target_compile_options(${AKKARADB_TARGET_NAME} PRIVATE /WX-)
    endif()
    target_link_libraries(${AKKARADB_TARGET_NAME} PRIVATE akkaradb)
    target_include_directories(${AKKARADB_TARGET_NAME} PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
            ${AKKARADB_INCLUDE_DIR}
            ${AKKENGINE_INCLUDE_DIR}
    )
    set_target_properties(${AKKARADB_TARGET_NAME} PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )
endforeach ()

add_dependencies(akkaradb_memtable_throughput_benchmark akkaradb_prepare_diskspd)
add_dependencies(akkaradb_sstable_throughput_benchmark akkaradb_prepare_diskspd)

add_custom_command(TARGET akkaradb_memtable_throughput_benchmark POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory "${AKKARADB_NATIVE_DIST_DIR}"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        $<TARGET_FILE:akkaradb_memtable_throughput_benchmark>
        "${AKKARADB_NATIVE_DIST_DIR}/$<TARGET_FILE_NAME:akkaradb_memtable_throughput_benchmark>"
        COMMENT "Copying memtable throughput benchmark to dist/native/${AKKARADB_RELEASE_VERSION}"
)

add_custom_command(TARGET akkaradb_sstable_throughput_benchmark POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory "${AKKARADB_NATIVE_DIST_DIR}"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
        $<TARGET_FILE:akkaradb_sstable_throughput_benchmark>
        "${AKKARADB_NATIVE_DIST_DIR}/$<TARGET_FILE_NAME:akkaradb_sstable_throughput_benchmark>"
        COMMENT "Copying sstable throughput benchmark to dist/native/${AKKARADB_RELEASE_VERSION}"
)

target_link_libraries(akkaradb_cluster_smoke_test PRIVATE akkaradb_cluster)

if (WIN32 AND BUILD_SHARED_LIBS)
    add_custom_command(TARGET akkaradb_benchmark POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:akkaradb>
            $<TARGET_FILE_DIR:akkaradb_benchmark>
            COMMENT "Copying akkaradb.dll to benchmark executable directory"
    )
    add_custom_command(TARGET akkaradb_vlog_tool POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:akkaradb>
            $<TARGET_FILE_DIR:akkaradb_vlog_tool>
            COMMENT "Copying akkaradb.dll to VLog tool executable directory"
    )
endif()

set(AKKARADB_SMOKE_TEST_TARGETS
        akkaradb_bptree_mutable_concurrency_stress_test
        akkaradb_cluster_smoke_test
        akkaradb_memtable_lifecycle_smoke_test
        akkaradb_parallel_memtable_visibility_smoke_test
        akkaradb_query_planner_smoke_test
        akkaradb_version_log_admission_visibility_smoke_test
        akkaradb_version_log_fault_injection_smoke_test
        akkaradb_version_log_recovery_concurrency_smoke_test
        akkaradb_version_log_tool_smoke_test
        akkaradb_engine_recovery_smoke_test
        akkaradb_wal_async_failure_smoke_test
        akkaradb_sst_snapshot_visibility_smoke_test
)
list(APPEND AKKARADB_SMOKE_TEST_TARGETS ${AKKARADB_API_SMOKE_TEST_TARGETS})

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
            if (TARGET akkaradb_api)
                add_custom_command(TARGET ${AKKARADB_TEST_TARGET} POST_BUILD
                        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        $<TARGET_FILE:akkaradb_api>
                        $<TARGET_FILE_DIR:${AKKARADB_TEST_TARGET}>
                        COMMENT "Copying akkaradb API backend DLL to ${AKKARADB_TEST_TARGET} output directory"
                )
            endif ()
            foreach (AKKARADB_OPTIONAL_BACKEND_TARGET IN LISTS AKKARADB_SHARED_PLUGIN_TARGETS)
                add_custom_command(TARGET ${AKKARADB_TEST_TARGET} POST_BUILD
                        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                        $<TARGET_FILE:${AKKARADB_OPTIONAL_BACKEND_TARGET}>
                        $<TARGET_FILE_DIR:${AKKARADB_TEST_TARGET}>
                        COMMENT "Copying optional AkkaraDB backend DLL to ${AKKARADB_TEST_TARGET} output directory"
                )
            endforeach ()
        endif ()
    endforeach ()
endif ()

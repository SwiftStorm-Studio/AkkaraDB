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
        akkaradb_transaction_smoke_test|benchmarks/smoke/transaction_smoke_test.cpp
        akkaradb_arena_smoke_test|benchmarks/smoke/arena_smoke_test.cpp
        akkaradb_memtable_throughput_benchmark|benchmarks/throughput/memtable_throughput_benchmark.cpp
        akkaradb_akkengine_bptree_put_benchmark|benchmarks/throughput/akkengine_bptree_put_benchmark.cpp
        akkaradb_wal_throughput_benchmark|benchmarks/throughput/wal_throughput_benchmark.cpp
        akkaradb_sstable_throughput_benchmark|benchmarks/throughput/sstable_throughput_benchmark.cpp
        akkaradb_sstable_bloom_negative_lookup_benchmark|benchmarks/throughput/sstable_bloom_negative_lookup_benchmark.cpp
        akkaradb_bptree_mutable_concurrency_stress_test|benchmarks/smoke/bptree_mutable_concurrency_stress_test.cpp
        akkaradb_cluster_observability_test|benchmarks/smoke/cluster_observability_test.cpp
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
target_link_libraries(akkaradb_cluster_observability_test PRIVATE akkaradb_cluster)
add_custom_target(akkaradb_cluster_soak
        COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --soak 0xA44A5EED 1
        DEPENDS akkaradb_cluster_smoke_test
        USES_TERMINAL
        COMMENT "Running deterministic cluster crash/latency/disconnect/leader/snapshot/transfer soak"
)
if (WIN32)
    target_link_libraries(akkaradb_cluster_smoke_test PRIVATE ws2_32)
    target_link_libraries(akkaradb_cluster_observability_test PRIVATE ws2_32)
endif ()

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
        akkaradb_transaction_smoke_test
        akkaradb_arena_smoke_test
        akkaradb_bptree_mutable_concurrency_stress_test
        akkaradb_cluster_observability_test
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
    if (WIN32 AND BUILD_SHARED_LIBS)
        add_custom_command(TARGET akkaradb POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E make_directory "${AKKARADB_TEST_OUTPUT_DIR}"
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_FILE:akkaradb> "${AKKARADB_TEST_OUTPUT_DIR}"
                COMMENT "Refreshing engine DLL for native tests"
        )
        # The import library can stay unchanged after a backend implementation
        # edit, leaving test executables unlinked and their POST_BUILD unrun.
        add_custom_command(TARGET akkaradb_cluster POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E make_directory "${AKKARADB_TEST_OUTPUT_DIR}"
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_FILE:akkaradb_cluster> "${AKKARADB_TEST_OUTPUT_DIR}"
                COMMENT "Refreshing cluster backend DLL for native tests"
        )
    endif ()
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
    set_tests_properties(akkaradb_cluster_observability_test PROPERTIES LABELS "cluster;observability" TIMEOUT 90)
    add_test(NAME akkaradb_raft_learners_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --raft-learners)
    add_test(NAME akkaradb_mirror_fencing_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --mirror-fencing)
    add_test(NAME akkaradb_mirror_recovery_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --mirror-recovery)
    set_tests_properties(akkaradb_mirror_recovery_test PROPERTIES LABELS "cluster;mirror;recovery" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_routing_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --routing)
    add_test(NAME akkaradb_raft_prevote_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --raft-prevote)
    add_test(NAME akkaradb_raft_contract_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --raft-contract)
    add_test(NAME akkaradb_raft_storage_contract_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --raft-storage-contract)
    add_test(NAME akkaradb_partition_recovery_safety_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --partition-recovery-safety)
    add_test(NAME akkaradb_transaction_term_safety_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --transaction-term-safety)
    set_tests_properties(akkaradb_partition_recovery_safety_test akkaradb_transaction_term_safety_test
            PROPERTIES LABELS "cluster;consistency;regression" TIMEOUT 240 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_transaction_storage_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --transaction-storage-contract)
    set_tests_properties(akkaradb_cluster_transaction_storage_test PROPERTIES LABELS "cluster;transactions;consistency;regression" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_stripe_storage_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --stripe-storage-contract)
    set_tests_properties(akkaradb_stripe_storage_test PROPERTIES LABELS "cluster;stripe;consistency;regression" TIMEOUT 120 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_raft_storage_contract_test PROPERTIES LABELS "cluster;raft;consistency;regression" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_raft_snapshot_boundary_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --raft-snapshot-boundary)
    set_tests_properties(akkaradb_raft_snapshot_boundary_test PROPERTIES LABELS "cluster;raft;snapshot;recovery" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_raft_snapshot_integrity_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --raft-snapshot-integrity)
    set_tests_properties(akkaradb_raft_snapshot_integrity_test PROPERTIES LABELS "cluster;raft;snapshot;consistency" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_stripe_read_consistency_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --stripe-read-consistency)
    set_tests_properties(akkaradb_stripe_read_consistency_test PROPERTIES LABELS "cluster;stripe;consistency" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_raft_transaction_policy_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --raft-transaction-policy)
    set_tests_properties(akkaradb_raft_transaction_policy_test PROPERTIES LABELS "cluster;raft;transactions;compatibility" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_p1_regressions_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-p1-regressions)
    add_test(NAME akkaradb_cluster_recovery_fencing_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-recovery-fencing)
    set_tests_properties(akkaradb_cluster_recovery_fencing_test PROPERTIES LABELS "cluster;replication;stripe;consistency;recovery;regression" TIMEOUT 120 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_raft_sequence_gap_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --raft-sequence-gap)
    add_test(NAME akkaradb_cluster_read_apply_order_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-read-apply-order)
    set_tests_properties(akkaradb_raft_sequence_gap_test akkaradb_cluster_read_apply_order_test
            PROPERTIES LABELS "cluster;raft;transactions;regression" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_integrity_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-integrity)
    add_test(NAME akkaradb_raft_state_recovery_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --raft-state-recovery)
    add_test(NAME akkaradb_stripe_state_recovery_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --stripe-state-recovery)
    set_tests_properties(akkaradb_stripe_state_recovery_test PROPERTIES LABELS "cluster;stripe;recovery;regression" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_nonraft_close_ack_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --nonraft-close-ack)
    add_test(NAME akkaradb_raft_apply_recovery_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --raft-apply-recovery)
    set_tests_properties(akkaradb_nonraft_close_ack_test akkaradb_raft_apply_recovery_test
            PROPERTIES LABELS "cluster;replication;recovery;history;regression" TIMEOUT 120 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_raft_state_recovery_test PROPERTIES LABELS "cluster;raft;recovery;regression" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_stripe_history_migration_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --stripe-history-migration)
    set_tests_properties(akkaradb_stripe_history_migration_test PROPERTIES LABELS "cluster;stripe;history;regression" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_integrity_test PROPERTIES LABELS "cluster;replication;transactions;history" TIMEOUT 240 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_connection_deadline_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-connection-deadline)
    add_test(NAME akkaradb_nonraft_liveness_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --nonraft-liveness)
    set_tests_properties(akkaradb_nonraft_liveness_test PROPERTIES LABELS "cluster;replication;liveness;deadline" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_connection_deadline_test PROPERTIES LABELS "cluster;raft;replication;deadline;shutdown" TIMEOUT 30 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_p1_regressions_test PROPERTIES LABELS "cluster;stripe;raft;regression;deadline" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_raft_contract_test PROPERTIES LABELS "cluster;raft;transactions;concurrency" TIMEOUT 240 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_queries_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-queries)
    add_test(NAME akkaradb_cluster_authority_contract_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --authority-contract)
    add_test(NAME akkaradb_stripe_parallelism_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --stripe-parallelism)
    set_tests_properties(akkaradb_stripe_parallelism_test PROPERTIES LABELS "cluster;stripe;concurrency" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_raft_log_delta_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --raft-log-delta)
    set_tests_properties(akkaradb_raft_log_delta_test PROPERTIES LABELS "cluster;raft;recovery" TIMEOUT 120 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_raft_log_retention_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --raft-log-retention)
    set_tests_properties(akkaradb_raft_log_retention_test PROPERTIES LABELS "cluster;raft;recovery;history" TIMEOUT 240 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_partition_placement_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --partition-placement)
    add_test(NAME akkaradb_cluster_placement_options_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --placement-options)
    set_tests_properties(akkaradb_cluster_placement_options_test PROPERTIES LABELS "cluster;placement;deadline" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_partition_raft_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --partition-raft)
    add_test(NAME akkaradb_cluster_partition_query_parallelism_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --partition-query-parallelism)
    add_test(NAME akkaradb_cluster_availability_options_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --availability-options)
    set_tests_properties(akkaradb_cluster_availability_options_test PROPERTIES LABELS "cluster;stripe;availability" TIMEOUT 30 RESOURCE_LOCK akkaradb_cluster_loopback)
    foreach(availability_case parity-plain parity-secure mirrors-plain mirrors-secure multiple-spares)
        add_test(NAME akkaradb_cluster_availability_${availability_case}_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --availability-${availability_case})
        set_tests_properties(akkaradb_cluster_availability_${availability_case}_test PROPERTIES LABELS "cluster;stripe;availability" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    endforeach()
    set_tests_properties(akkaradb_cluster_partition_query_parallelism_test PROPERTIES LABELS "cluster;partition;queries;concurrency" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_partition_raft_test PROPERTIES LABELS "cluster;partition;raft" TIMEOUT 360 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_partition_placement_test PROPERTIES LABELS "cluster;partition;placement" TIMEOUT 300 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_query_delivery_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --query-delivery)
    add_test(NAME akkaradb_cluster_transactions_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-transactions-basic)
    set_tests_properties(akkaradb_cluster_transactions_test PROPERTIES LABELS "cluster;transactions;recovery" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_transaction_visibility_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-transactions-visibility)
    set_tests_properties(akkaradb_cluster_transaction_visibility_test PROPERTIES LABELS "cluster;transactions" TIMEOUT 60 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_transaction_capacity_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-transactions-capacity)
    add_test(NAME akkaradb_cluster_transaction_concurrency_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-transactions-concurrency-plain)
    add_test(NAME akkaradb_cluster_transaction_concurrency_secure_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-transactions-concurrency-secure)
    add_test(NAME akkaradb_cluster_transaction_unused_partition_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-transactions-unused-failure)
    add_test(NAME akkaradb_cluster_transaction_concurrent_capacity_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-transactions-concurrent-capacity)
    set_tests_properties(akkaradb_cluster_transaction_concurrency_secure_test akkaradb_cluster_transaction_unused_partition_test akkaradb_cluster_transaction_concurrent_capacity_test PROPERTIES LABELS "cluster;transactions;concurrency;recovery" TIMEOUT 360 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_transaction_reservations_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-transactions-reservations)
    add_test(NAME akkaradb_cluster_transaction_prepare_scaling_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-transactions-prepare-scaling)
    add_test(NAME akkaradb_cluster_transaction_recovery_fairness_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-transactions-recovery-fairness)
    add_test(NAME akkaradb_cluster_transaction_prepared_snapshot_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-transactions-prepared-snapshot)
    add_test(NAME akkaradb_cluster_transaction_prepare_integrity_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-transactions-prepare-integrity)
    add_test(NAME akkaradb_cluster_transaction_expired_reservation_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-transactions-expired-reservation)
    set_tests_properties(akkaradb_cluster_transaction_expired_reservation_test PROPERTIES LABELS "cluster;transactions;recovery" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_node_incoming_resources_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --node-resources-incoming)
    set_tests_properties(akkaradb_cluster_node_incoming_resources_test PROPERTIES LABELS "cluster;raft;resources" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_transaction_prepare_scaling_test akkaradb_cluster_transaction_recovery_fairness_test akkaradb_cluster_transaction_prepared_snapshot_test akkaradb_cluster_transaction_prepare_integrity_test PROPERTIES LABELS "cluster;transactions;concurrency;recovery" TIMEOUT 240 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_transaction_reservations_test PROPERTIES LABELS "cluster;transactions;recovery" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_fault_domains_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --fault-domains)
    add_test(NAME akkaradb_cluster_failure_retention_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-failure-retention)
    add_test(NAME akkaradb_cluster_retained_requests_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-retained-requests)
    add_test(NAME akkaradb_cluster_retained_transactions_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-retained-transactions)
    set_tests_properties(akkaradb_cluster_failure_retention_test PROPERTIES LABELS "cluster;raft;requests;snapshot" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_retained_requests_test akkaradb_cluster_retained_transactions_test PROPERTIES LABELS "cluster;raft;requests;transactions;recovery" TIMEOUT 600 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_retained_transactions_test PROPERTIES TIMEOUT 1200)
    add_test(NAME akkaradb_cluster_request_retention_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --request-retention)
    add_test(NAME akkaradb_cluster_local_request_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --local-request-application)
    set_tests_properties(akkaradb_cluster_local_request_test PROPERTIES LABELS "cluster;requests;storage;recovery" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_request_retention_test PROPERTIES LABELS "cluster;requests;storage" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_nonraft_request_storage_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --nonraft-request-storage)
    add_test(NAME akkaradb_cluster_reconnect_durable_ack_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --reconnect-durable-ack)
    add_test(NAME akkaradb_cluster_rollback_catchup_history_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --rollback-catchup-history)
    set_tests_properties(akkaradb_cluster_reconnect_durable_ack_test akkaradb_cluster_rollback_catchup_history_test
        PROPERTIES LABELS "cluster;replication;recovery;regression" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_request_authority_membership_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --request-authority-membership)
    set_tests_properties(akkaradb_cluster_request_authority_membership_test PROPERTIES LABELS "cluster;requests;membership;recovery" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_nonraft_request_storage_test PROPERTIES LABELS "cluster;requests;storage" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_nonraft_requests_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --nonraft-requests)
    set_tests_properties(akkaradb_cluster_nonraft_requests_test PROPERTIES LABELS "cluster;requests;recovery" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_write_outcomes_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --write-outcomes)
    set_tests_properties(akkaradb_cluster_fault_domains_test PROPERTIES LABELS "cluster;placement;fault-domains" TIMEOUT 90 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_write_outcomes_test PROPERTIES LABELS "cluster;raft;retry;outcome" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_node_resources_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --node-resources)
    add_test(NAME akkaradb_cluster_node_resources_transfer_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --node-resources-transfer)
    set_tests_properties(akkaradb_cluster_node_resources_transfer_test PROPERTIES LABELS "cluster;resources;transfer" TIMEOUT 240 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_node_resources_test PROPERTIES LABELS "cluster;resources;concurrency" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_transaction_concurrency_test PROPERTIES LABELS "cluster;transactions;concurrency;recovery" TIMEOUT 360 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_transaction_capacity_test PROPERTIES LABELS "cluster;transactions" TIMEOUT 120 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_transaction_recovery_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-transactions-recovery)
    set_tests_properties(akkaradb_cluster_transaction_recovery_test PROPERTIES LABELS "cluster;transactions;recovery" TIMEOUT 480 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_transaction_snapshot_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --cluster-transactions-snapshots)
    set_tests_properties(akkaradb_cluster_transaction_snapshot_test PROPERTIES LABELS "cluster;transactions;snapshot" TIMEOUT 240 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_query_delivery_test PROPERTIES LABELS "cluster;queries;streaming;history" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_query_resource_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --query-resources)
    add_test(NAME akkaradb_cluster_global_query_conflicts_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --global-query-conflicts)
    set_tests_properties(akkaradb_cluster_global_query_conflicts_test PROPERTIES LABELS "cluster;queries;consistency" TIMEOUT 240 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_transaction_recovery_diagnostics_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --transaction-recovery-diagnostics)
    set_tests_properties(akkaradb_cluster_transaction_recovery_diagnostics_test PROPERTIES LABELS "cluster;transactions;observability" TIMEOUT 120 RESOURCE_LOCK akkaradb_cluster_loopback)
    add_test(NAME akkaradb_cluster_global_queries_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --global-query-consistency)
    set_tests_properties(akkaradb_cluster_global_queries_test PROPERTIES LABELS "cluster;queries;consistency" TIMEOUT 240 RESOURCE_LOCK akkaradb_cluster_loopback)
    foreach(mode IN ITEMS delivery mirror stripe)
        add_test(NAME akkaradb_cluster_global_query_${mode}_test COMMAND $<TARGET_FILE:akkaradb_cluster_smoke_test> --global-query-${mode})
        set_tests_properties(akkaradb_cluster_global_query_${mode}_test PROPERTIES LABELS "cluster;queries;consistency" TIMEOUT 240 RESOURCE_LOCK akkaradb_cluster_loopback)
    endforeach()
    set_tests_properties(akkaradb_cluster_query_resource_test PROPERTIES LABELS "cluster;queries;deadline" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_authority_contract_test PROPERTIES LABELS "cluster;fencing;contract" TIMEOUT 180 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_cluster_queries_test PROPERTIES LABELS "cluster;queries" TIMEOUT 600 RESOURCE_LOCK akkaradb_cluster_loopback)
    set_tests_properties(akkaradb_raft_prevote_test PROPERTIES LABELS "cluster;raft;prevote" TIMEOUT 180)
    set_tests_properties(akkaradb_cluster_routing_test PROPERTIES LABELS "cluster;routing" TIMEOUT 240)
    set_tests_properties(akkaradb_mirror_fencing_test PROPERTIES LABELS "cluster;mirror;fencing" TIMEOUT 300)
    set_tests_properties(akkaradb_raft_learners_test PROPERTIES LABELS "cluster;raft;learners" TIMEOUT 240)
    set_tests_properties(akkaradb_cluster_smoke_test akkaradb_raft_learners_test akkaradb_mirror_fencing_test akkaradb_cluster_routing_test akkaradb_raft_prevote_test
            PROPERTIES RESOURCE_LOCK akkaradb_cluster_loopback)
endif ()

if (AKKARADB_BUILD_TESTS)
    set_tests_properties(akkaradb_transaction_smoke_test PROPERTIES TIMEOUT 180 LABELS "transactions;recovery")
endif ()

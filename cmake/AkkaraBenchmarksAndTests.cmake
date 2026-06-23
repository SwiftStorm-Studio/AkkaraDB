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
target_include_directories(akkaradb_benchmark PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)
set_target_properties(akkaradb_benchmark PROPERTIES
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
    if(WIN32)
        target_link_libraries(akkaradb_tcp_api_throughput_benchmark PRIVATE ws2_32)
    endif()
    list(APPEND AKKARADB_API_SMOKE_TEST_TARGETS akkaradb_api_server_smoke_test)
endif ()

set(AKKARADB_COMMON_TEST_TARGETS
        akkaradb_akkengine_smoke_test|benchmarks/smoke/akkengine_smoke_test.cpp
        akkaradb_crypto_identity_smoke_test|benchmarks/smoke/crypto_identity_smoke_test.cpp
        akkaradb_typed_api_smoke_test|benchmarks/smoke/typed_api_smoke_test.cpp
        akkaradb_memtable_smoke_test|benchmarks/smoke/memtable_smoke_test.cpp
        akkaradb_memtable_throughput_benchmark|benchmarks/throughput/memtable_throughput_benchmark.cpp
        akkaradb_wal_throughput_benchmark|benchmarks/throughput/wal_throughput_benchmark.cpp
        akkaradb_sstable_throughput_benchmark|benchmarks/throughput/sstable_throughput_benchmark.cpp
        akkaradb_sstable_bloom_negative_lookup_benchmark|benchmarks/throughput/sstable_bloom_negative_lookup_benchmark.cpp
        akkaradb_versionlog_smoke_test|benchmarks/smoke/versionlog_smoke_test.cpp
        akkaradb_manifest_smoke_test|benchmarks/smoke/manifest_smoke_test.cpp
        akkaradb_cluster_smoke_test|benchmarks/smoke/cluster_smoke_test.cpp
        akkaradb_erasure_smoke_test|benchmarks/smoke/erasure_smoke_test.cpp
        akkaradb_blob_smoke_test|benchmarks/smoke/blob_smoke_test.cpp
        akkaradb_sstable_smoke_test|benchmarks/smoke/sstable_smoke_test.cpp
        akkaradb_wal_smoke_test|benchmarks/smoke/wal_smoke_test.cpp
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

target_link_libraries(akkaradb_cluster_smoke_test PRIVATE akkaradb_cluster)

add_executable(akkaradb_crc32c_smoke_test
        benchmarks/smoke/crc32c_smoke_test.cpp
        ${AKKENGINE_SRC_DIR}/cpu/crc32c/CRC32CRef.cpp
        ${AKKENGINE_SRC_DIR}/cpu/crc32c/CRC32CX86SSE42.cpp
        ${AKKENGINE_SRC_DIR}/cpu/crc32c/CRC32CX86AVX2.cpp
        ${AKKENGINE_SRC_DIR}/cpu/crc32c/CRC32CX86AVX512.cpp
        ${AKKENGINE_SRC_DIR}/cpu/crc32c/CRC32CArmCRC.cpp
        ${AKKENGINE_SRC_DIR}/cpu/crc32c/CRC32CDispatch.cpp
)
if(MSVC)
    target_compile_options(akkaradb_crc32c_smoke_test PRIVATE /WX-)
endif()
target_include_directories(akkaradb_crc32c_smoke_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
        ${AKKARADB_INCLUDE_DIR}
        ${AKKENGINE_INCLUDE_DIR}
)
set_target_properties(akkaradb_crc32c_smoke_test PROPERTIES
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

set(AKKARADB_SMOKE_TEST_TARGETS
        akkaradb_akkengine_smoke_test
        akkaradb_crypto_identity_smoke_test
        akkaradb_crc32c_smoke_test
        akkaradb_typed_api_smoke_test
        akkaradb_memtable_smoke_test
        akkaradb_versionlog_smoke_test
        akkaradb_manifest_smoke_test
        akkaradb_cluster_smoke_test
        akkaradb_erasure_smoke_test
        akkaradb_blob_smoke_test
        akkaradb_sstable_smoke_test
        akkaradb_wal_smoke_test
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

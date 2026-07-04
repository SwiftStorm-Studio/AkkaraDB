# ==================== Installation ====================
install(TARGETS akkaradb
        EXPORT AkkaraDBTargets
        LIBRARY DESTINATION lib COMPONENT SDK
        ARCHIVE DESTINATION lib COMPONENT SDK
        RUNTIME DESTINATION bin COMPONENT SDK
        FILE_SET HEADERS DESTINATION include COMPONENT SDK
        FILE_SET akkengine_headers DESTINATION include COMPONENT SDK
)
install(TARGETS ${AKKARADB_INSTALL_PLUGIN_TARGETS}
        EXPORT AkkaraDBTargets
        LIBRARY DESTINATION lib COMPONENT SDK
        ARCHIVE DESTINATION lib COMPONENT SDK
        RUNTIME DESTINATION bin COMPONENT SDK
)
install(TARGETS akkaradb_benchmark RUNTIME DESTINATION bin)

if (NOT BUILD_SHARED_LIBS)
    install(TARGETS
            libzstd_static
            akkaradb_monocypher
            everest
            p256m
            mbedtls
            tfpsacrypto
            mbedx509
            EXPORT AkkaraDBTargets
            LIBRARY DESTINATION lib COMPONENT SDK
            ARCHIVE DESTINATION lib COMPONENT SDK
            RUNTIME DESTINATION bin COMPONENT SDK
    )
    install(DIRECTORY "${mbedtls_SOURCE_DIR}/include/" DESTINATION include COMPONENT SDK)
    install(DIRECTORY "${mbedtls_SOURCE_DIR}/tf-psa-crypto/include/" DESTINATION include COMPONENT SDK)
    install(DIRECTORY
            "${zstd_SOURCE_DIR}/lib/"
            DESTINATION include/zstd
            COMPONENT SDK
            FILES_MATCHING
            PATTERN "*.h"
    )
    install(DIRECTORY
            "${monocypher_SOURCE_DIR}/src/"
            DESTINATION include/akkaradb/third_party/monocypher
            COMPONENT SDK
            FILES_MATCHING
            PATTERN "*.h"
    )
endif ()

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
install(EXPORT AkkaraDBTargets
        FILE AkkaraDBTargets.cmake
        NAMESPACE AkkaraDB::
        DESTINATION lib/cmake/AkkaraDB
        COMPONENT SDK
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

if (BUILD_SHARED_LIBS)
    set(AKKARADB_CXX_SDK_PACKAGE_DEPENDS ${AKKARADB_SDK_PACKAGE_TARGET})
    if (TARGET akkaradb_jni)
        list(APPEND AKKARADB_CXX_SDK_PACKAGE_DEPENDS akkaradb_jni)
    endif ()
    add_custom_target(akkaradb_package_cxx_sdk
            COMMAND ${CMAKE_COMMAND}
            -DBUILD_DIR="${CMAKE_BINARY_DIR}"
            -DINSTALL_CONFIG=$<CONFIG>
            -DRELEASE_DIST_DIR="${AKKARADB_RELEASE_DIST_DIR}"
            -DDIST_DIR="${AKKARADB_NATIVE_DIST_DIR}"
            -DEXTRACT_DIR="${AKKARADB_SDK_EXTRACT_DIR}"
            -DPACKAGE_NAME="${AKKARADB_SDK_PACKAGE_NAME}"
            -DZIP_PATH="${AKKARADB_SDK_ZIP}"
            -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/PackageSdkDist.cmake"
            DEPENDS ${AKKARADB_CXX_SDK_PACKAGE_DEPENDS}
            COMMENT "Packaging AkkaraDB SDK: dist/${AKKARADB_RELEASE_VERSION}/${AKKARADB_SDK_PACKAGE_NAME}"
    )
    if (WIN32)
        add_custom_target(akkaradb_package_release_artifacts
                COMMAND powershell.exe -ExecutionPolicy Bypass
                -File "${CMAKE_CURRENT_SOURCE_DIR}/scripts/package_release_artifacts.ps1"
                -SkipWindows
                DEPENDS akkaradb_package_cxx_sdk
                COMMENT "Packaging AkkaraDB Windows and Linux release artifacts"
        )
    endif ()
    add_custom_command(TARGET akkaradb POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E make_directory "${AKKARADB_NATIVE_DIST_DIR}"
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:akkaradb>
            "${AKKARADB_NATIVE_DIST_DIR}/${AKKARADB_NATIVE_DIST_LIBRARY_NAME}"
            COMMENT "Copying akkaradb native library to dist/native/${AKKARADB_RELEASE_VERSION}"
    )
    if (TARGET akkaradb_api)
        add_custom_command(TARGET akkaradb_api POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E make_directory "${AKKARADB_NATIVE_DIST_DIR}"
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_FILE:akkaradb_api>
                "${AKKARADB_NATIVE_DIST_DIR}/${AKKARADB_API_DIST_LIBRARY_NAME}"
                COMMENT "Copying akkaradb API backend library to dist/native/${AKKARADB_RELEASE_VERSION}"
        )
    endif ()
    add_custom_command(TARGET akkaradb_cluster POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E make_directory "${AKKARADB_NATIVE_DIST_DIR}"
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:akkaradb_cluster>
            "${AKKARADB_NATIVE_DIST_DIR}/${AKKARADB_CLUSTER_DIST_LIBRARY_NAME}"
            COMMENT "Copying AkkaraDB cluster backend library to dist/native/${AKKARADB_RELEASE_VERSION}"
    )
    if (TARGET akkaradb_api_http)
        add_custom_command(TARGET akkaradb_api_http POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E make_directory "${AKKARADB_NATIVE_DIST_DIR}"
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_FILE:akkaradb_api_http>
                "${AKKARADB_NATIVE_DIST_DIR}/${AKKARADB_API_HTTP_DIST_LIBRARY_NAME}"
                COMMENT "Copying AkkaraDB HTTP API backend library to dist/native/${AKKARADB_RELEASE_VERSION}"
        )
    endif ()
    if (TARGET akkaradb_api_tcp)
        add_custom_command(TARGET akkaradb_api_tcp POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E make_directory "${AKKARADB_NATIVE_DIST_DIR}"
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_FILE:akkaradb_api_tcp>
                "${AKKARADB_NATIVE_DIST_DIR}/${AKKARADB_API_TCP_DIST_LIBRARY_NAME}"
                COMMENT "Copying AkkaraDB TCP API backend library to dist/native/${AKKARADB_RELEASE_VERSION}"
        )
    endif ()
    if (TARGET akkaradb_api_grpc)
        add_custom_command(TARGET akkaradb_api_grpc POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E make_directory "${AKKARADB_NATIVE_DIST_DIR}"
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_FILE:akkaradb_api_grpc>
                "${AKKARADB_NATIVE_DIST_DIR}/${AKKARADB_API_GRPC_DIST_LIBRARY_NAME}"
                COMMENT "Copying AkkaraDB gRPC API backend library to dist/native/${AKKARADB_RELEASE_VERSION}"
        )
    endif ()

endif ()

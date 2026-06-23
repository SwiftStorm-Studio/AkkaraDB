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
    foreach (AKKARADB_OPTIONAL_BACKEND_TARGET IN LISTS AKKARADB_SHARED_PLUGIN_TARGETS)
        add_custom_command(TARGET ${AKKARADB_OPTIONAL_BACKEND_TARGET} POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E make_directory "${AKKARADB_NATIVE_DIST_DIR}"
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_FILE:${AKKARADB_OPTIONAL_BACKEND_TARGET}>
                "${AKKARADB_NATIVE_DIST_DIR}/$<TARGET_FILE_NAME:${AKKARADB_OPTIONAL_BACKEND_TARGET}>"
                COMMENT "Copying optional AkkaraDB backend library to dist/native/${AKKARADB_RELEASE_VERSION}"
        )
    endforeach ()
    add_custom_command(TARGET ${AKKARADB_SDK_PACKAGE_TARGET} POST_BUILD
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
endif ()

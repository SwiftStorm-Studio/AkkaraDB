# ==================== JNI Bridge ====================
if(AKKARADB_BUILD_JNI)
    find_package(JNI REQUIRED)
    add_library(akkaradb_jni SHARED
            ${AKKARA_ROOT}/jni/AkkaraJni.cpp
    )
    target_link_libraries(akkaradb_jni PRIVATE akkaradb)
    target_include_directories(akkaradb_jni PRIVATE
            ${JNI_INCLUDE_DIRS}
    )
    target_compile_definitions(akkaradb_jni PRIVATE
            AKKARADB_JNI_COMPAT_LINE="${AKKARADB_COMPAT_LINE}"
            AKKARADB_REQUIRED_NATIVE_GENERATION="${AKKARADB_NATIVE_GENERATION}"
    )
    set_target_properties(akkaradb_jni PROPERTIES
            CXX_STANDARD 23
            CXX_STANDARD_REQUIRED ON
            OUTPUT_NAME "${AKKARADB_JNI_OUTPUT_NAME}"
    )
    if(WIN32)
        set_target_properties(akkaradb_jni PROPERTIES
                RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
        )
    endif()

    add_custom_command(TARGET akkaradb_jni POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E make_directory "${AKKARADB_JNI_DIST_DIR}"
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:akkaradb_jni>
            "${AKKARADB_JNI_DIST_DIR}/$<TARGET_FILE_NAME:akkaradb_jni>"
            COMMAND ${CMAKE_COMMAND}
            -DDIST_DIR="${AKKARADB_NATIVE_DIST_DIR}"
            -DPACKAGE_DIR="${AKKARADB_NATIVE_PACKAGE_DIR}"
            -DPLATFORM="${AKKARADB_DIST_PLATFORM}"
            -DDETAIL_VERSION_BASE="${AKKARADB_DETAIL_VERSION_BASE}"
            -DPRERELEASE="${AKKARADB_PRERELEASE}"
            -DNATIVE_LIBRARY_NAME="${AKKARADB_NATIVE_DIST_LIBRARY_NAME}"
            -DJNI_DIST_DIR="${AKKARADB_JNI_DIST_DIR}"
            -DJNI_LIBRARY_NAME=$<TARGET_FILE_NAME:akkaradb_jni>
            -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/PackageNativeDist.cmake"
            COMMENT "Copying akkaradb JNI library to dist/jni/${AKKARADB_RELEASE_VERSION}"
    )

    message(STATUS "JNI bridge enabled target: akkaradb_jni")
endif()

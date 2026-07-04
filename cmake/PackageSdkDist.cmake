if (NOT DEFINED BUILD_DIR)
    message(FATAL_ERROR "BUILD_DIR is required")
endif ()
if (NOT DEFINED INSTALL_CONFIG)
    message(FATAL_ERROR "INSTALL_CONFIG is required")
endif ()
if (NOT DEFINED RELEASE_DIST_DIR)
    message(FATAL_ERROR "RELEASE_DIST_DIR is required")
endif ()
if (NOT DEFINED EXTRACT_DIR)
    message(FATAL_ERROR "EXTRACT_DIR is required")
endif ()
if (NOT DEFINED PACKAGE_NAME)
    message(FATAL_ERROR "PACKAGE_NAME is required")
endif ()
if (NOT DEFINED ZIP_PATH)
    message(FATAL_ERROR "ZIP_PATH is required")
endif ()

file(MAKE_DIRECTORY "${RELEASE_DIST_DIR}")
file(REMOVE_RECURSE "${EXTRACT_DIR}")

execute_process(
        COMMAND "${CMAKE_COMMAND}"
        --install "${BUILD_DIR}"
        --prefix "${EXTRACT_DIR}"
        --config "${INSTALL_CONFIG}"
        --component SDK
        RESULT_VARIABLE INSTALL_RESULT
)
if (NOT INSTALL_RESULT EQUAL 0)
    message(FATAL_ERROR "Failed to install AkkaraDB SDK component")
endif ()

if (DEFINED DIST_DIR)
    file(MAKE_DIRECTORY "${DIST_DIR}")
    file(GLOB STALE_LINUX_DIST_LIBS "${DIST_DIR}/libakkaradb*.so" "${DIST_DIR}/libakkaradb*.so.*")
    foreach (STALE_LINUX_DIST_LIB IN LISTS STALE_LINUX_DIST_LIBS)
        file(REMOVE "${STALE_LINUX_DIST_LIB}")
    endforeach ()
    file(REMOVE_RECURSE "${DIST_DIR}/include")
    if (EXISTS "${EXTRACT_DIR}/include")
        file(COPY "${EXTRACT_DIR}/include" DESTINATION "${DIST_DIR}")
    endif ()
endif ()

file(REMOVE "${ZIP_PATH}")

get_filename_component(EXTRACT_PARENT_DIR "${EXTRACT_DIR}" DIRECTORY)

execute_process(
        COMMAND "${CMAKE_COMMAND}" -E tar cf "${ZIP_PATH}" --format=zip -- "${PACKAGE_NAME}"
        WORKING_DIRECTORY "${EXTRACT_PARENT_DIR}"
        RESULT_VARIABLE PACKAGE_RESULT
)
if (NOT PACKAGE_RESULT EQUAL 0)
    message(FATAL_ERROR "Failed to create AkkaraDB SDK package: ${ZIP_PATH}")
endif ()

file(REMOVE_RECURSE "${EXTRACT_DIR}")

message(STATUS "AkkaraDB SDK package created: ${ZIP_PATH}")
if (DEFINED DIST_DIR)
    message(STATUS "AkkaraDB SDK include directory exported to: ${DIST_DIR}/include")
endif ()

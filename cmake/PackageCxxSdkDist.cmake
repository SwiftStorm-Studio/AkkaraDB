if (NOT DEFINED BUILD_DIR)
    message(FATAL_ERROR "BUILD_DIR is required")
endif ()
if (NOT DEFINED INSTALL_CONFIG)
    message(FATAL_ERROR "INSTALL_CONFIG is required")
endif ()
if (NOT DEFINED DIST_DIR)
    message(FATAL_ERROR "DIST_DIR is required")
endif ()
if (NOT DEFINED EXTRACT_DIR)
    message(FATAL_ERROR "EXTRACT_DIR is required")
endif ()

file(REMOVE_RECURSE "${EXTRACT_DIR}")
file(REMOVE_RECURSE "${DIST_DIR}")
file(MAKE_DIRECTORY "${EXTRACT_DIR}")

execute_process(
        COMMAND "${CMAKE_COMMAND}"
        --install "${BUILD_DIR}"
        --prefix "${EXTRACT_DIR}"
        --config "${INSTALL_CONFIG}"
        --component SDK
        RESULT_VARIABLE INSTALL_RESULT
)
if (NOT INSTALL_RESULT EQUAL 0)
    message(FATAL_ERROR "Failed to install AkkaraDB SDK component for CXX SDK export")
endif ()

if (NOT EXISTS "${EXTRACT_DIR}/include")
    message(FATAL_ERROR "CXX SDK export expected an include directory in the installed SDK component")
endif ()

file(MAKE_DIRECTORY "${DIST_DIR}")
file(COPY "${EXTRACT_DIR}/include" DESTINATION "${DIST_DIR}")

file(REMOVE_RECURSE "${EXTRACT_DIR}")

message(STATUS "AkkaraDB CXX SDK headers exported to: ${DIST_DIR}")

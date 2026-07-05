if (NOT DEFINED SOURCE_DIR)
    message(FATAL_ERROR "SOURCE_DIR is required")
endif ()
if (NOT DEFINED INSTALL_CONFIG)
    message(FATAL_ERROR "INSTALL_CONFIG is required")
endif ()
if (NOT DEFINED LINUX_PRESET)
    set(LINUX_PRESET "release-linux")
endif ()

set(WSL_PACKAGE_COMMAND
        "set -euo pipefail; command -v cmake >/dev/null || { echo 'cmake is required in WSL' >&2; exit 127; }; cmake --preset ${LINUX_PRESET}; cmake --build --preset ${LINUX_PRESET} --target akkaradb_package_cxx_sdk --config ${INSTALL_CONFIG}"
)

message(STATUS "Packaging AkkaraDB Linux CXX SDK via WSL preset '${LINUX_PRESET}'")
execute_process(
        COMMAND wsl.exe --cd "${SOURCE_DIR}" bash -lc "${WSL_PACKAGE_COMMAND}"
        RESULT_VARIABLE WSL_PACKAGE_RESULT
)
if (NOT WSL_PACKAGE_RESULT EQUAL 0)
    message(FATAL_ERROR "Linux CXX SDK packaging failed with exit code ${WSL_PACKAGE_RESULT}")
endif ()

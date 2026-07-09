include_guard(GLOBAL)

set(AKKARADB_QUERY_REWRITE_CONFIGURE_PRESET "windows-clang-cl-release" CACHE STRING
        "CMake configure preset used inside the copied query-rewrite source tree")
set(AKKARADB_QUERY_REWRITE_BUILD_PRESET "${AKKARADB_QUERY_REWRITE_CONFIGURE_PRESET}" CACHE STRING
        "CMake build preset used inside the copied query-rewrite source tree")
set(AKKARADB_QUERY_REWRITE_WORK_DIR "${CMAKE_CURRENT_SOURCE_DIR}/builds/akkara-query-rewrite/${AKKARADB_QUERY_REWRITE_CONFIGURE_PRESET}" CACHE PATH
        "Working directory for copied source, rewrite outputs, and logs")
set(AKKARADB_QUERY_REWRITE_PLUGIN_PRESET "windows-msys2-ucrt64-release" CACHE STRING
        "CMake preset used to build the Akkara query Clang plugin")
set(AKKARADB_QUERY_REWRITE_BUILD_TARGET "" CACHE STRING
        "Optional target to build in the copied query-rewrite source tree")

if (WIN32)
    find_program(AKKARADB_POWERSHELL NAMES pwsh powershell)
    if (AKKARADB_POWERSHELL)
        set(AKKARADB_QUERY_REWRITE_SCRIPT "${CMAKE_CURRENT_SOURCE_DIR}/scripts/akkara-query-rewrite-build.ps1")
        set(AKKARADB_QUERY_REWRITE_COMMAND
                "${AKKARADB_POWERSHELL}"
                -NoProfile
                -ExecutionPolicy
                Bypass
                -File
                "${AKKARADB_QUERY_REWRITE_SCRIPT}"
                -SourceDir
                "${CMAKE_CURRENT_SOURCE_DIR}"
                -WorkDir
                "${AKKARADB_QUERY_REWRITE_WORK_DIR}"
                -ConfigurePreset
                "${AKKARADB_QUERY_REWRITE_CONFIGURE_PRESET}"
                -BuildPreset
                "${AKKARADB_QUERY_REWRITE_BUILD_PRESET}"
                -PluginPreset
                "${AKKARADB_QUERY_REWRITE_PLUGIN_PRESET}"
                -Clean
        )

        if (NOT "${AKKARADB_QUERY_REWRITE_BUILD_TARGET}" STREQUAL "")
            list(APPEND AKKARADB_QUERY_REWRITE_COMMAND -BuildTarget "${AKKARADB_QUERY_REWRITE_BUILD_TARGET}")
        endif ()

        add_custom_target(akkaradb_query_rewrite_build
                COMMAND ${AKKARADB_QUERY_REWRITE_COMMAND}
                WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
                COMMENT "Copying, rewriting, and building AkkaraDB sources with the query Clang plugin"
                VERBATIM
        )
    else ()
        message(STATUS "AkkaraDB query rewrite build target is unavailable: PowerShell was not found")
    endif ()
else ()
    message(STATUS "AkkaraDB query rewrite build target is currently Windows-only")
endif ()

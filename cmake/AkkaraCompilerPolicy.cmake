include_guard(GLOBAL)

set(AKKARADB_SUPPORTED_CXX_COMPILER "LLVM Clang" CACHE INTERNAL "AkkaraDB supported C++ compiler family")

if (NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    message(FATAL_ERROR
            "AkkaraDB Native requires LLVM Clang. "
            "Use clang-cl on Windows and clang++ on Linux/macOS. "
            "Current compiler id: ${CMAKE_CXX_COMPILER_ID}")
endif ()

set(AKKARADB_CXX_FRONTEND_IS_MSVC OFF)
if (CMAKE_CXX_COMPILER_FRONTEND_VARIANT STREQUAL "MSVC"
        OR CMAKE_CXX_SIMULATE_ID STREQUAL "MSVC")
    set(AKKARADB_CXX_FRONTEND_IS_MSVC ON)
endif ()

if (WIN32)
    if (NOT AKKARADB_CXX_FRONTEND_IS_MSVC)
        message(FATAL_ERROR
                "AkkaraDB Native on Windows requires clang-cl with the MSVC toolchain. "
                "MinGW/UCRT clang++ is not the supported Windows compiler path for the native library.")
    endif ()
else ()
    if (AKKARADB_CXX_FRONTEND_IS_MSVC)
        message(FATAL_ERROR
                "AkkaraDB Native outside Windows requires clang++ with the GNU-style Clang frontend.")
    endif ()
endif ()

if (WIN32)
    message(STATUS "AkkaraDB compiler policy: Windows clang-cl with MSVC toolchain")
else ()
    message(STATUS "AkkaraDB compiler policy: clang++")
endif ()

cmake_minimum_required(VERSION 3.24)

project(AkkaraQueryClangPlugin
    VERSION 0.1.0
    DESCRIPTION "Clang plugin skeleton for rewriting AkkaraDB Native C++ query lambdas"
    LANGUAGES C CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(LLVM REQUIRED CONFIG)
find_package(Clang REQUIRED CONFIG)

message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}: ${LLVM_DIR}")
message(STATUS "Found Clang: ${Clang_DIR}")

find_library(CLANG_CPP_LIBRARY
    NAMES clang-cpp
    HINTS ${LLVM_LIBRARY_DIRS}
    REQUIRED
)
find_library(LLVM_SHARED_LIBRARY
    NAMES LLVM-${LLVM_VERSION_MAJOR} LLVM
    HINTS ${LLVM_LIBRARY_DIRS}
    REQUIRED
)

list(APPEND CMAKE_MODULE_PATH "${LLVM_CMAKE_DIR}")
include(AddLLVM)

add_library(akkara_query_clang_plugin MODULE
    src/AkkaraQueryPlugin.cpp
)

target_compile_features(akkara_query_clang_plugin PRIVATE cxx_std_20)

target_include_directories(akkara_query_clang_plugin
    PRIVATE
        ${LLVM_INCLUDE_DIRS}
        ${CLANG_INCLUDE_DIRS}
)

target_compile_definitions(akkara_query_clang_plugin
    PRIVATE
        __STDC_CONSTANT_MACROS
        __STDC_FORMAT_MACROS
        __STDC_LIMIT_MACROS
)

if(MSVC)
    target_compile_options(akkara_query_clang_plugin PRIVATE /EHsc /Zc:__cplusplus)
else()
    target_compile_options(akkara_query_clang_plugin PRIVATE -fno-rtti)
endif()

target_link_libraries(akkara_query_clang_plugin
    PRIVATE
        ${CLANG_CPP_LIBRARY}
        ${LLVM_SHARED_LIBRARY}
)

set_target_properties(akkara_query_clang_plugin PROPERTIES
    OUTPUT_NAME "akkara-query"
)

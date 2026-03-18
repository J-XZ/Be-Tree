cmake_minimum_required(VERSION 3.10)
project(BeTree)

# 导出 compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 17)

# 使用ccache
find_program(CCACHE_PROGRAM ccache)

# use clang++, optimize for debugging
set(CMAKE_CXX_COMPILER "clang++-18")
set(CMAKE_C_COMPILER "clang-18")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

if(CCACHE_PROGRAM)
    message(STATUS "Using ccache: ${CCACHE_PROGRAM}")
    set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
endif()

add_executable(test test.cpp swap_space.cpp backing_store.cpp)

target_include_directories(test PRIVATE ${CMAKE_SOURCE_DIR})
target_include_directories(test PRIVATE ${CMAKE_SOURCE_DIR}/thirdparty_libs/thread_safe_print/include)
target_link_directories(test PRIVATE ${CMAKE_SOURCE_DIR}/thirdparty_libs/thread_safe_print/build)
target_link_libraries(test thread_safe_print)

# 清理目标
set_directory_properties(PROPERTIES ADDITIONAL_MAKE_CLEAN_FILES "${CMAKE_BINARY_DIR}/test;${CMAKE_BINARY_DIR}/*.o")

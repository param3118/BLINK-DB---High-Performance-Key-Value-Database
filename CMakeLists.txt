cmake_minimum_required(VERSION 3.20)

project(BlinkDB VERSION 0.3.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(SQLite3 3.24 REQUIRED)
find_package(zstd CONFIG REQUIRED)

add_library(blinkdb_storage part-a/src/blinkdb.cpp)
target_include_directories(blinkdb_storage PUBLIC part-a/src)
target_link_libraries(blinkdb_storage PUBLIC SQLite::SQLite3 zstd::libzstd)

if(MSVC)
    target_compile_options(blinkdb_storage PRIVATE /W4 /permissive-)
else()
    target_compile_options(blinkdb_storage PRIVATE -Wall -Wextra -Wpedantic)
endif()

option(BLINKDB_BUILD_DEMO "Build the standalone storage-engine demo" ON)
option(BLINKDB_BUILD_TESTS "Build storage-engine tests" ON)

if(BLINKDB_BUILD_DEMO)
    find_package(Threads REQUIRED)
    add_executable(blinkdb part-a/src/main.cpp)
    target_link_libraries(blinkdb PRIVATE blinkdb_storage Threads::Threads)
endif()

if(BLINKDB_BUILD_TESTS)
    include(CTest)
    find_package(Threads REQUIRED)
    add_executable(blinkdb_storage_tests part-a/tests/storage_engine_tests.cpp)
    target_link_libraries(blinkdb_storage_tests PRIVATE blinkdb_storage Threads::Threads)
    add_test(NAME blinkdb_storage_tests COMMAND blinkdb_storage_tests)
    set_tests_properties(blinkdb_storage_tests PROPERTIES TIMEOUT 30)
endif()

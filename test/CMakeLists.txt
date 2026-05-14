cmake_minimum_required(VERSION 3.14)

find_package(GTest REQUIRED)
include(GoogleTest)

add_executable(db_table_test db_table_test.cpp)
target_link_libraries(db_table_test PRIVATE
    incomplete-imdragonfly
    net
    third_party_deps
    GTest::gtest
    GTest::gtest_main
)
target_include_directories(db_table_test PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/net
)
target_compile_options(db_table_test PRIVATE ${CXX_WARNING_OPTIONS})
gtest_discover_tests(db_table_test)

add_library(redis_dict STATIC
    redis/alloc.cpp
    redis/dict.cpp
    redis/sds.cpp
)
target_include_directories(redis_dict PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/redis)
# 对于 redis_dict，我们使用宽松模式编译，因为它是从C代码移植过来的
target_compile_options(redis_dict PRIVATE
    -fpermissive
    -Wno-pedantic
    -Wno-shadow
    -Wno-error
)

add_executable(db_table_perf db_table_perf.cpp)
target_link_libraries(db_table_perf PRIVATE
    incomplete-imdragonfly
    net
    third_party_deps
    redis_dict
)
target_include_directories(db_table_perf PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/net
    ${CMAKE_CURRENT_SOURCE_DIR}/redis
)
target_compile_options(db_table_perf PRIVATE ${CXX_WARNING_OPTIONS})

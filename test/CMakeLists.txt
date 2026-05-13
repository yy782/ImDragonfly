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
gtest_discover_tests(db_table_test)

add_executable(db_table_perf db_table_perf.cpp)
target_link_libraries(db_table_perf PRIVATE
    incomplete-imdragonfly
    net
    third_party_deps
)
target_include_directories(db_table_perf PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/net
)

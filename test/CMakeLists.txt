cmake_minimum_required(VERSION 3.14)

# 单元测试可执行文件（统一入口，包含所有 *_test.cpp 和 *_perf.cpp）
add_executable(unit_tests
    db_table_test.cpp
    intent_lock_test.cpp
)
target_link_libraries(unit_tests PRIVATE
    incomplete-imdragonfly
    net
    third_party_deps
    GTest::gtest_main
    GTest::gtest
)
target_include_directories(unit_tests PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/net
)
target_compile_options(unit_tests PRIVATE ${CXX_WARNING_OPTIONS})
target_compile_definitions(unit_tests PRIVATE UNIT_TESTS)
gtest_discover_tests(unit_tests)
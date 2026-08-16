cmake_minimum_required(VERSION 3.14)

# 单元测试可执行文件（统一入口，包含所有 *_test.cpp 和 *_perf.cpp）
add_executable(unit_tests
    db_table_test.cpp
    tx_queue_test.cpp
    task_queue_test.cpp
    intent_lock_test.cpp
    synchronized_test.cpp
    cmd_arg_parser_test.cpp
    compact_obj_test.cpp
    op_status_test.cpp
    parse_resp_test.cpp
    rdb_serializer_test.cpp
    reply_builder_test.cpp
    skiplist_test.cpp
    string_or_view_test.cpp
    wait_queue_test.cpp
    transaction_test.cpp
    intrusive_ptr_test.cpp
)
target_link_libraries(unit_tests PRIVATE
    imdragonfly_lib_test
    net
    third_party_deps
    GTest::gtest_main
    GTest::gtest
)
target_include_directories(unit_tests PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/util
)
target_compile_options(unit_tests PRIVATE ${CXX_WARNING_OPTIONS})
target_compile_definitions(unit_tests PRIVATE UNIT_TESTS)
gtest_discover_tests(unit_tests)
# ImDragonfly/src/CMakeLists.txt

file(GLOB_RECURSE STORAGE_SOURCES 
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
)

# 主程序库: 不带 UNIT_TESTS，链接到 imdragonfly 可执行文件
add_library(imdragonfly_lib STATIC ${STORAGE_SOURCES})

target_include_directories(imdragonfly_lib PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/util
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_options(imdragonfly_lib PRIVATE ${CXX_WARNING_OPTIONS})

target_link_libraries(imdragonfly_lib PUBLIC
    ${CMAKE_THREAD_LIBS_INIT}
    net
    third_party_deps
)

# 测试库: 带 UNIT_TESTS，仅链接到 unit_tests 可执行文件
if(BUILD_TESTS)
    add_library(imdragonfly_lib_test STATIC ${STORAGE_SOURCES})
    target_compile_definitions(imdragonfly_lib_test PRIVATE UNIT_TESTS)
    target_include_directories(imdragonfly_lib_test PUBLIC
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/src/util
        ${CMAKE_CURRENT_SOURCE_DIR}
    )
    target_compile_options(imdragonfly_lib_test PRIVATE ${CXX_WARNING_OPTIONS})
    target_link_libraries(imdragonfly_lib_test PUBLIC
        ${CMAKE_THREAD_LIBS_INIT}
        net
        third_party_deps
    )
endif()
# ImDragonfly/src/CMakeLists.txt

file(GLOB_RECURSE STORAGE_SOURCES 
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
)

add_library(imdragonfly_lib STATIC ${STORAGE_SOURCES} )

if(BUILD_TESTS)
    target_compile_definitions(imdragonfly_lib PUBLIC UNIT_TESTS)
endif()

target_include_directories(imdragonfly_lib PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/YY
    ${CMAKE_SOURCE_DIR}/YY/base
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_options(imdragonfly_lib PRIVATE ${CXX_WARNING_OPTIONS})

target_link_libraries(imdragonfly_lib PUBLIC
    ${CMAKE_THREAD_LIBS_INIT}
    net
    base
    thread
    third_party_deps 
)

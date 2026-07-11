# StorageEngine/src/CMakeLists.txt

file(GLOB_RECURSE STORAGE_SOURCES 
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
)

add_library(incomplete-imdragonfly STATIC ${STORAGE_SOURCES} )

target_include_directories(incomplete-imdragonfly PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/YY
    ${CMAKE_SOURCE_DIR}/YY/base
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_options(incomplete-imdragonfly PRIVATE ${CXX_WARNING_OPTIONS})

target_link_libraries(incomplete-imdragonfly PUBLIC
    ${CMAKE_THREAD_LIBS_INIT}
    net
    base
    thread
    third_party_deps 
)



file(GLOB_RECURSE STORAGE_SOURCES 
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"

)

add_library(net STATIC ${STORAGE_SOURCES} )

target_include_directories(net PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# 链接库
target_link_libraries(net PUBLIC
    ${CMAKE_THREAD_LIBS_INIT}
    third_party_deps 
    uring
)
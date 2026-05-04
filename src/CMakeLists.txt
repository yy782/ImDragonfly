# StorageEngine/src/CMakeLists.txt

# 收集源文件
file(GLOB_RECURSE STORAGE_SOURCES 
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"

)

# file(GLOB_RECURSE STORAGE_HEADERS 
#     "${CMAKE_CURRENT_SOURCE_DIR}/*.hpp"
# )${STORAGE_HEADERS}

# 创建核心库
add_library(incomplete-imdragonfly STATIC ${STORAGE_SOURCES} )

# 包含目录
target_include_directories(incomplete-imdragonfly PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/net
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# 链接库
target_link_libraries(incomplete-imdragonfly PUBLIC
    ${CMAKE_THREAD_LIBS_INIT}
    net
    third_party_deps 
)


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
    ${CMAKE_SOURCE_DIR}/helio

    ${CMAKE_CURRENT_SOURCE_DIR}/command_layer
    ${CMAKE_CURRENT_SOURCE_DIR}/detail
    ${CMAKE_CURRENT_SOURCE_DIR}/redis
    ${CMAKE_CURRENT_SOURCE_DIR}/sharding
)

# 链接库
target_link_libraries(incomplete-imdragonfly PUBLIC
    ${CMAKE_THREAD_LIBS_INIT}
    absl::flags
    absl::strings
    absl::hash
    absl::base
    base
    fibers2
    io
)


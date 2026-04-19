# StorageEngine/src/CMakeLists.txt

# 收集源文件
file(GLOB_RECURSE STORAGE_SOURCES 
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"

)

file(GLOB_RECURSE STORAGE_HEADERS 
    "${CMAKE_CURRENT_SOURCE_DIR}/include/*.hpp"
)

# 创建核心库
add_library(storage_core STATIC ${STORAGE_SOURCES} ${STORAGE_HEADERS})

# 包含目录
target_include_directories(storage_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# 链接库
target_link_libraries(storage_core PUBLIC
    ${CMAKE_THREAD_LIBS_INIT}
)


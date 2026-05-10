# StorageEngine/CMakeLists.txt
cmake_minimum_required(VERSION 3.14)
project(StorageEngine VERSION 1.0.0)

# C++ 标准
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

option(BUILD_TESTS "Build tests" OFF)

# cmake -DBUILD_TESTING=OFF -DHELIO_BUILD_TESTS=OFF ..
# 开启 Debug/Release 模式
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

include(cmake/third_party.cmake)


if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(WARNING_OPTIONS
        -g -O0
        -std=c++20
        -rdynamic
        -march=native
        -Wall
        -Wextra                 #额外警告
        -Wpedantic              #严格遵守C++标准
        -Werror                 #将警告视为错误
        -Wno-unknown-pragmas    #需要关闭的警告
        -Wshadow                #变量遮掩警告
        -Wunused-parameter     #未使用参数警告
        -Woverloaded-virtual
        -Wpointer-arith
        -Wwrite-strings
    )
else()
    set(WARNING_OPTIONS
        -O3                     # 生产优化
        -std=c++20
        -march=native
        -DNDEBUG
        -Wall
        -Wextra
        -Wno-error              # 警告不升级为错误
        -Wno-shadow
        -Wno-conversion   
    )
endif()    

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    add_compile_options(${WARNING_OPTIONS})
endif()

# 主代码
add_subdirectory(net)
add_subdirectory(src)
# 测试
if(BUILD_TESTS)
    add_subdirectory(test)
endif()

# 主程序
add_executable(imdragonfly main.cpp)
target_include_directories(imdragonfly PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/net
)


target_link_libraries(imdragonfly PRIVATE
    incomplete-imdragonfly
    net
    third_party_deps 
)

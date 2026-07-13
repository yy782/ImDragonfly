# StorageEngine/CMakeLists.txt
cmake_minimum_required(VERSION 3.14)

find_program(CLANG_EXECUTABLE clang)
find_program(CLANGXX_EXECUTABLE clang++)

if(CLANG_EXECUTABLE AND CLANGXX_EXECUTABLE)
    set(CMAKE_C_COMPILER "${CLANG_EXECUTABLE}" CACHE PATH "C compiler" FORCE)
    set(CMAKE_CXX_COMPILER "${CLANGXX_EXECUTABLE}" CACHE PATH "C++ compiler" FORCE)
else()
    message(FATAL_ERROR "clang/clang++ not found! Please install Clang 17+")
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

project(StorageEngine VERSION 1.0.0)


list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

option(BUILD_TESTS "Build tests" OFF)
# cmake CMAKE_BUTLD_TYPE = Release ..
# cmake -DBUILD_TESTING=OFF -DHELIO_BUILD_TESTS=OFF ..
# 开启 Debug/Release 模式
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

include(cmake/third_party.cmake)
option(USE_ASAN "Enable AddressSanitizer" OFF)
if(USE_ASAN)
    set(ASAN_FLAGS 
        -fsanitize=address 
        -fno-omit-frame-pointer
    )
    set(ASAN_LDFLAGS -fsanitize=address)
    add_compile_options(${ASAN_FLAGS})
    add_link_options(${ASAN_LDFLAGS})
    message(STATUS "AddressSanitizer is ENABLED")
else()
    set(ASAN_FLAGS "")
    set(ASAN_LDFLAGS "")
    message(STATUS "AddressSanitizer is DISABLED")
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CXX_WARNING_OPTIONS
        -g -O0
        -rdynamic
        -march=native
        -Wall
        -Wextra                 #额外警告
        -Wpedantic              #严格遵守C++标准
        -Wno-unknown-pragmas    #需要关闭的警告
        -Wshadow                #变量遮掩警告
        -Wunused-parameter     #未使用参数警告
        -Woverloaded-virtual
        -Wpointer-arith
        -Wwrite-strings
        ${ASAN_FLAGS}
    )
else()
    set(CXX_WARNING_OPTIONS
        -O3                     # 生产优化
        -march=native
        -DNDEBUG
        -Wall
        -Wextra
        -Wno-error              # 警告不升级为错误
        -Wno-shadow
        -Wno-conversion   
    )
endif()    

# 不编译 YY — 项目 util/ 已提供全部工具类（Thread, TaskQueue 等）
add_subdirectory(net)

add_subdirectory(src)

if(BUILD_TESTS)
    add_subdirectory(test/redis)
    add_subdirectory(test)   
endif()

add_executable(imdragonfly main.cpp)
target_include_directories(imdragonfly PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/util
)
target_compile_options(imdragonfly PRIVATE ${CXX_WARNING_OPTIONS})

target_link_libraries(imdragonfly PRIVATE
    incomplete-imdragonfly
    net
    third_party_deps 
)

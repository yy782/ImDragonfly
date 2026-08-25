cmake_minimum_required(VERSION 3.10)

file(GLOB IO_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
)

add_library(io ${IO_SOURCES})

target_include_directories(io PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/util
)

target_link_libraries(io PUBLIC
    third_party_deps
    uring
)

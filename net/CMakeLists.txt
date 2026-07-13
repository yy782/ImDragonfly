cmake_minimum_required(VERSION 3.10)

file(GLOB NET_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
)

add_library(net ${NET_SOURCES})

target_include_directories(net PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/util
)

target_link_libraries(net PUBLIC
    third_party_deps
)

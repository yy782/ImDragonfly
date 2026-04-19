file(GLOB_RECURSE NETWORK_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
)

file(GLOB_RECURSE NETWORK_HEADERS
    "${CMAKE_CURRENT_SOURCE_DIR}/*.hpp"
)

add_library(network STATIC ${NETWORK_SOURCES} ${NETWORK_HEADERS})

target_include_directories(network PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(network PUBLIC
    storage_core
    helio
)
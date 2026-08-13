
include(FetchContent)
include(FindPkgConfig)
set(FETCHCONTENT_BASE_DIR "${CMAKE_BINARY_DIR}/_deps")

add_library(third_party_deps INTERFACE)

find_library(URING_LIBRARY NAMES uring liburing)
find_path(URING_INCLUDE_DIR NAMES liburing.h)

if(URING_LIBRARY AND URING_INCLUDE_DIR)
    message(STATUS "[third_party] 使用系统 liburing: ${URING_LIBRARY}")
    add_library(uring SHARED IMPORTED)
    set_target_properties(uring PROPERTIES
        IMPORTED_LOCATION "${URING_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${URING_INCLUDE_DIR}"
    )
else()
    message(STATUS "[third_party] 系统未找到 liburing，下载到 build/_deps ...")
    FetchContent_Declare(
        liburing
        GIT_REPOSITORY  https://github.com/axboe/liburing.git
        GIT_TAG         master
        GIT_SHALLOW     TRUE
    )
    FetchContent_MakeAvailable(liburing)
    # liburing 的 CMake 项目会生成 target: uring
endif()

find_library(MIMALLOC_LIBRARY NAMES mimalloc libmimalloc)
find_path(MIMALLOC_INCLUDE_DIR NAMES mimalloc.h)

if(MIMALLOC_LIBRARY AND MIMALLOC_INCLUDE_DIR)
    message(STATUS "[third_party] 使用系统 mimalloc: ${MIMALLOC_LIBRARY}")
    add_library(mimalloc STATIC IMPORTED)
    set_target_properties(mimalloc PROPERTIES
        IMPORTED_LOCATION "${MIMALLOC_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${MIMALLOC_INCLUDE_DIR}"
    )
    target_link_libraries(third_party_deps INTERFACE mimalloc)
else()
    message(STATUS "[third_party] 系统未找到 mimalloc，下载到 build/_deps ...")
    FetchContent_Declare(
        mimalloc
        GIT_REPOSITORY  https://github.com/microsoft/mimalloc.git
        GIT_TAG         v2.1.7
        GIT_SHALLOW     TRUE
    )
    # mimalloc 默认编译为静态库
    set(MI_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(MI_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(MI_BUILD_OBJECT  OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(mimalloc)
    target_link_libraries(third_party_deps INTERFACE mimalloc-static)
endif()

find_package(Boost QUIET)
if(Boost_FOUND)
    message(STATUS "[third_party] 使用系统 Boost: ${Boost_INCLUDE_DIRS}")
    target_include_directories(third_party_deps INTERFACE ${Boost_INCLUDE_DIRS})
else()
    message(STATUS "[third_party] 系统未找到 Boost，下载 header-only 到 build/_deps ...")
    # Boost 太大，只下载需要的 header 文件
    FetchContent_Declare(
        boost_headers
        URL             https://github.com/boostorg/boost/releases/download/boost-1.85.0/boost-1.85.0-b2-nodocs.tar.xz
        URL_HASH        SHA256=4b29c48eaba6846a0c5bd6894bc3c4ffc881d681d641136e0c5c98cddb2d0f1b
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    FetchContent_Populate(boost_headers)
    target_include_directories(third_party_deps INTERFACE ${boost_headers_SOURCE_DIR})
endif()

find_package(glog QUIET)
if(glog_FOUND)
    message(STATUS "[third_party] 使用系统 glog")
    target_link_libraries(third_party_deps INTERFACE glog::glog)
else()
    find_library(GLOG_LIBRARY NAMES glog libglog)
    find_path(GLOG_INCLUDE_DIR NAMES glog/logging.h)
    if(GLOG_LIBRARY AND GLOG_INCLUDE_DIR)
        message(STATUS "[third_party] 使用系统 glog (manual): ${GLOG_LIBRARY}")
        add_library(glog_imported UNKNOWN IMPORTED)
        set_target_properties(glog_imported PROPERTIES
            IMPORTED_LOCATION "${GLOG_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${GLOG_INCLUDE_DIR}"
        )
        target_link_libraries(third_party_deps INTERFACE glog_imported)
    else()
        message(STATUS "[third_party] 系统未找到 glog，下载到 build/_deps ...")
        FetchContent_Declare(
            glog
            GIT_REPOSITORY  https://github.com/google/glog.git
            GIT_TAG         v0.7.1
            GIT_SHALLOW     TRUE
        )
        set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
        set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)
        set(WITH_GTEST OFF CACHE BOOL "" FORCE)
        FetchContent_MakeAvailable(glog)
        target_link_libraries(third_party_deps INTERFACE glog::glog)
    endif()
endif()

if(BUILD_TESTS)
    # 先尝试 find_package（某些发行版预编译了 GTest）
    find_package(GTest QUIET)
    if(NOT GTest_FOUND)
        # 尝试手动查找系统安装的头文件 + 源码（Ubuntu 模式）
        find_path(GTEST_INCLUDE_DIR gtest/gtest.h
            PATHS /usr/include /usr/local/include
        )
        find_path(GTEST_SRC_DIR src/gtest-all.cc
            PATHS /usr/src/gtest /usr/src/googletest/googletest
        )
        if(GTEST_INCLUDE_DIR AND GTEST_SRC_DIR)
            message(STATUS "[third_party] 使用系统 GTest 源码 (Ubuntu): 自动编译")
            add_library(gtest STATIC ${GTEST_SRC_DIR}/src/gtest-all.cc)
            target_include_directories(gtest PRIVATE ${GTEST_INCLUDE_DIR} ${GTEST_SRC_DIR})
            target_include_directories(gtest INTERFACE ${GTEST_INCLUDE_DIR})
            find_package(Threads REQUIRED)
            target_link_libraries(gtest PUBLIC Threads::Threads)
            add_library(GTest::gtest ALIAS gtest)
            add_library(GTest::gtest_main ALIAS gtest)
            set(GTest_FOUND TRUE)
        endif()
    endif()

    if(GTest_FOUND)
        message(STATUS "[third_party] 使用系统 GTest")
    else()
        message(STATUS "[third_party] 系统未找到 GTest，下载到 build/_deps ...")
        FetchContent_Declare(
            googletest
            GIT_REPOSITORY  https://github.com/google/googletest.git
            GIT_TAG         v1.15.2
            GIT_SHALLOW     TRUE
        )
        set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
        FetchContent_MakeAvailable(googletest)
    endif()
    enable_testing()
endif()

function(add_cxx_test NAME)
    cmake_parse_arguments(ARG "" "" "LABELS" ${ARGN})

    set(TEST_TARGET "${NAME}")
    add_executable(${TEST_TARGET} "${NAME}.cc")
    target_link_libraries(${TEST_TARGET} PRIVATE 
        pthread
        ${ARG_UNPARSED_ARGUMENTS}
        GTest::gtest
        GTest::gtest_main
    )
    if(ARG_LABELS)
        gtest_discover_tests(${TEST_TARGET} LABELS ${ARG_LABELS})
    else()
        gtest_discover_tests(${TEST_TARGET})
    endif()
endfunction()

function(target_link_cxx TARGET)
    target_link_libraries(${TARGET} ${ARGN})
endfunction()


message(STATUS "[third_party] 依赖配置完成")
message(STATUS "[third_party] FETCHCONTENT_BASE_DIR = ${FETCHCONTENT_BASE_DIR}")
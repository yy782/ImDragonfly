// main.cpp
// ./imdragonfly
// valgrind ./imdragonfly , 与mimalloc, glog冲突
#include <glog/logging.h>
// cd programs/ImDragonfly
#include "src/network/redis_server.hpp"
#include <memory>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <cstdio>

using namespace dfly;

/*
mimalloc: warning: mi_usable_size: pointer might not point to a valid heap region: 0x7aacbc020000
(this may still be a valid very large allocation (over 64MiB))
mimalloc: warning: (yes, the previous pointer 0x7aacbc020000 was valid after all)
mimalloc: warning: mi_usable_size: pointer might not point to a valid heap region: 0x7aacbc020000
(this may still be a valid very large allocation (over 64MiB))
mimalloc: warning: mimalloc: warning: mi_usable_size: pointer might not point to a valid heap region: 0x7aacb8020000
(this may still be a valid very large allocation (over 64MiB))
mimalloc: warning: (yes, the previous pointer 0x7aacb8020000 was valid after all)
mimalloc: warning: mi_usable_size: pointer might not point to a valid heap region: 0x7aacb8020000
(this may still be a valid very large allocation (over 64MiB))
mimalloc: warning: (yes, the previous pointer 0x7aacb8020000 was valid after all)
mimalloc: warning: mi_usable_size: pointer might not point to a valid heap region: 0x7aacb8030080
(this may still be a valid very large allocation (over 64MiB))
mimalloc: warning: (yes, the previous pointer 0x7aacb8030080 was valid after all)
mimalloc: warning: mi_usable_size: pointer might not point to a valid heap region: 0x7aacb8030080
(this may still be a valid very large allocation (over 64MiB))
mimalloc: warning: (yes, the previous pointer 0x7aacb8030080 was valid after all)
(yes, the previous pointer 0x7aacbc020000 was valid after all)
mimalloc: warning: mi_usable_size: pointer might not point to a valid heap region: 0x7aacbc030080
(this may still be a valid very large allocation (over 64MiB))
mimalloc: warning: (yes, the previous pointer 0x7aacbc030080 was valid after all)
mimalloc: warning: mi_usable_size: pointer might not point to a valid heap region: 0x7aacbc030080
(this may still be a valid very large allocation (over 64MiB))
mimalloc: warning: (yes, the previous pointer 0x7aacbc030080 was valid after all)
mimalloc: warning: mi_usable_size: pointer might not point to a valid heap region: 0x7aacb4020000
(this may still be a valid very large allocation (over 64MiB)) mimalloc和ASAN冲突
*/


/*
程序结束时ASAN会报内存泄漏的错误，但是这实际上不是错误，是堆内存操作没有执行完时，程序被终止时的正常情况，ASAN无法区分这种情况和真正的内存泄漏，所以会误报内存泄漏错误。
*/

// export ASAN_OPTIONS="detect_leaks=1:leak_check_at_exit=0:leak_check_interval=1024:halt_on_error=0:log_path=/home/yy/programs/ImDragonfly/build/asan.log"
// ./imdragonfly

int main(int argc, char *argv[]) {
    (void)argc;

    int ret = mkdir("./logs", 0755);
    if (ret != 0 && errno != EEXIST) {
        fprintf(stderr, "Failed to create logs directory: %s\n", strerror(errno));
        return 1;
    }
    
    FLAGS_log_dir = "./logs";
    FLAGS_logtostderr = false;
    FLAGS_alsologtostderr = false;
    FLAGS_minloglevel = 0;
    FLAGS_v = 2;  
#ifndef NDEBUG
    FLAGS_logbufsecs = 0;
#endif

    google::InitGoogleLogging(argv[0]);
    
    LOG(INFO) << "ImDragonfly server starting...";

    int listen_fd = base::ListenFd();
    LOG(INFO) << "Listening on fd: " << listen_fd;

    RedisServer server(listen_fd, 4);
    LOG(INFO) << "RedisServer initialized with 4 shards";

    server.Start();
    
    LOG(INFO) << "ImDragonfly server shutting down...";
    google::ShutdownGoogleLogging();
    return 0;
}
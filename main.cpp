// main.cpp
// ./imdragonfly
#include <glog/logging.h>

#include "src/network/redis_server.hpp"
#include <memory>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <cstdio>

using namespace dfly;


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
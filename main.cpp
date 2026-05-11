// main.cpp
// ./imdragonfly
#include <glog/logging.h>

#include "src/network/redis_server.hpp"
#include <memory>

using namespace dfly;


int main(int argc, char *argv[]) {

    (void)argc;
    FLAGS_log_dir = "./logs";
    google::InitGoogleLogging(argv[0]);
    FLAGS_alsologtostderr = false;
    


    int listen_fd = base::ListenFd();

    RedisServer server(listen_fd, 4); // 监听6379端口，使用4个分片
    server.Start();
    google::ShutdownGoogleLogging();
    return 0;
}
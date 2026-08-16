// main.cpp
// ./imdragonfly
// valgrind ./imdragonfly , 与mimalloc, glog冲突
#include <glog/logging.h>
// cd programs/ImDragonfly
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>

#include "net/fd_wrapper.hpp"
#include "src/network/redis_server.hpp"

using namespace dfly;

// ASAN对协程有误报，注意一下

int main(int argc, char *argv[]) {
  // google::ParseCommandLineFlags(&argc, &argv, true); 没有引入#include
  // <gflags/gflags.h>，所以不可用

  FLAGS_logtostderr = true;
  FLAGS_alsologtostderr = false;
  FLAGS_minloglevel = 0;
#ifndef NDEBUG
  FLAGS_logbufsecs = 0;
#endif
  google::InitGoogleLogging(argv[0]);

  int ret = mkdir("./logs", 0755);
  if (ret != 0 && errno != EEXIST) {
    LOG(ERROR) << "Failed to create logs directory: " << strerror(errno);
    google::ShutdownGoogleLogging();
    return 1;
  }

  FLAGS_log_dir = "./logs";
  FLAGS_logtostderr = false;
  LOG(INFO) << "ImDragonfly server starting...";
  int num = 4;
  if (argc > 1) {
    num = std::atoi(argv[1]);
  }

  // 端口由命令行指定：./imdragonfly [shards] [port]，默认 6379。
  uint16_t port = 6379;
  if (argc > 2) {
    port = static_cast<uint16_t>(std::atoi(argv[2]));
  }

  bool enable_rdb = true;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--no-rdb" || arg == "--no-snapshot") {
      enable_rdb = false;
    }
  }

  int listenFd = base::ListenFd(port);
  if (listenFd < 0) {
    LOG(ERROR) << "Failed to create listen socket";
    google::ShutdownGoogleLogging();
    return 1;
  }

  RedisServer server(listenFd, num, enable_rdb);
  LOG(INFO) << "RedisServer initialized with " << num << " shards"
            << ", rdb=" << (enable_rdb ? "on" : "off");
  server.Start();

  LOG(INFO) << "ImDragonfly server shutting down...";
  google::ShutdownGoogleLogging();
  return 0;
}
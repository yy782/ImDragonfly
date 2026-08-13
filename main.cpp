// main.cpp
// ./imdragonfly
// valgrind ./imdragonfly , 与mimalloc, glog冲突
#include <glog/logging.h>
// cd programs/ImDragonfly
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cstdio>
#include <memory>

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

  int listenFd = base::ListenFd();
  if (listenFd < 0) {
    LOG(ERROR) << "Failed to create listen socket";
    google::ShutdownGoogleLogging();
    return 1;
  }

  RedisServer server(listenFd, num);
  LOG(INFO) << "RedisServer initialized with " << num << " shards";
  server.Start();

  LOG(INFO) << "ImDragonfly server shutting down...";
  google::ShutdownGoogleLogging();
  return 0;
}
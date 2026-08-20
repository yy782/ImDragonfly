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
#include <new>
#include <string>
#include <thread>

#include "io/fd_wrapper.hpp"
#include "src/server/redis_server.hpp"
#include "src/util/json_config.hpp"

using namespace dfly;

// ASAN对协程有误报，注意一下

int main(int argc, char* argv[]) {
  std::set_new_handler([]() noexcept {
    std::fputs("out of memory: operator new failed\n", stderr);
    std::fflush(stderr);
    std::abort();
  });

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
  uint16_t port = 6379;
  bool enable_rdb = true;
  std::string config_path;
  util::JsonConfig config;

  // 命令行参数：./imdragonfly [shards] [port] [--no-rdb] [--config <path>]
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--no-rdb" || arg == "--no-snapshot") {
      enable_rdb = false;
    } else if (arg == "--config") {
      if (i + 1 < argc) {
        config_path = argv[++i];
      } else {
        LOG(ERROR) << "--config 需要一个文件路径参数";
        google::ShutdownGoogleLogging();
        return 1;
      }
    } else if (i == 1) {
      num = std::atoi(argv[1]);
    } else if (i == 2) {
      port = static_cast<uint16_t>(std::atoi(argv[2]));
    }
  }

  // 指定了配置文件则加载，并让配置覆盖命令行参数
  const util::JsonConfig* cfg = nullptr;
  if (!config_path.empty()) {
    std::string err;
    if (!config.LoadFromFile(config_path, &err)) {
      LOG(ERROR) << "加载配置文件失败: " << err;
      google::ShutdownGoogleLogging();
      return 1;
    }
    num = static_cast<int>(config.GetInt("shards", num));
    port = static_cast<uint16_t>(config.GetInt("port", port));
    enable_rdb = config.GetBool("enable_rdb", enable_rdb);
    cfg = &config;
    LOG(INFO) << "已加载配置文件: " << config_path;
  }

  int listenFd = base::ListenFd(port);
  if (listenFd < 0) {
    LOG(ERROR) << "Failed to create listen socket";
    google::ShutdownGoogleLogging();
    return 1;
  }

  RedisServer::Init(listenFd, num, enable_rdb, cfg);
  LOG(INFO) << "RedisServer initialized with " << num << " shards"
            << ", rdb=" << (enable_rdb ? "on" : "off");
  RedisServer::Instance().Start();
  RedisServer::Destroy();

  LOG(INFO) << "ImDragonfly server shutting down...";
  google::ShutdownGoogleLogging();
  return 0;
}
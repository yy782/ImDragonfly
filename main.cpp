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
#include "src/server/command_line.hpp"
#include "src/server/redis_server.hpp"
#include "src/util/json_config.hpp"
#include "src/util/startup_log.hpp"

using namespace dfly;

namespace {

// 递归创建目录（等价于 mkdir -p），支持 ./logs/imdragonfly2 这类多级路径。
bool MakeDirs(const std::string& path) {
  if (path.empty()) {
    return true;
  }
  for (size_t i = 0; i < path.size(); ++i) {
    if (path[i] != '/') {
      continue;
    }
    if (i == 0) {
      continue;  // 绝对路径根目录 "/" 已存在
    }
    std::string prefix = path.substr(0, i);
    if (prefix.empty() || prefix == ".") {
      continue;
    }
    if (mkdir(prefix.c_str(), 0755) != 0 && errno != EEXIST) {
      return false;
    }
  }
  if (mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
    return false;
  }
  return true;
}

}  // namespace

// ASAN对协程有误报，注意一下


// TODO 实现Multi-Raft，让无共享框架的每个线程跑一个raft实例， 约束:禁止多分片命令，比如 MSET, MGET , 


int main(int argc, char* argv[]) {
  // 忽略 SIGPIPE：客户端在响应发出前断连时，往对端已关闭的 socket 写会
  // 触发 SIGPIPE，默认动作是**终止整个进程** —— 一个断连的客户端就能把
  // 服务打挂。写操作会改为返回 EPIPE，由 socket.cc 记日志后走正常断连。
  signal(SIGPIPE, SIG_IGN);

  std::set_new_handler([]() noexcept {
    std::fputs("out of memory: operator new failed\n", stderr);
    std::fflush(stderr);
    std::abort();
  });

  // google::ParseCommandLineFlags(&argc, &argv, true); 没有引入#include
  // <gflags/gflags.h>，所以不可用

  FLAGS_logtostderr = false;  // 常规日志只写日志文件
  FLAGS_alsologtostderr = false;  // 终端仅打印启动信息（见 util::StartupLog）
  FLAGS_minloglevel = 0;
#ifndef NDEBUG
  FLAGS_logbufsecs = 0;
#endif
  google::InitGoogleLogging(argv[0]);

  // 先解析命令行参数，以便用 log_dir 决定谷歌日志输出目录
  std::string err;
  if (!ParseCommandLine(argc, argv, &err)) {
    LOG(ERROR) << err;
    google::ShutdownGoogleLogging();
    return 1;
  }

  int num = shards;
  uint16_t listen_port = redis_port;

  // 指定了配置文件则加载，并让配置覆盖命令行参数（含 log_dir）
  util::JsonConfig config;
  const util::JsonConfig* cfg = nullptr;
  if (!config_path.empty()) {
    if (!config.LoadFromFile(config_path, &err)) {
      LOG(ERROR) << "加载配置文件失败: " << err;
      google::ShutdownGoogleLogging();
      return 1;
    }
    log_dir = config.GetString("log_dir", log_dir);
    num = static_cast<int>(config.GetInt("shards", num));
    listen_port = static_cast<uint16_t>(config.GetInt("port", listen_port));
    cfg = &config;
  }

  // 创建日志目录（递归创建，支持 ./logs/imdragonfly2 这类多级路径）
  if (!MakeDirs(log_dir)) {
    LOG(ERROR) << "Failed to create logs directory: " << log_dir << ": "
               << strerror(errno);
    google::ShutdownGoogleLogging();
    return 1;
  }

  FLAGS_log_dir = log_dir;
  FLAGS_logtostderr = false;
  if (cfg) {
    LOG(INFO) << "已加载配置文件: " << config_path;
  }
  LOG(INFO) << "ImDragonfly server starting...";

  int listenFd = base::ListenFd(listen_port);
  if (listenFd < 0) {
    LOG(ERROR) << "Failed to create listen socket";
    google::ShutdownGoogleLogging();
    return 1;
  }

  RedisServer::Init(listenFd, num, cfg);
  util::StartupLog("RedisServer initialized with " + std::to_string(num) +
                   " shards");
  RedisServer::Instance().Start();
  RedisServer::Destroy();

  LOG(INFO) << "ImDragonfly server shutting down...";
  google::ShutdownGoogleLogging();
  return 0;
}
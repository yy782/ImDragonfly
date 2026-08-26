#pragma once
#include <glog/logging.h>

#include <cstdio>
#include <string>

namespace util {

// 同时写入日志文件（glog）并打印到终端（stdout）。
inline void StartupLog(const std::string& msg) {
  std::printf("%s\n", msg.c_str());
  std::fflush(stdout);
  LOG(INFO) << msg;
}

}  // namespace util

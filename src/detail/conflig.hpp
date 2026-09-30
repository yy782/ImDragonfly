#pragma once

#include <cstdint>
#include <string>

namespace dfly {

inline int shards = 4;
inline uint16_t redis_port = 6379;  // redis 监听端口（区别于 raft 节点间的端口）
inline std::string config_path;
inline bool use_raft = false;
inline std::string log_dir = "./logs";  // 谷歌日志(glog)输出目录

}  // namespace dfly

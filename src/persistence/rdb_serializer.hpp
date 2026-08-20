
#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "detail/common_types.hpp"

namespace dfly {

// 每个 shard 一个文件 dump-<sid>.rdb，覆盖所有 namespace 下该 shard 的
// 全部 db。保存时先写 .tmp 再原子 rename，避免半写文件被当成有效快照加载。
class RdbSerializer {
 public:
  // 在 shard 线程内调用：遍历所有 namespace，写入该 shard 的全部 db 到
  // dump-<sid>.rdb。返回是否成功。
  static bool SaveShard(ShardId sid, std::string_view dir);

  // 在 shard 线程内调用：加载 dump-<sid>.rdb 到该 shard 的 DbSlice。
  // 文件不存在视为空 shard（成功）；文件损坏返回 false（调用方拒绝加载）。
  static bool LoadShard(ShardId sid, std::string_view dir);

  static std::string FileName(ShardId sid) {
    return "dump-" + std::to_string(sid) + ".rdb";
  }
};

}  // namespace dfly

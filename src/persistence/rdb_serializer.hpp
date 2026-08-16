
#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "detail/common_types.hpp"

namespace dfly {

// RDB 快照格式（自定义二进制，Rust 风格显式长度前缀，小端）：
//
//   magic: "IMDRDB02" (8 字节)
//   每条记录：
//     u8   0x01 (record marker)
//     u32  ns_len | ns bytes         <- 所属命名空间（默认命名空间为空串）
//     u32  dbid
//     u32  key_len | key bytes
//     u64  expire_at_ms (0 = 无 TTL，绝对毫秒时间戳，原样保存)
//     u8   value_kind
//         0 = int     : i64
//         1 = string  : u32 len + bytes
//         2 = list    : u32 count + (u32 len + bytes)*
//         3 = hash    : u32 count + (u32 klen + kbytes + u32 vlen + vbytes)*
//         4 = set     : u32 count + (u32 len + bytes)*
//         5 = zset    : u32 count + (u32 len + member + u64 score_bits)*
//   EOF: u8 0xFF
//
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

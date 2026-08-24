
#pragma once

#include <cstdint>
#include <span>
#include <string_view>
#include <utility>
namespace dfly {


using Arg = std::string_view;  // 单个命令参数

using ArgSlice = std::span<const Arg>;  // 这两个是等价的

using CmdArgList = std::span<const Arg>;


using DbIndex = uint16_t;
using ShardId = uint16_t;
using LockFp = uint64_t;
using SlotId = std::uint16_t;

using IndexSlice = std::pair<unsigned, unsigned>;

// 内存资源类型别名：统一引用入口，替换实现（如换掉 mimalloc）时只改这一处。
class MiMemoryResource;  // 定义于 util/mi_memory_resource.hpp
using MemResource = MiMemoryResource;

constexpr DbIndex kInvalidDbId = DbIndex(-1);
constexpr ShardId kInvalidSid = ShardId(-1);

class EngineShard;
class Transaction;
class DbSlice;
class ConnectionContext;
class CommandContext;
class CommandRegistry;
class Interpreter;

namespace cmd {
struct CoroTask;
}


class RedisSession;
using RedisSessionPtr = std::shared_ptr<RedisSession>;
using RedisSessionWeakPtr = std::weak_ptr<RedisSession>;
inline ShardId ShardIndex(std::string_view key, ssize_t shard_set_size) {
  const char* data = key.data();
  size_t len = key.size();
  size_t hash = 0x9e3779b97f4a7c15ULL;

  // 每次处理 32 字节
  size_t i = 0;
  if (len >= 32) {
    __m256i vec = _mm256_setzero_si256();
    for (; i + 32 <= len; i += 32) {
      __m256i chunk =
          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
      vec = _mm256_xor_si256(vec, chunk);
    }
    // 混合结果
    alignas(32) uint64_t buffer[4];
    _mm256_store_si256(reinterpret_cast<__m256i*>(buffer), vec);
    hash ^= buffer[0] ^ buffer[1] ^ buffer[2] ^ buffer[3];
  }

  // 处理剩余字节
  for (; i < len; ++i) {
    hash ^= data[i] + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
  }

  if ((shard_set_size & (shard_set_size - 1)) == 0)
    return hash & (shard_set_size - 1);
  return hash % shard_set_size;
}

struct KeyLockArgs {
  DbIndex db_index = 0;
  std::vector<LockFp> fps;
};

// 由 key 计算锁指纹（LockFp）。
inline LockFp KeyFingerprint(std::string_view key) {
  return std::hash<std::string_view>{}(key);
}

class DbContext {
 public:
  DbContext() = default;
  DbContext(DbIndex index, uint64_t time_now_ms)
      : db_index_(index), time_now_ms_(time_now_ms) {}
  DbContext(const DbContext& o) noexcept { *this = o; }
  DbContext& operator=(const DbContext& o) noexcept {
    db_index_ = o.db_index_;
    time_now_ms_ = o.time_now_ms_;
    return *this;
  }
  DbIndex GetDbIndex() const { return db_index_; }
  uint64_t GetTimeNowMs() const { return time_now_ms_; }

 private:
  DbIndex db_index_;
  uint64_t time_now_ms_;
};




}  // namespace dfly
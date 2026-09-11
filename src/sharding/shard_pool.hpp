#pragma once

#include <glog/logging.h>

#include <cstdint>
#include <memory>

#include "io/uring_proactor_pool.hpp"
#include "shard.hpp"

namespace dfly {

class ShardPool {
 public:
  explicit ShardPool(base::UringProactorPool* pp) : pp_(pp) {}

  uint32_t size() const { return size_; }
  base::UringProactorPool* pool() { return pp_; }
  void Init(uint32_t size);
  void Shutdown();

  template <typename F>
  void Post(ShardId sid, F&& f) {
    DCHECK_LT(sid, size_);
    bool success = shards_[sid]->GetQueue()->TryAdd(std::forward<F>(f));
    if (!success) {
      LOG(FATAL) << "Shard " << sid << " task queue overflow, TryAdd failed";
    }
  }

  Shard* At(ShardId sid) { return shards_[sid]; }

  // 在每个分片线程上点火 raft 日志刷新定时器。必须在 Init() 之后调用
  // —— 那时 shards_[sid] 已全部填好、thread_local shard_ 已赋值。
  // 不能放在 Shard 构造函数里，见 raft.md §12.7。
  void StartRaftLogTimers() {
    for (ShardId sid = 0; sid < size_; ++sid) {
      Post(sid, [] { Shard::tlocal()->StartRaftLogTimer(); });
    }
  }

 private:
  void InitThreadLocal(base::UringProactor* pb);

  base::UringProactorPool* pp_;
  std::unique_ptr<Shard*[]> shards_;
  uint32_t size_ = 0;
};

extern ShardPool* shard_pool;
}  // namespace dfly

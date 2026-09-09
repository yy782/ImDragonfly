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

 private:
  void InitThreadLocal(base::UringProactor* pb);

  base::UringProactorPool* pp_;
  std::unique_ptr<Shard*[]> shards_;
  uint32_t size_ = 0;
};

extern ShardPool* shard_pool;
}  // namespace dfly

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
  void Post(ShardId sid, F&& f) {  // 只能在mpmc模式调用
    DCHECK_LT(sid, size_);
    bool success = shards_[sid]->GetQueue()->TryAdd(std::forward<F>(f));
    if (!success) {
      LOG(FATAL) << "Shard " << sid << " task queue overflow, TryAdd failed";
    }
  }

  template <typename F>
  void PostShard(ShardId consumer, ShardId producer,
                 F&& f) {  // 只能在spsc模式调用
    DCHECK_LT(consumer, size_);
    bool success =
        shards_[consumer]->GetQueue()->TryAdd(producer, std::forward<F>(f));
    if (!success) {
      LOG(FATAL) << "Shard " << producer << " -> " << consumer
                 << " task queue overflow, TryAdd failed";
    }
  }

  template <typename F>
  void BroadcastFromMain(F&& f) {  // 只能在spsc模式调用
    DCHECK(main_queue_);
    bool success = main_queue_->TryBroadcastFromMain(std::forward<F>(f));
    if (!success) {
      LOG(FATAL) << "main -> shards task queue overflow, "
                    "TryBroadcastFromMain failed";
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

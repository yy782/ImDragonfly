// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include <glog/logging.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <latch>
#include <memory>

#include "engine_shard.hpp"
#include "net/uring_proactor_pool.hpp"
#include "util/Time.hpp"
namespace dfly {
class TieredStorage;
class ShardDocIndices;
class BlockingController;
class EngineShardSet;

class EngineShardSet {
 public:
  explicit EngineShardSet(base::UringProactorPool* pp) : pp_(pp) {}
  uint32_t size() const { return size_; }
  base::UringProactorPool* pool() { return pp_; }
  void Init(uint32_t size);
  void PreShutdown();
  void Shutdown();

  template <typename F>
  auto Add(ShardId sid, F&& f) {
    DCHECK_LT(sid, size_);
    bool success = shards_[sid]->GetQueue()->TryAdd(std::forward<F>(f));
    if (!success) {
      // 队列满溢：非阻塞 TryAdd 失败，任务被丢弃会导致等待方永久挂起。
      // Debug/Release 行为一致，统一用 LOG(FATAL) 暴露问题。
      LOG(FATAL) << "Shard " << sid << " task queue overflow, TryAdd failed";
    }
    return success;
  }
  template <typename U>
  void RunBlockingInParallel(U&& func) {
    RunBlockingInParallel(std::forward<U>(func), [](uint32_t) { return true; });
  }
  template <typename U, typename P>
  void RunBlockingInParallel(U&& func, P&& pred) {
    uint32_t Count = 0;
    for (uint32_t i = 0; i < size(); ++i) {
      if (!pred(i)) continue;
      Count++;
    }
    std::latch latch(Count);
    for (uint32_t i = 0; i < size(); ++i) {
      if (!pred(i)) continue;
      auto dest = pp_->at(i);
      dest->DispatchBrief([&func, &latch, i]() mutable {
        func(EngineShard::tlocal());
        latch.count_down();
      });
    }
    latch.wait();
  }

  template <typename Func, typename P>
  void DispatchBriefInParallel(Func&& f, P&& pred) {
    for (uint32_t i = 0; i < size(); ++i) {
      if (!pred(i)) continue;
      auto dest = pp_->at(i);
      dest->DispatchBrief([f]() mutable { f(EngineShard::tlocal()); });
    }
  }

 private:
  void InitThreadLocal(base::UringProactor* pb);
  base::UringProactorPool* pp_;
  std::unique_ptr<EngineShard*[]> shards_;
  uint32_t size_ = 0;
};

extern EngineShardSet* shard_set;
}  // namespace dfly
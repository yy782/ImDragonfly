// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include <glog/logging.h>
#include <latch>
#include <memory>

#include "Time.hpp"
#include "YY/net/EventLoopThreadPool.h"
#include "engine_shard.hpp"
namespace dfly {
class TieredStorage;
class ShardDocIndices;
class BlockingController;
class EngineShardSet;

class EngineShardSet {
 public:
  explicit EngineShardSet(yy::net::EventLoopThreadPool* pp) : pp_(pp) {}
  uint32_t size() const { return size_; }
  yy::net::EventLoopThreadPool* pool() { return pp_; }
  void Init(uint32_t size);
  void PreShutdown();
  void Shutdown();

  template <typename F>
  auto Add(ShardId sid, F&& f) {
    DCHECK_LT(sid, size_);
    bool success = shards_[sid]->GetQueue()->TryAdd(std::forward<F>(f));
    // 暂时使用TryAdd,
    // 以后测试了AsyncAdd()函数的准确性，再切换，release下出错，切debug模式就ok了
    DCHECK(success);
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
      dest->DispatchBrief([&func, &latch]() mutable {
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

  yy::net::EventLoop* pool(int idx) const { return pp_->at(idx); }

 private:
  void InitThreadLocal(yy::net::EventLoop* pb);
  yy::net::EventLoopThreadPool* pp_;
  std::unique_ptr<EngineShard*[]> shards_;
  uint32_t size_ = 0;
};

extern EngineShardSet* shard_set;
}  // namespace dfly

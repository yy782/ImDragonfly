// ============================================================================
// shard_pool.cpp —— 分片线程池门面 ShardPool 实现（独立设计）
// ============================================================================
#include "engine_shard_set.hpp"

#include <glog/logging.h>

#include <latch>
#include <memory>

#include "detail/common.hpp"
#include "util/maths.hpp"

namespace dfly {

ShardPool* shard_pool = nullptr;

void ShardPool::Init(uint32_t sz) {
  LOG(INFO) << "Initializing ShardPool with " << sz << " shards";
  shards_.reset(new EngineShard*[sz]);
  size_ = sz;

  pp_->AwaitOnAll([this](std::shared_ptr<base::UringProactor> pb) {
    InitThreadLocal(pb.get());
  });

  LOG(INFO) << "ShardPool initialized with " << sz << " shards";
}

void ShardPool::Shutdown() {
  // 分片线程各自销毁本线程的 EngineShard，主线程等待全部完成。
  std::latch latch(size_);
  for (uint32_t i = 0; i < size_; ++i) {
    pp_->at(i)->DispatchBrief([&latch]() mutable {
      EngineShard::DestroyThreadLocal();
      latch.count_down();
    });
  }
  latch.wait();
}

void ShardPool::InitThreadLocal(base::UringProactor* pb) {
  EngineShard::InitThreadLocal(pb);
  EngineShard* es = EngineShard::tlocal();
  shards_[es->shard_id()] = es;
  util::Thread::set_cpu_affinity(es->shard_id());
}

}  // namespace dfly

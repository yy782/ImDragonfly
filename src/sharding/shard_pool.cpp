#include "shard_pool.hpp"

#include <glog/logging.h>

#include <cstdlib>
#include <latch>
#include <memory>

#include "detail/common_types.hpp"
#include "util/maths.hpp"
#include "detail/task_queue.hpp"

namespace dfly {

ShardPool* shard_pool = nullptr;

void ShardPool::Init(uint32_t sz) {

  if constexpr (!dfly::kUseMpmcTaskQueue) {
    if (sz == 0 || (sz & (sz - 1)) != 0) {
      LOG(ERROR) << "shards 必须是 2 的幂（SPSC 分片段队列要求），当前: " << sz;
      std::exit(1);
    }
  }
  LOG(INFO) << "Initializing ShardPool with " << sz << " shards";
  shards_.reset(new Shard*[sz]);
  size_ = sz;

  // 段式分片段队列必须先分配段内存（segs_）才能接收投递：
  if constexpr (!dfly::kUseMpmcTaskQueue) {
    for (uint32_t i = 0; i < sz; ++i) {
      pp_->at(i)->GetTaskQueue().InitShardQueue(i, sz);
    }
  }

  pp_->AwaitOnAllFromMain([this](base::UringProactor* pb) { InitThreadLocal(pb); });

  LOG(INFO) << "ShardPool initialized with " << sz << " shards";
}

void ShardPool::Shutdown() {
  std::latch latch(size_);
  for (uint32_t i = 0; i < size_; ++i) {
    pp_->at(i)->DispatchBriefFromMain([&latch]() mutable {
      Shard::DestroyThreadLocal();
      latch.count_down();
    });
  }
  latch.wait();
}

void ShardPool::InitThreadLocal(base::UringProactor* pb) {
  Shard::InitThreadLocal(pb);
  Shard* es = Shard::tlocal();
  shards_[es->shard_id()] = es;
  util::Thread::set_cpu_affinity(es->shard_id());
}

}  // namespace dfly

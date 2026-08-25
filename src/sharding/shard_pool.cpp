#include "shard_pool.hpp"

#include <glog/logging.h>

#include <latch>
#include <memory>

#include "detail/common_types.hpp"
#include "util/maths.hpp"

namespace dfly {

ShardPool* shard_pool = nullptr;

void ShardPool::Init(uint32_t sz) {
  LOG(INFO) << "Initializing ShardPool with " << sz << " shards";
  shards_.reset(new Shard*[sz]);
  size_ = sz;

  pp_->AwaitOnAll([this](base::UringProactor* pb) { InitThreadLocal(pb); });

  LOG(INFO) << "ShardPool initialized with " << sz << " shards";
}

void ShardPool::Shutdown() {
  std::latch latch(size_);
  for (uint32_t i = 0; i < size_; ++i) {
    pp_->at(i)->DispatchBrief([&latch]() mutable {
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

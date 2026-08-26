#include "shard_pool.hpp"

#include <glog/logging.h>

#include <cstdlib>
#include <latch>
#include <memory>

#include "detail/common_types.hpp"
#include "detail/task_queue.hpp"
#include "util/maths.hpp"
#include "util/startup_log.hpp"

namespace dfly {

ShardPool* shard_pool = nullptr;

void ShardPool::Init(uint32_t sz) {
  if constexpr (!dfly::kUseMpmcTaskQueue) {
    if (sz == 0 || (sz & (sz - 1)) != 0) {
      LOG(ERROR) << "shards 必须是 2 的幂（SPSC 分片段队列要求），当前: " << sz;
      std::exit(1);
    }
    dfly::InitShardQueueIfSpsc(*dfly::main_queue_, sz);
  }
  LOG(INFO) << "Initializing ShardPool with " << sz << " shards";
  shards_.reset(new Shard*[sz]);
  size_ = sz;

  for (uint32_t i = 0; i < sz; ++i) {
    dfly::InitShardQueueIfSpsc(pp_->at(i)->GetTaskQueue(), sz);
  }

  pp_->DispatchBriefFromMain(
      [this](base::UringProactor* pb) { InitThreadLocal(pb); });

  LOG(INFO) << "ShardPool initialized with " << sz << " shards";
}

void ShardPool::Shutdown() {
  std::latch latch(size_);
  pp_->DispatchBriefFromMain([&latch](base::UringProactor*) {
    Shard::DestroyThreadLocal();
    latch.count_down();
  });
  latch.wait();
}

void ShardPool::InitThreadLocal(base::UringProactor* pb) {
  Shard::InitThreadLocal(pb);
  Shard* es = Shard::tlocal();
  shards_[es->shard_id()] = es;
  util::Thread::set_cpu_affinity(es->shard_id());
  util::StartupLog("Shard " + std::to_string(es->shard_id()) +
                   " thread-local initialized (tid=" +
                   std::to_string(util::Thread::current_tid()) + ")");
}

}  // namespace dfly

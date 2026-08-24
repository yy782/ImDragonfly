// ============================================================================
// shard_pool.hpp —— 分片线程池门面 ShardPool（独立设计）
//
// 职责边界（按论文语义划分）：
//   - 分片线程生命周期：Init 创建（绑定 proactor、CPU 亲和性），
//     Shutdown 销毁；
//   - 按分片路由：Post(sid, task) 把任务投递到指定分片线程的任务队列，
//     是"协调者 → 分片"的投递原语（调度链上唯一需要的跨分片通信）；
//   - At(sid) / size()：按 sid 访问分片实例。
//
// 不做的事：不提供"跨分片并行执行"集合工具。调度路径上的集合点由
// Transaction::barrier_ 承担（见 transaction.cpp 的 hop 投递与回滚广播），
// 本类不再重复实现同步设施。
// ============================================================================
#pragma once

#include <glog/logging.h>

#include <cstdint>
#include <memory>

#include "engine_shard.hpp"
#include "net/uring_proactor_pool.hpp"

namespace dfly {

class ShardPool {
 public:
  explicit ShardPool(base::UringProactorPool* pp) : pp_(pp) {}

  uint32_t size() const { return size_; }
  base::UringProactorPool* pool() { return pp_; }

  void Init(uint32_t size);
  void Shutdown();

  // 按 sid 路由：把任务投递到指定分片线程执行。
  template <typename F>
  void Post(ShardId sid, F&& f) {
    DCHECK_LT(sid, size_);
    bool success = shards_[sid]->GetQueue()->TryAdd(std::forward<F>(f));
    if (!success) {
      // 队列满溢：非阻塞 TryAdd 失败，任务被丢弃会导致等待方永久挂起。
      // Debug/Release 行为一致，统一用 LOG(FATAL) 暴露问题。
      LOG(FATAL) << "Shard " << sid << " task queue overflow, TryAdd failed";
    }
  }

  EngineShard* At(ShardId sid) { return shards_[sid]; }

 private:
  void InitThreadLocal(base::UringProactor* pb);

  base::UringProactorPool* pp_;
  std::unique_ptr<EngineShard*[]> shards_;
  uint32_t size_ = 0;
};

extern ShardPool* shard_pool;
}  // namespace dfly

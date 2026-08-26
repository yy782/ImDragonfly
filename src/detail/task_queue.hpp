#pragma once
#include <glog/logging.h>

#include <atomic>
#include <cassert>
#include <cstddef>

#include "cppcoro/async_task.hpp"
#include "detail/common_types.hpp"
#include "detail/spsc_shard_queue.hpp"
#include "util/function.hpp"
#include "util/mi_memory_resource.hpp"
#include "util/mpmc_queue.hpp"
#include "util/synchronization.hpp"

namespace dfly {

inline constexpr bool kUseMpmcTaskQueue = true;

inline constexpr size_t kMaxPerSegment = 64;

template <bool UseMpmc>
class TaskQueueImpl;

using TaskQueue = TaskQueueImpl<kUseMpmcTaskQueue>;

inline TaskQueue* main_queue_ = nullptr;

template <>
class TaskQueueImpl<true> {
 public:
  using CbFunc = util::unique_function<void()>;

  explicit TaskQueueImpl(
      unsigned queue_size = 128,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource());

  template <typename F>
  bool TryAdd(F&& f) {
    return queue_.try_enqueue(std::forward<F>(f));
  }

  template <typename F>
  bool TryAdd(ShardId, F&&) {
    static_assert(sizeof(F) == 0,
                  "TaskQueue: TryAdd(ShardId, F&&) not supported; "
                  "MPMC 下没有分片区分，请用 TryAdd(F&&)");
    return false;
  }

  template <typename F>
  bool TryBroadcastFromMain(F&&) {
    static_assert(sizeof(F) == 0,
                  "TaskQueue: TryBroadcastFromMain(F&&) not supported; "
                  "MPMC 下没有分片区分，请用 TryAdd(F&&)");
    return false;
  }

  template <typename F>
  bool TryPostFromMain(ShardId, F&&) {
    static_assert(sizeof(F) == 0,
                  "TaskQueue: TryPostFromMain(ShardId, F&&) not supported; "
                  "MPMC 下没有分片区分，请用 TryAdd(F&&)");
    return false;
  }

  bool TryDrain();
  // 模板化使 static_assert 惰性：MPMC 下无人调用则不实例化、
  // 不报错；一旦误用（调用 TryDrainSeg）才在编译期拒斥。
  template <typename F = void>
  bool TryDrainSeg(uint32_t /*max_task_num*/, ShardId /*seg*/) {
    static_assert(sizeof(F) == 0, "TaskQueue: TryDrainSeg not supported");
    return false;
  }
  bool Empty() const;

 private:
  using FuncQ = util::mpmc_queue<CbFunc>;
  FuncQ queue_;
};

template <>
class TaskQueueImpl<false> {
 public:
  using CbFunc = util::function<void()>;

  explicit TaskQueueImpl(
      unsigned queue_size = 128,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource());

  void InitShardQueue(size_t shard_num);

  template <typename F>
  bool TryAdd(F&&) {
    static_assert(sizeof(F) == 0,
                  "TaskQueue: TryAdd(F&&) not supported; use TryPostFromMain/"
                  "TryBroadcastFromMain (main 语境) or TryAdd(ShardId, F&&)/"
                  "PostShard (分片语境)");
    return false;
  }

  template <typename F>
  bool TryAdd(ShardId producer, F&& f) {
    return shard_queue_.TryAdd(producer, std::forward<F>(f));
  }

  template <typename F>
  bool TryBroadcastFromMain(F&& f) {  // 主线程调用
    return shard_queue_.TryAddForAllSeg(std::forward<F>(f));
  }

  template <typename F>
  bool TryPostFromMain(ShardId seg, F&& f) {  // 主线程调用
    return shard_queue_.TryAddForSeg(seg, std::forward<F>(f));
  }

  bool TryDrain();
  bool TryDrainSeg(uint32_t max_task_num, ShardId seg);
  bool Empty() const;

 private:
  spsc_shard_queue<CbFunc> shard_queue_;
  unsigned queue_size_;
  std::pmr::memory_resource* mr_ = nullptr;
};

template <typename Q>
void InitShardQueueIfSpsc(Q& queue, size_t shard_num) {
  if constexpr (!kUseMpmcTaskQueue) {
    queue.InitShardQueue(shard_num);
  }
}

}  // namespace dfly

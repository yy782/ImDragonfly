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

template <bool UseMpmc>
class TaskQueueImpl;

template <>
class TaskQueueImpl<true> {
 public:
  using CbFunc = util::unique_function<void()>;

  explicit TaskQueueImpl(
      unsigned queue_size = 128,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource())
      : queue_(queue_size, mr) {}

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
  bool TryAddFromMain(F&&) {
    static_assert(sizeof(F) == 0,
                  "TaskQueue: TryAddFromMain(F&&) not supported; "
                  "MPMC 下没有 main/分片区分，请用 TryAdd(F&&)");
    return false;
  }

  void Shutdown() { is_closed_.store(true, std::memory_order_seq_cst); }

  bool TryDrain() {
    CbFunc func;
    while (queue_.try_dequeue(func)) {
      func();
    }
    return true;
  }

  bool isRuning() const { return !is_closed_.load(std::memory_order_relaxed); }

  bool Empty() const { return queue_.empty(); }

 private:
  using FuncQ = util::mpmc_queue<CbFunc>;
  FuncQ queue_;
  std::atomic<bool> is_closed_{false};
};

template <>
class TaskQueueImpl<false> {
 public:
  using CbFunc = util::unique_function<void()>;

  explicit TaskQueueImpl(
      unsigned queue_size = 128,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource())
      : queue_size_(queue_size), mr_(mr) {
    assert((queue_size & (queue_size - 1)) == 0 &&
           "TaskQueue: queue_size must be a power of two");
  }

  void InitShardQueue(ShardId owner_id, size_t shard_num) {
    shard_queue_.Init(queue_size_, owner_id, shard_num, mr_);
  }

  template <typename F>
  bool TryAdd(F&&) {
    static_assert(sizeof(F) == 0,
                  "TaskQueue: TryAdd(F&&) not supported; use TryAddFromMain "
                  "(main 语境) or TryAdd(ShardId, F&&)/PostShard (分片语境)");
    return false;
  }

  template <typename F>
  bool TryAdd(ShardId producer, F&& f) {
    return shard_queue_.TryAdd(producer, std::forward<F>(f));
  }

  template <typename F>
  bool TryAddFromMain(F&& f) {
    return shard_queue_.TryAddFromMain(std::forward<F>(f));
  }

  void Shutdown() { is_closed_.store(true, std::memory_order_seq_cst); }

  bool TryDrain() { return shard_queue_.TryDrain(); }

  bool isRuning() const { return !is_closed_.load(std::memory_order_relaxed); }

  bool Empty() const { return shard_queue_.Empty(); }

 private:
  spsc_shard_queue<CbFunc> shard_queue_;
  unsigned queue_size_;
  std::pmr::memory_resource* mr_ = nullptr;
  std::atomic<bool> is_closed_{false};
};

template <typename Q>
void InitShardQueueIfSpsc(Q& queue, ShardId owner_id, size_t shard_num) {
  if constexpr (!kUseMpmcTaskQueue) {
    queue.InitShardQueue(owner_id, shard_num);
  }
}

using TaskQueue = TaskQueueImpl<kUseMpmcTaskQueue>;

}  // namespace dfly

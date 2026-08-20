
#pragma once
#include <glog/logging.h>

#include <atomic>

#include "cppcoro/async_task.hpp"
#include "detail/memory_resource.hpp"
#include "util/function.hpp"
#include "util/mpmc_queue.hpp"
#include "util/synchronization.hpp"
namespace util {

class TaskQueue {
 public:
  explicit TaskQueue(
      unsigned queue_size = 128,
      PMR_NS::memory_resource* mr = PMR_NS::get_default_resource())
      : queue_(queue_size, mr) {}

  template <typename F>
  bool TryAdd(F&& f) {
    bool enqueued = queue_.try_enqueue(std::forward<F>(f));
    if (enqueued) {
      return true;
    }
    return false;
  }

  void Shutdown() { is_closed_.store(true, std::memory_order_seq_cst); }

  bool TryDrain() {
    CbFunc func;
    while (queue_.try_dequeue(func)) {
      func();  // 禁止异常， func里抛异常直接abort
    }
    return true;
  }

  bool isRuning() const { return !is_closed_.load(std::memory_order_relaxed); }

  bool Empty() const { return queue_.empty(); }

  using CbFunc = util::unique_function<void()>;

 private:
  using FuncQ = util::mpmc_queue<CbFunc>;
  FuncQ queue_;
  std::atomic<bool> is_closed_{false};
};

}  // namespace util
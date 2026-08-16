
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
      pull_ec_.notify();
      return true;
    }
    return false;
  }

  template <typename F>
  cppcoro::AsyncTask AsyncAdd(F&& f) {
    while (true) {
      auto key = push_ec_.prepareWait();
      if (TryAdd(std::forward<F>(f))) {
        break;
      }
      co_await push_ec_.wait(
          key.epoch());  // 这里挂起协程后，
                         // 负责执行下文的线程是处理任务队列的线程
    }
    co_return;
  }

  void Shutdown() {
    is_closed_.store(true, std::memory_order_seq_cst);
    pull_ec_.notifyAll();
  }

  void Run() {  // 目前没用这个接口，pull_ec_是多余了
    CbFunc func;
    while (true) {
      pull_ec_.wait();
      if (is_closed_.load(std::memory_order_acquire)) break;
      if (!queue_.try_dequeue(func)) {
        continue;
      }
      push_ec_.notify();
      func();
    }
  }

  bool TryDrain() {
    CbFunc func;
    while (queue_.try_dequeue(func)) {
      push_ec_.notify();
      func();
    }
    return true;
  }

  bool isRuning() const { return !is_closed_.load(std::memory_order_relaxed); }

  bool Empty() const { return queue_.empty(); }

  using CbFunc = util::unique_function<void()>;

 private:
  using FuncQ = util::mpmc_queue<CbFunc>;

  FuncQ queue_;

  EventCount push_ec_;
  ThreadEvent pull_ec_;

  std::atomic<bool> is_closed_{false};
};

}  // namespace util
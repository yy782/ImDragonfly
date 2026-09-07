#pragma once
#include <glog/logging.h>

#include <atomic>
#include <cassert>
#include <cstddef>

#include "cppcoro/async_task.hpp"
#include "detail/common_types.hpp"
#include "util/function.hpp"
#include "util/mi_memory_resource.hpp"
#include "util/mpsc_queue.hpp"
#include "util/synchronization.hpp"

namespace dfly {

class TaskQueue {
 public:
  using CbFunc = util::unique_function<void()>;

  explicit TaskQueue(
      unsigned queue_size = 128,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource());

  template <typename F>
  bool TryAdd(F&& f) {
    return queue_.try_enqueue(std::forward<F>(f));
  }

  bool TryDrain();
  bool Empty() const;

 private:
  using FuncQ = util::mpsc_queue<CbFunc>;
  FuncQ queue_;
};

}  // namespace dfly

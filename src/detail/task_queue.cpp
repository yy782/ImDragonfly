#include "detail/task_queue.hpp"

#include <cassert>

namespace dfly {

TaskQueue::TaskQueue(unsigned queue_size, std::pmr::memory_resource* mr)
    : queue_(queue_size, mr) {}

bool TaskQueue::TryDrain() {
  CbFunc func;
  while (queue_.try_dequeue(func)) {
    func();
  }
  return true;
}

bool TaskQueue::Empty() const { return queue_.empty(); }

}  // namespace dfly

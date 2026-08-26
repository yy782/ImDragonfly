#include "detail/task_queue.hpp"

#include <cassert>

#include "sharding/shard.hpp"

namespace dfly {

TaskQueueImpl<true>::TaskQueueImpl(unsigned queue_size,
                                   std::pmr::memory_resource* mr)
    : queue_(queue_size, mr) {}

void TaskQueueImpl<true>::Shutdown() {
  is_closed_.store(true, std::memory_order_seq_cst);
}

bool TaskQueueImpl<true>::TryDrain() {
  CbFunc func;
  while (queue_.try_dequeue(func)) {
    func();
  }
  return true;
}

bool TaskQueueImpl<true>::isRuning() const {
  return !is_closed_.load(std::memory_order_relaxed);
}

bool TaskQueueImpl<true>::Empty() const { return queue_.empty(); }

TaskQueueImpl<false>::TaskQueueImpl(unsigned queue_size,
                                    std::pmr::memory_resource* mr)
    : queue_size_(queue_size), mr_(mr) {
  assert((queue_size & (queue_size - 1)) == 0 &&
         "TaskQueue: queue_size must be a power of two");
}

void TaskQueueImpl<false>::InitShardQueue(size_t shard_num) {
  shard_queue_.Init(queue_size_, shard_num, mr_);
}

void TaskQueueImpl<false>::Shutdown() {
  is_closed_.store(true, std::memory_order_seq_cst);
}

bool TaskQueueImpl<false>::TryDrain() {
  auto* main_q = reinterpret_cast<TaskQueueImpl<false>*>(dfly::main_queue_);
  if (this == main_q) return false;  // main 队列：main 不消费

  bool progress = false;
  if (main_q->TryDrainSeg(kMaxPerSegment, Shard::tlocal()->shard_id())) {
    progress = true;
  }
  for (size_t i = 0; i < shard_queue_.ShardNum(); ++i) {
    if (shard_queue_.TryDrain(kMaxPerSegment, i)) {
      progress = true;
    }
  }
  return progress;
}

bool TaskQueueImpl<false>::TryDrainSeg(uint32_t max_task_num, ShardId seg) {
  return shard_queue_.TryDrain(max_task_num, seg);
}

bool TaskQueueImpl<false>::isRuning() const {
  return !is_closed_.load(std::memory_order_relaxed);
}

bool TaskQueueImpl<false>::Empty() const { return shard_queue_.Empty(); }

}  // namespace dfly

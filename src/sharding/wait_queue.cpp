// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/sharding/wait_queue.hpp"

#include "src/sharding/engine_shard_set.hpp"
namespace dfly {
namespace detail {

void WaitQueue::Link(Waiter* waiter) { wait_list_.push_back(*waiter); }

void WaitQueue::Unlink(Waiter* waiter) {
  auto it = WaitList::s_iterator_to(*waiter);
  wait_list_.erase(it);
}

bool WaitQueue::NotifyOne() {
  if (wait_list_.empty()) return false;

  Waiter* waiter = &wait_list_.front();

  wait_list_.pop_front();

  shard_set->Add(waiter->shard_id, [waiter]() { waiter->handler.resume(); });
  return true;
}

bool WaitQueue::NotifyAll() {
  bool notified = false;
  auto it = wait_list_.begin();

  while (it != wait_list_.end()) {
    Waiter& waiter = *it;
    it = wait_list_.erase(it);
    shard_set->Add(waiter.shard_id, [waiter]() { waiter.handler.resume(); });
    notified = true;
  }
  return notified;
}

}  // namespace detail
}  // namespace dfly

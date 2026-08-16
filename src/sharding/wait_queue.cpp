// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/sharding/wait_queue.hpp"

namespace dfly {
namespace detail {

void WaitQueue::Link(Waiter* waiter) { wait_list_.push_back(*waiter); }

void WaitQueue::Unlink(Waiter* waiter) {
  auto it = WaitList::s_iterator_to(*waiter);
  wait_list_.erase(it);
}

bool WaitQueue::NotifyOne(Waiter*& out) {
  if (wait_list_.empty()) return false;

  out = &wait_list_.front();
  wait_list_.pop_front();
  return true;
}

bool WaitQueue::NotifyAll(std::vector<Waiter*>& out) {
  if (wait_list_.empty()) return false;

  auto it = wait_list_.begin();
  while (it != wait_list_.end()) {
    Waiter& waiter = *it;
    it = wait_list_.erase(it);
    out.push_back(&waiter);
  }
  return true;
}

}  // namespace detail
}  // namespace dfly

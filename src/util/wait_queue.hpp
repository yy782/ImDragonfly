// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <assert.h>

#include <boost/intrusive/list.hpp>
#include <coroutine>
#include <functional>
#include <variant>
#include <vector>
namespace util {
namespace detail {

struct Waiter {
  std::coroutine_handle<> handler;
  using ListHookType = boost::intrusive::list_member_hook<
      boost::intrusive::link_mode<boost::intrusive::safe_link>>;
  ListHookType wait_hook{};

  bool IsLinked() const { return wait_hook.is_linked(); }

  ~Waiter() { assert(!IsLinked()); }
};

class WaitQueue {
 public:
  bool empty() const { return wait_list_.empty(); }

  void Link(Waiter* waiter) { wait_list_.push_back(*waiter); }

  void Unlink(Waiter* waiter) {
    auto it = WaitList::s_iterator_to(*waiter);
    wait_list_.erase(it);
  }

  bool NotifyOne(std::coroutine_handle<>& out_handler) {
    if (wait_list_.empty()) return false;

    Waiter* waiter = &wait_list_.front();
    wait_list_.pop_front();
    out_handler = waiter->handler;
    return true;
  }
  bool NotifyAll(std::vector<std::coroutine_handle<>>& out_handlers) {
    if (wait_list_.empty()) return false;

    auto it = wait_list_.begin();
    while (it != wait_list_.end()) {
      Waiter& waiter = *it;
      it = wait_list_.erase(it);
      out_handlers.push_back(waiter.handler);
    }
    return true;
  }

 private:
  using WaitList = boost::intrusive::list<
      Waiter,
      boost::intrusive::member_hook<Waiter, Waiter::ListHookType,
                                    &Waiter::wait_hook>,
      boost::intrusive::constant_time_size<false>>;

  WaitList wait_list_;
};

}  // namespace detail
}  // namespace util
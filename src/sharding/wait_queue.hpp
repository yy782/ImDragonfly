// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <glog/logging.h>

#include <coroutine>
#include <vector>

#include "detail/common_types.hpp"
#include "util/intrusive_list.hpp"
namespace dfly {
namespace detail {

struct Waiter {
  std::coroutine_handle<> handler;
  using ListHookType = util::list_member_hook<util::link_mode::safe_link>;
  ListHookType wait_hook{};
  ShardId shard_id = -1;
  bool IsLinked() const { return wait_hook.is_linked(); }

  ~Waiter() { DCHECK(!IsLinked()); }
};

class WaitQueue {
 public:
  bool empty() const { return wait_list_.empty(); }

  void Link(Waiter* waiter);

  void Unlink(Waiter* waiter);

  bool NotifyOne(Waiter*& out);

  bool NotifyAll(std::vector<Waiter*>& out);

 private:
  using WaitList = util::intrusive_list<Waiter, Waiter::ListHookType,
                                        &Waiter::wait_hook, false>;

  WaitList wait_list_;
};

}  // namespace detail
}  // namespace dfly

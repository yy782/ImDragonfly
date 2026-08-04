// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <glog/logging.h>

#include <boost/intrusive/list.hpp>
#include <coroutine>

#include "detail/common_types.hpp"
namespace dfly {
namespace detail {

struct Waiter {
  std::coroutine_handle<> handler;
  using ListHookType = boost::intrusive::list_member_hook<
      boost::intrusive::link_mode<boost::intrusive::safe_link>>;
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


  bool NotifyOne();


  bool NotifyAll();

 private:
  using WaitList = boost::intrusive::list<
      Waiter,
      boost::intrusive::member_hook<Waiter, Waiter::ListHookType,
                                    &Waiter::wait_hook>,
      boost::intrusive::constant_time_size<false>>;

  WaitList wait_list_;
};

}  // namespace detail
}  // namespace dfly

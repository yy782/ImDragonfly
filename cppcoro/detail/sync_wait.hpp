

#ifndef COROUTINE_SYNC_WAIT_H
#define COROUTINE_SYNC_WAIT_H

#include "lightweight_manual_reset_event.hpp"
#include "sync_wait_task.hpp"
#include "awaiter_impl.hpp"

#include <cstdint>
#include <atomic>

namespace cppcoro
{
template<typename AWAITABLE>
auto sync_wait(AWAITABLE&& awaitable)
-> typename detail::awaitable_traits<AWAITABLE&&>::await_result_t
{
  auto task = detail::make_sync_wait_task(std::forward<AWAITABLE>(awaitable));
  detail::lightweight_manual_reset_event event;
  task.start(event);
  event.wait();
  return task.result();
}
}

#endif 

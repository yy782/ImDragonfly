// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
// synchronization

#include "sharding/synchronization.hpp"

#include "engine_shard.hpp"
#include "engine_shard_set.hpp"

namespace dfly {

bool EventCount::notify() noexcept {
  uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_release);
  if (prev & kWaiterMask) {
    detail::Waiter* waiter = nullptr;
    {
      std::unique_lock lk(lock_);
      if (!wait_queue_.NotifyOne(waiter)) return false;
    }
    auto handler = waiter->handler;
    ShardId sid = waiter->shard_id;
    shard_set->Add(sid, [handler]() { handler.resume(); });
    return true;
  }
  return false;
}

bool EventCount::notifyAll() noexcept {
  uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_release);
  if (prev & kWaiterMask) {
    std::vector<detail::Waiter*> waiters;
    {
      std::unique_lock lk(lock_);
      if (!wait_queue_.NotifyAll(waiters)) return false;
    }
    for (detail::Waiter* waiter : waiters) {
      auto handler = waiter->handler;
      ShardId sid = waiter->shard_id;
      shard_set->Add(sid, [handler]() { handler.resume(); });
    }
    return true;
  }
  return false;
}

bool EventCount::WaitAwaitable::await_suspend(
    std::coroutine_handle<> awaitingCoroutine) noexcept {
  std::unique_lock lk(event_->lock_);
  if ((event_->val_.load(std::memory_order_relaxed) >> event_->kEpochShift) ==
      epoch_) {
    waiter.handler = awaitingCoroutine;
    waiter.shard_id = EngineShard::tlocal()->shard_id();
    event_->wait_queue_.Link(&waiter);
    lk.unlock();
    SuspendWithResume = true;
  } else {
    SuspendWithResume = false;
    lk.unlock();
  }
  return SuspendWithResume;
}

}  // namespace dfly

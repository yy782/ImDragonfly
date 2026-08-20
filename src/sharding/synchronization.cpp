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
  if (!(prev & kWaiterMask)) return false;

  // 单 waiter 快路径：先 NotifyOne 试一次，队列空了说明只有一个 waiter，
  // 直接处理 single，零堆分配。多 waiter 才退化到 vector。
  detail::Waiter* single = nullptr;
  std::vector<detail::Waiter*> rest;
  {
    std::unique_lock lk(lock_);
    if (!wait_queue_.NotifyOne(single)) return false;
    if (!wait_queue_.empty()) {
      // 还有更多 waiter，把剩下的批量弹出
      wait_queue_.NotifyAll(rest);
    }
  }
  shard_set->Add(single->shard_id, [h = single->handler]() { h.resume(); });
  for (detail::Waiter* w : rest) {
    shard_set->Add(w->shard_id, [h = w->handler]() { h.resume(); });
  }
  return true;
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

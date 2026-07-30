#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "YY/net/EventLoopThreadPool.h"
#include "sharding/engine_shard_set.hpp"
#include "sharding/synchronization.hpp"

using namespace dfly;

class ThreadEventTest : public ::testing::Test {
 protected:
  ThreadEvent event_;
};

TEST_F(ThreadEventTest, NotifyBeforeWaitReturnsImmediately) {
  event_.notify();
  event_.wait();
  SUCCEED();
}

TEST_F(ThreadEventTest, NotifyWakesSingleWaiter) {
  std::atomic<bool> woken{false};

  std::thread t([&]() {
    event_.wait();
    woken.store(true);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(woken.load());

  event_.notify();
  t.join();

  EXPECT_TRUE(woken.load());
}

TEST_F(ThreadEventTest, NotifyAllWakesAllWaiters) {
  constexpr int kWaiters = 4;
  std::atomic<int> woken{0};
  std::vector<std::thread> threads;

  for (int i = 0; i < kWaiters; ++i) {
    threads.emplace_back([&]() {
      event_.wait();
      woken.fetch_add(1);
    });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(woken.load(), 0);

  event_.notifyAll();

  for (auto& t : threads) t.join();

  EXPECT_EQ(woken.load(), kWaiters);
}

TEST_F(ThreadEventTest, MultipleNotificationsAccumulate) {
  constexpr int kNotifyCount = 5;
  for (int i = 0; i < kNotifyCount; ++i) event_.notify();

  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < kNotifyCount; ++i) event_.wait();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(elapsed, std::chrono::milliseconds(100));
}

TEST_F(ThreadEventTest, ResetClearsPendingSignal) {
  event_.notify();
  EXPECT_NO_THROW(event_.wait());

  event_.notify();
  event_.reset();

  std::atomic<bool> woken{false};
  std::thread t([&]() {
    event_.wait();
    woken.store(true);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(woken.load());

  event_.notify();
  t.join();
  EXPECT_TRUE(woken.load());
}

TEST_F(ThreadEventTest, EachWaitConsumesOneNotification) {
  event_.notify();
  event_.notify();

  auto start = std::chrono::steady_clock::now();
  event_.wait();
  event_.wait();
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(elapsed, std::chrono::milliseconds(100));
}

// ============================================================================
// EventCount tests - uses EngineShardSet + EventLoopThreadPool
// ============================================================================
class EventCountTest : public ::testing::Test {
 protected:
  static constexpr int kNumShards = 4;

  void SetUp() override {
    pool_.run();
    shard_set = new EngineShardSet(&pool_);
    shard_set->Init(pool_.size());
  }

  void TearDown() override {
    pool_.stop();
    sleep(1);
    delete shard_set;
  }

  template <typename Pred>
  void WaitUntil(Pred&& pred, std::chrono::milliseconds timeout =
                                  std::chrono::milliseconds(5000)) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
      if (std::chrono::steady_clock::now() >= deadline) {
        FAIL() << "Timed out waiting for condition";
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  yy::net::EventLoopThreadPool pool_{kNumShards};
  EventCount ec_;
};

// notify 在 co_await 前调用，epoch 已变，协程不挂起。记录 wait 前 pid 并断言。
TEST_F(EventCountTest, CoroutineWaitEpochChangedNoSuspend) {
  std::atomic<bool> didSuspend{true};

  shard_set->Add(0, [&didSuspend, this]() {
    auto entry = [&]() -> cppcoro::AsyncTask {
      auto waiter = [&]() -> cppcoro::task<void> {
        auto key = ec_.prepareWait();
        ec_.notify();
        uint32_t before_sid = EngineShard::tlocal()->shard_id();
        bool suspended = co_await ec_.wait(key.epoch());
        uint32_t after_sid = EngineShard::tlocal()->shard_id();
        EXPECT_EQ(before_sid, after_sid);
        didSuspend.store(suspended);
      };
      co_await waiter();
      EXPECT_FALSE(didSuspend.load());
    };
    entry();
  });
}

// 多分片各自 wait，主线程 notifyAll 全部唤醒。每个分片记录 wait 前 pid。
TEST_F(EventCountTest, CoroutineNotifyAllWakesMultipleShards) {
  constexpr int kWaiters = kNumShards;
  std::atomic<int> woken{0};
  std::atomic<int> readyCount{0};
  std::vector<uint32_t> before_sids(kWaiters);

  for (int i = 0; i < kWaiters; ++i) {
    shard_set->Add(i, [&woken, &readyCount, &before_sids, this, i]() {
      auto entry = [&]() -> cppcoro::AsyncTask {
        auto waiter = [&]() -> cppcoro::task<void> {
          auto key = ec_.prepareWait();
          before_sids[i] = EngineShard::tlocal()->shard_id();
          co_await ec_.wait(key.epoch());
          uint32_t after_sid = EngineShard::tlocal()->shard_id();
          EXPECT_EQ(before_sids[i], after_sid);
          woken.fetch_add(1);
        };
        co_await waiter();
      };
      entry();
      readyCount.fetch_add(1);
    });
  }

  WaitUntil([&] { return readyCount.load() >= kWaiters; });
  EXPECT_EQ(woken.load(), 0);
  EXPECT_TRUE(ec_.notifyAll());
  WaitUntil([&] { return woken.load() >= kWaiters; });
  EXPECT_EQ(woken.load(), kWaiters);
}

// 条件已经为 true，协程走 fast path 不挂起。记录 pid 断言。
TEST_F(EventCountTest, AwaitFastPathConditionAlreadyTrue) {
  std::atomic<bool> flag{true};
  std::atomic<bool> preempt{true};

  shard_set->Add(0, [&flag, &preempt, this]() {
    auto entry = [&]() -> cppcoro::AsyncTask {
      auto coro = [&]() -> cppcoro::task<void> {
        uint32_t before_sid = EngineShard::tlocal()->shard_id();
        bool result = co_await ec_.await(
            [&] { return flag.load(std::memory_order_acquire); });
        uint32_t after_sid = EngineShard::tlocal()->shard_id();
        EXPECT_EQ(before_sid, after_sid);
        preempt.store(result);
      };
      co_await coro();
    };
    entry();
  });

  // Wait briefly for shard callback to execute
  std::this_thread::sleep_for(std::chrono::seconds(1));
  EXPECT_FALSE(preempt.load());
}

// 条件初始 false → 协程挂起 → 主线程 notify → 协程在相同分片恢复。
TEST_F(EventCountTest, AwaitWaitAndNotifySingleWaiter) {
  std::atomic<bool> flag{false};
  std::atomic<bool> preempt{false};
  std::atomic<bool> started{false};
  uint32_t before_sid = 0;

  shard_set->Add(0, [&flag, &preempt, &started, &before_sid, this]() {
    auto entry = [&]() -> cppcoro::AsyncTask {
      auto coro = [&]() -> cppcoro::task<void> {
        before_sid = EngineShard::tlocal()->shard_id();
        bool result = co_await ec_.await(
            [&] { return flag.load(std::memory_order_acquire); });
        uint32_t after_sid = EngineShard::tlocal()->shard_id();
        EXPECT_EQ(before_sid, after_sid);
        preempt.store(result);
      };
      co_await coro();
    };
    entry();
    started.store(true);
  });

  WaitUntil([&] { return started.load(); });
  EXPECT_FALSE(preempt.load());

  flag.store(true, std::memory_order_release);
  ec_.notify();

  WaitUntil([&] { return preempt.load(); });
  EXPECT_TRUE(preempt.load());
}

// 多个分片的协程 wait，notifyAll 全部唤醒，每个分片断言 pid 一致。
TEST_F(EventCountTest, AwaitNotifyAllMultipleWaiters) {
  constexpr int kWaiters = kNumShards;
  std::atomic<bool> flag{false};
  std::atomic<int> woken{0};
  std::atomic<int> started{0};
  std::vector<uint32_t> before_sids(kWaiters);

  for (int i = 0; i < kWaiters; ++i) {
    shard_set->Add(i, [&flag, &woken, &started, &before_sids, this, i]() {
      auto entry = [&]() -> cppcoro::AsyncTask {
        auto coro = [&]() -> cppcoro::task<void> {
          before_sids[i] = EngineShard::tlocal()->shard_id();
          co_await ec_.await(
              [&] { return flag.load(std::memory_order_acquire); });
          uint32_t after_sid = EngineShard::tlocal()->shard_id();
          EXPECT_EQ(before_sids[i], after_sid);
          woken.fetch_add(1);
        };
        co_await coro();
      };
      entry();
      started.fetch_add(1);
    });
  }

  WaitUntil([&] { return started.load() >= kWaiters; });
  EXPECT_EQ(woken.load(), 0);

  flag.store(true, std::memory_order_release);
  ec_.notifyAll();

  WaitUntil([&] { return woken.load() >= kWaiters; });
  EXPECT_EQ(woken.load(), kWaiters);
}

// ============================================================================
// BlockingCounter tests - uses EngineShardSet + EventLoopThreadPool
// ============================================================================
class BlockingCounterTest : public ::testing::Test {
 protected:
  static constexpr int kNumShards = 4;

  void SetUp() override {
    pool_.run();
    shard_set = new EngineShardSet(&pool_);
    shard_set->Init(pool_.size());
  }

  void TearDown() override {
    pool_.stop();
    sleep(1);
    delete shard_set;
  }

  template <typename Pred>
  void WaitUntil(Pred&& pred, std::chrono::milliseconds timeout =
                                  std::chrono::milliseconds(5000)) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
      if (std::chrono::steady_clock::now() >= deadline) {
        FAIL() << "Timed out waiting for condition";
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  yy::net::EventLoopThreadPool pool_{kNumShards};
  EmbeddedBlockingCounter counter_;
};

// Dec 到 0 唤醒单个 waiter，记录 wait 前 pid 并断言恢复在同一线程。
TEST_F(BlockingCounterTest, DecToZeroWakesWaiter) {
  counter_.Start(1);
  std::atomic<bool> completed{false};
  std::atomic<bool> cancelled{false};
  std::atomic<bool> started{false};
  uint32_t before_sid = 0;

  shard_set->Add(0, [&completed, &cancelled, &started, &before_sid, this]() {
    auto entry = [&]() -> cppcoro::AsyncTask {
      auto coro = [&]() -> cppcoro::task<void> {
        before_sid = EngineShard::tlocal()->shard_id();
        bool result = co_await counter_.Wait();
        uint32_t after_sid = EngineShard::tlocal()->shard_id();
        EXPECT_EQ(before_sid, after_sid);
        completed.store(true);
        cancelled.store(!result);
      };
      co_await coro();
    };
    entry();
    started.store(true);
  });

  WaitUntil([&] { return started.load(); });
  EXPECT_FALSE(completed.load());

  counter_.Dec();

  WaitUntil([&] { return completed.load(); });
  EXPECT_TRUE(completed.load());
  EXPECT_FALSE(cancelled.load());
}

// 多个分片的 waiter 等待，Dec 到 0 全部唤醒，断言 pid 一致。
TEST_F(BlockingCounterTest, DecToZeroWakesMultipleWaiters) {
  constexpr int kWaiters = kNumShards;
  counter_.Start(1);
  std::atomic<int> woken{0};
  std::atomic<int> started{0};
  std::vector<uint32_t> before_sids(kWaiters);

  for (int i = 0; i < kWaiters; ++i) {
    shard_set->Add(i, [&woken, &started, &before_sids, this, i]() {
      auto entry = [&]() -> cppcoro::AsyncTask {
        auto coro = [&]() -> cppcoro::task<void> {
          before_sids[i] = EngineShard::tlocal()->shard_id();
          co_await counter_.Wait();
          uint32_t after_sid = EngineShard::tlocal()->shard_id();
          EXPECT_EQ(before_sids[i], after_sid);
          woken.fetch_add(1);
        };
        co_await coro();
      };
      entry();
      started.fetch_add(1);
    });
  }

  WaitUntil([&] { return started.load() >= kWaiters; });
  EXPECT_EQ(woken.load(), 0);

  counter_.Dec();

  WaitUntil([&] { return woken.load() >= kWaiters; });
  EXPECT_EQ(woken.load(), kWaiters);
}

// 多次 Dec 直到 count 归零才唤醒 waiter，断言 pid 一致。
TEST_F(BlockingCounterTest, MultipleDecReachesZero) {
  counter_.Start(3);
  std::atomic<bool> completed{false};
  std::atomic<bool> started{false};
  uint32_t before_sid = 0;

  shard_set->Add(0, [&completed, &started, &before_sid, this]() {
    auto entry = [&]() -> cppcoro::AsyncTask {
      auto coro = [&]() -> cppcoro::task<void> {
        before_sid = EngineShard::tlocal()->shard_id();
        bool result = co_await counter_.Wait();
        uint32_t after_sid = EngineShard::tlocal()->shard_id();
        EXPECT_EQ(before_sid, after_sid);
        completed.store(result);
      };
      co_await coro();
    };
    entry();
    started.store(true);
  });

  WaitUntil([&] { return started.load(); });

  counter_.Dec();  // count = 2
  EXPECT_EQ(counter_.GetCount(), 2);
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  EXPECT_FALSE(completed.load());

  counter_.Dec();  // count = 1
  EXPECT_EQ(counter_.GetCount(), 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  EXPECT_FALSE(completed.load());

  counter_.Dec();  // count = 0 -> wakes
  EXPECT_EQ(counter_.GetCount(), 0);
  WaitUntil([&] { return completed.load(); });
  EXPECT_TRUE(completed.load());
}

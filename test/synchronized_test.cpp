#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "cppcoro/async_task.hpp"
#include "cppcoro/task.hpp"
#include "util/synchronization.hpp"
using namespace util;

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

class EventCountTest : public ::testing::Test {
 protected:
  EventCount ec_;

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
};

TEST_F(EventCountTest, CoroutineWaitEpochChangedNoSuspend) {
  std::atomic<bool> didSuspend{true};

  auto entry = [&]() -> cppcoro::AsyncTask {
    auto waiter = [&]() -> cppcoro::task<void> {
      auto key = ec_.prepareWait();
      ec_.notify();
      bool suspended = co_await ec_.wait(key.epoch());
      didSuspend.store(suspended);
      co_return;
    };
    co_await waiter();
    EXPECT_FALSE(didSuspend.load());
    co_return;
  };
  entry();
}

TEST_F(EventCountTest, CoroutineNotifyAllWakesMultipleThreads) {
  constexpr int kWaiters = 4;
  std::atomic<int> woken{0};
  std::atomic<int> readyCount{0};

  auto entry = [&]() -> cppcoro::AsyncTask {
    auto waiter = [&]() -> cppcoro::task<void> {
      auto key = ec_.prepareWait();
      co_await ec_.wait(key.epoch());
      woken.fetch_add(1);
      co_return;
    };
    co_await waiter();
    co_return;
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < kWaiters; ++i) {
    threads.emplace_back([&]() {
      entry();
      readyCount.fetch_add(1);
    });
    threads.back().join();
  }
  WaitUntil([&] { return readyCount.load() >= kWaiters; });
  EXPECT_EQ(woken.load(), 0);
  EXPECT_TRUE(ec_.notifyAll());
  EXPECT_EQ(woken.load(), kWaiters);
}

TEST_F(EventCountTest, AwaitFastPathConditionAlreadyTrue) {
  std::atomic<bool> flag{true};
  std::atomic<bool> preempt{true};

  auto entry = [&]() -> cppcoro::AsyncTask {
    auto coro = [&]() -> cppcoro::task<void> {
      bool result = co_await ec_.await(
          [&] { return flag.load(std::memory_order_acquire); });
      preempt.store(result);
      co_return;
    };
    co_await coro();
    co_return;
  };
  entry();

  EXPECT_FALSE(preempt.load());
}

TEST_F(EventCountTest, AwaitWaitAndNotifySingleWaiter) {
  std::atomic<bool> flag{false};
  std::atomic<bool> preempt{false};
  std::atomic<bool> started{false};

  auto entry = [&]() -> cppcoro::AsyncTask {
    auto coro = [&]() -> cppcoro::task<void> {
      bool result = co_await ec_.await(
          [&] { return flag.load(std::memory_order_acquire); });
      preempt.store(result);
      co_return;
    };
    co_await coro();
    co_return;
  };

  std::thread t([&]() {
    entry();
    started.store(true);
  });

  WaitUntil([&] { return started.load(); });
  EXPECT_FALSE(preempt.load());

  flag.store(true, std::memory_order_release);
  ec_.notify();

  t.join();
  EXPECT_TRUE(preempt.load());
}

TEST_F(EventCountTest, AwaitNotifyAllMultipleWaiters) {
  constexpr int kWaiters = 4;
  std::atomic<bool> flag{false};
  std::atomic<int> woken{0};
  std::atomic<int> started{0};

  auto entry = [&]() -> cppcoro::AsyncTask {
    auto coro = [&]() -> cppcoro::task<void> {
      co_await ec_.await([&] { return flag.load(std::memory_order_acquire); });
      woken.fetch_add(1);
      co_return;
    };
    co_await coro();
    co_return;
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < kWaiters; ++i) {
    threads.emplace_back([&]() {
      entry();
      started.fetch_add(1);
    });
  }

  WaitUntil([&] { return started.load() >= kWaiters; });
  EXPECT_EQ(woken.load(), 0);

  flag.store(true, std::memory_order_release);
  ec_.notifyAll();

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(woken.load(), kWaiters);
}

class BlockingCounterTest : public ::testing::Test {
 protected:
  EmbeddedBlockingCounter counter_;

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
};

TEST_F(BlockingCounterTest, DecToZeroWakesWaiter) {
  counter_.Start(1);
  std::atomic<bool> completed{false};
  std::atomic<bool> cancelled{false};

  auto entry = [&]() -> cppcoro::AsyncTask {
    auto coro = [&]() -> cppcoro::task<void> {
      bool result = co_await counter_.Wait();
      completed.store(true);
      cancelled.store(!result);
      co_return;
    };
    co_await coro();
    co_return;
  };

  std::thread t([&]() { entry(); });
  WaitUntil([&] { return counter_.IsCompleted() == false; });
  EXPECT_FALSE(completed.load());

  counter_.Dec();

  t.join();
  EXPECT_TRUE(completed.load());
  EXPECT_FALSE(cancelled.load());
}

TEST_F(BlockingCounterTest, DecToZeroWakesMultipleWaiters) {
  constexpr int kWaiters = 4;
  counter_.Start(1);
  std::atomic<int> woken{0};

  auto entry = [&]() -> cppcoro::AsyncTask {
    auto coro = [&]() -> cppcoro::task<void> {
      co_await counter_.Wait();
      woken.fetch_add(1);
      co_return;
    };
    co_await coro();
    co_return;
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < kWaiters; ++i) threads.emplace_back([&]() { entry(); });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_EQ(woken.load(), 0);

  counter_.Dec();

  for (auto& t : threads) t.join();
  EXPECT_EQ(woken.load(), kWaiters);
}

TEST_F(BlockingCounterTest, MultipleDecReachesZero) {
  counter_.Start(3);
  std::atomic<bool> completed{false};

  auto entry = [&]() -> cppcoro::AsyncTask {
    auto coro = [&]() -> cppcoro::task<void> {
      bool result = co_await counter_.Wait();
      completed.store(result);
      co_return;
    };
    co_await coro();
    co_return;
  };

  std::thread t([&]() { entry(); });

  counter_.Dec();  // count = 2
  EXPECT_EQ(counter_.GetCount(), 2);
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  EXPECT_FALSE(completed.load());

  counter_.Dec();  // count = 1
  EXPECT_EQ(counter_.GetCount(), 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  EXPECT_FALSE(completed.load());

  counter_.Dec();  // count = 0 → wakes
  EXPECT_EQ(counter_.GetCount(), 0);
  t.join();
  EXPECT_TRUE(completed.load());
}

TEST_F(BlockingCounterTest, ReWaitAfterNotifyAllNoDeadlock) {
  counter_.Start(1);
  std::atomic<int> woken{0};

  auto entry = [&]() -> cppcoro::AsyncTask {
    auto coro = [&]() -> cppcoro::task<void> {
      co_await counter_.Wait();
      counter_.Start(1);
      co_await counter_.Wait();

      co_return;
    };
    co_await coro();
    co_return;
  };

  std::thread t1([&]() {
    entry();
    woken.fetch_add(1);
  });
  WaitUntil([&] { return woken.load() >= 1; });

  counter_.Dec();

  counter_.Dec();

  t1.join();
}
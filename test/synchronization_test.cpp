#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <chrono>
#include <future>
#include "net/util/synchronization.hpp"
#include "cppcoro/task.hpp"

namespace util {

namespace {

template<typename T>
struct SyncRunner {
  static T run(cppcoro::task<T>&& task) {
    T result{};
    bool done = false;
    
    auto runner = [&]() -> cppcoro::task<void> {
      result = co_await std::move(task);
      done = true;
    }();
    
    while (!done) {
      std::this_thread::yield();
    }
    
    return result;
  }
};

template<>
struct SyncRunner<void> {
  static void run(cppcoro::task<void>&& task) {
    bool done = false;
    
    auto runner = [&]() -> cppcoro::task<void> {
      co_await std::move(task);
      done = true;
    }();
    
    while (!done) {
      std::this_thread::yield();
    }
  }
};

} // namespace

class EventCountTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(EventCountTest, PrepareWait) {
  EventCount ec;
  
  auto key = ec.prepareWait();
  EXPECT_EQ(key.epoch(), 0);
}

TEST_F(EventCountTest, NotifyNoWaiter) {
  EventCount ec;
  
  bool result = ec.notify();
  EXPECT_FALSE(result);
  
  result = ec.notifyAll();
  EXPECT_FALSE(result);
}

TEST_F(EventCountTest, FinishWait) {
  EventCount ec;
  
  ec.finishWait();
}

TEST_F(EventCountTest, AwaitFastPath) {
  EventCount ec;
  bool condition = true;
  
  auto task = ec.await([&]() { return condition; });
  bool preempt = SyncRunner<bool>::run(std::move(task));
  EXPECT_FALSE(preempt);
}

TEST_F(EventCountTest, AwaitWithNotify) {
  EventCount ec;
  bool condition = false;
  
  std::thread notifier([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    condition = true;
    ec.notify();
  });
  
  auto task = ec.await([&]() { return condition; });
  SyncRunner<bool>::run(std::move(task));
  
  notifier.join();
}

TEST_F(EventCountTest, AwaitWithNotifyAll) {
  EventCount ec;
  bool condition = false;
  std::atomic<int> counter{0};
  const int kNumThreads = 3;
  
  std::vector<std::thread> threads;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&]() {
      auto task = ec.await([&]() { return condition; });
      bool result = SyncRunner<bool>::run(std::move(task));
      if (result) {
        counter.fetch_add(1);
      }
    });
  }
  
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  condition = true;
  ec.notifyAll();
  
  for (auto& t : threads) {
    t.join();
  }
  
  EXPECT_EQ(counter.load(), kNumThreads);
}

class DoneTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(DoneTest, ResetInitially) {
  Done done;
  
  done.Reset();
}

TEST_F(DoneTest, WaitAfterNotify) {
  Done done;
  
  done.Notify();
  auto task = done.Wait();
  bool result = SyncRunner<bool>::run(std::move(task));
  EXPECT_TRUE(result);
}

TEST_F(DoneTest, WaitWithReset) {
  Done done;
  
  done.Notify();
  auto task1 = done.Wait(Done::AND_RESET);
  bool first_wait = SyncRunner<bool>::run(std::move(task1));
  EXPECT_TRUE(first_wait);
  
  bool waiter_done = false;
  std::thread waiter([&]() {
    auto task = done.Wait();
    SyncRunner<bool>::run(std::move(task));
    waiter_done = true;
  });
  
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  done.Notify();
  waiter.join();
  
  EXPECT_TRUE(waiter_done);
}

class ThreadEventTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(ThreadEventTest, BasicNotify) {
  ThreadEvent te;
  bool waited = false;
  
  auto waiter = std::thread([&]() {
    te.wait();
    waited = true;
  });
  
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  te.notify();
  waiter.join();
  
  EXPECT_TRUE(waited);
}

TEST_F(ThreadEventTest, NotifyAll) {
  ThreadEvent te;
  std::atomic<int> counter{0};
  const int kNumThreads = 5;
  
  std::vector<std::thread> threads;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&]() {
      te.wait();
      counter.fetch_add(1);
    });
  }
  
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  te.notifyAll();
  
  for (auto& t : threads) {
    t.join();
  }
  
  EXPECT_EQ(counter.load(), kNumThreads);
}

TEST_F(ThreadEventTest, Reset) {
  ThreadEvent te;
  
  te.notify();
  te.reset();
  
  bool waiter_done = false;
  auto waiter = std::thread([&]() {
    te.wait();
    waiter_done = true;
  });
  
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  te.notify();
  waiter.join();
  
  EXPECT_TRUE(waiter_done);
}

TEST_F(ThreadEventTest, MultipleNotify) {
  ThreadEvent te;
  std::atomic<int> counter{0};
  
  auto waiter1 = std::thread([&]() {
    te.wait();
    counter.fetch_add(1);
  });
  
  auto waiter2 = std::thread([&]() {
    te.wait();
    counter.fetch_add(1);
  });
  
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  te.notify();
  te.notify();
  
  waiter1.join();
  waiter2.join();
  
  EXPECT_EQ(counter.load(), 2);
}

class BlockingCounterTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(BlockingCounterTest, IsCompletedInitiallyZero) {
  BlockingCounter bc(0);
  
  EXPECT_TRUE(bc->IsCompleted());
}

TEST_F(BlockingCounterTest, IsCompletedNonZero) {
  BlockingCounter bc(5);
  
  EXPECT_FALSE(bc->IsCompleted());
}

TEST_F(BlockingCounterTest, Start) {
  BlockingCounter bc(0);
  
  EXPECT_TRUE(bc->IsCompleted());
  
  bc->Start(3);
  EXPECT_FALSE(bc->IsCompleted());
}

TEST_F(BlockingCounterTest, Add) {
  BlockingCounter bc(1);
  
  bc->Add(2);
  
  EXPECT_FALSE(bc->IsCompleted());
}

TEST_F(BlockingCounterTest, WaitWithDec) {
  BlockingCounter bc(3);
  
  std::atomic<int> counter{0};
  std::thread waiter([&]() {
    auto task = bc->Wait();
    bool result = SyncRunner<bool>::run(std::move(task));
    counter.fetch_add(result ? 1 : 0);
  });
  
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  bc->Dec();
  bc->Dec();
  bc->Dec();
  
  waiter.join();
  EXPECT_EQ(counter.load(), 1);
}

TEST_F(BlockingCounterTest, WaitWithCancel) {
  BlockingCounter bc(10);
  
  bool was_canceled = false;
  std::thread waiter([&]() {
    auto task = bc->Wait();
    bool result = SyncRunner<bool>::run(std::move(task));
    was_canceled = !result;
  });
  
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  bc->Cancel();
  
  waiter.join();
  EXPECT_TRUE(was_canceled);
}

}  // namespace util

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
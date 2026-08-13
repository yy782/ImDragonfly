// TaskQueue / mpmc_queue 单元测试。
//
// 高并发压力测试的任务总数默认 2000，可通过环境变量覆盖：
//     TASK_QUEUE_NUM_TASKS=10000 ./build/unit_tests \
//         --gtest_filter='*TaskQueue*Concurrent*:*MpmcQueue*MultiThread*'

#include "util/task_queue.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <memory_resource>
#include <mutex>
#include <new>
#include <thread>
#include <vector>

#include "util/mpmc_queue.hpp"

namespace {

// 任务总数：优先取环境变量 TASK_QUEUE_NUM_TASKS，默认 2000（不硬编码）。
size_t TaskCountFromEnv() {
  const char* env = std::getenv("TASK_QUEUE_NUM_TASKS");
  if (env == nullptr) {
    return 2000;
  }
  char* end = nullptr;
  const unsigned long long n = std::strtoull(env, &end, 10);
  return n > 0 ? static_cast<size_t>(n) : 2000;
}

// 统计分配/释放次数的 PMR memory_resource，用于验证自定义分配器被正确使用。
class CountingResource : public std::pmr::memory_resource {
 public:
  std::size_t alloc_count = 0;
  std::size_t dealloc_count = 0;
  std::size_t alloc_bytes = 0;
  std::size_t dealloc_bytes = 0;

 protected:
  void* do_allocate(std::size_t bytes, std::size_t alignment) override {
    alloc_count++;
    alloc_bytes += bytes;
    return ::operator new(bytes, std::align_val_t(alignment));
  }
  void do_deallocate(void* p, std::size_t bytes,
                     std::size_t alignment) override {
    dealloc_count++;
    dealloc_bytes += bytes;
    ::operator delete(p, std::align_val_t(alignment));
  }
  bool do_is_equal(
      const std::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }
};

}  // namespace

// ═══════════════════════════════════════════════════════════
// TaskQueue 测试
// ═══════════════════════════════════════════════════════════

TEST(TaskQueueTest, BasicFifo) {
  util::TaskQueue q(8);
  std::vector<int> order;
  std::mutex mu;
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(q.TryAdd([&, i]() {
      std::lock_guard<std::mutex> lk(mu);
      order.push_back(i);
    }));
  }
  EXPECT_FALSE(q.Empty());
  q.TryDrain();
  EXPECT_TRUE(q.Empty());
  ASSERT_EQ(order.size(), 5u);
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(order[i], i) << "单生产者/单消费者应保持 FIFO 顺序";
  }
}

TEST(TaskQueueTest, Capacity) {
  util::TaskQueue q(4);  // 容量 4（2 的幂）
  for (int i = 0; i < 4; ++i) {
    EXPECT_TRUE(q.TryAdd([] {}));
  }
  EXPECT_FALSE(q.TryAdd([] {})) << "队列满时 TryAdd 应失败";
  q.TryDrain();
  EXPECT_TRUE(q.Empty());
  EXPECT_TRUE(q.TryAdd([] {})) << "排空后应可继续投递";
  q.TryDrain();
  EXPECT_TRUE(q.Empty());
}

// 核心压力测试：10 个线程投递，1 个线程 TryDrain 消费。
// 校验：所有任务恰好执行一次（无丢失、无重复）。
TEST(TaskQueueTest, ConcurrentDrain) {
  const size_t kNumTasks = TaskCountFromEnv();
  constexpr size_t kProducers = 10;
  util::TaskQueue q(64);  // 小容量制造更多入队竞争

  std::vector<std::atomic<uint8_t>> seen(kNumTasks);
  for (auto& s : seen) s.store(0, std::memory_order_relaxed);
  std::atomic<size_t> done{0};

  auto producer = [&](size_t begin, size_t count) {
    for (size_t i = begin; i < begin + count; ++i) {
      auto task = [&, i]() {
        seen[i].store(1, std::memory_order_relaxed);
        done.fetch_add(1, std::memory_order_relaxed);
      };
      while (!q.TryAdd(std::move(task))) {
        std::this_thread::yield();  // 队列满则重试，保证全部投递
      }
    }
  };

  const size_t per = kNumTasks / kProducers;
  const size_t rem = kNumTasks % kProducers;
  std::vector<std::thread> threads;
  threads.reserve(kProducers);
  size_t begin = 0;
  for (size_t p = 0; p < kProducers; ++p) {
    const size_t count = per + (p < rem ? 1 : 0);
    threads.emplace_back(producer, begin, count);
    begin += count;
  }

  std::thread consumer([&]() {
    while (done.load(std::memory_order_relaxed) < kNumTasks) {
      q.TryDrain();
      std::this_thread::yield();
    }
    q.TryDrain();  // 全部完成后最后清空一轮
  });

  for (auto& t : threads) t.join();
  consumer.join();

  EXPECT_EQ(done.load(), kNumTasks);
  for (size_t i = 0; i < kNumTasks; ++i) {
    EXPECT_EQ(seen[i].load(std::memory_order_relaxed), 1u)
        << "任务 " << i << " 未被执行";
  }
}

// 自定义 PMR 分配器：分配必须来自传入的 resource，且分配/释放对称。
TEST(TaskQueueTest, CustomAllocator) {
  CountingResource res;
  {
    util::TaskQueue q(8, &res);
    ASSERT_TRUE(q.TryAdd([] {}));
    ASSERT_TRUE(q.TryAdd([] {}));
    q.TryDrain();
    EXPECT_GT(res.alloc_count, 0u) << "队列 buffer 应通过自定义分配器分配";
  }
  EXPECT_EQ(res.alloc_count, res.dealloc_count);
  EXPECT_EQ(res.alloc_bytes, res.dealloc_bytes);
}

// ═══════════════════════════════════════════════════════════
// mpmc_queue 直接测试
// ═══════════════════════════════════════════════════════════

TEST(MpmcQueueTest, SingleThreadBasic) {
  util::mpmc_queue<int> q(4);
  EXPECT_EQ(q.capacity(), 4u);
  EXPECT_TRUE(q.empty());
  EXPECT_FALSE(q.is_full());

  EXPECT_TRUE(q.try_enqueue(1));
  EXPECT_TRUE(q.try_enqueue(2));
  EXPECT_FALSE(q.empty());
  int v = 0;
  EXPECT_TRUE(q.try_dequeue(v));
  EXPECT_EQ(v, 1);
  EXPECT_TRUE(q.try_dequeue(v));
  EXPECT_EQ(v, 2);
  EXPECT_FALSE(q.try_dequeue(v));
  EXPECT_TRUE(q.empty());

  // 环形复用：出队后继续入队，跨越多个"环"仍正确
  for (int i = 0; i < 100; ++i) {
    EXPECT_TRUE(q.try_enqueue(i));
    EXPECT_TRUE(q.try_dequeue(v));
    EXPECT_EQ(v, i);
  }
}

// mpmc_queue 多生产者/多消费者正确性：每个值恰好被消费一次。
TEST(MpmcQueueTest, MultiThreadSum) {
  const size_t kNumTasks = TaskCountFromEnv();
  constexpr size_t kProducers = 4;
  constexpr size_t kConsumers = 4;
  util::mpmc_queue<uint64_t> q(64);

  std::vector<std::atomic<uint8_t>> seen(kNumTasks);
  for (auto& s : seen) s.store(0, std::memory_order_relaxed);
  std::atomic<size_t> consumed{0};

  std::vector<std::thread> prod_threads;
  prod_threads.reserve(kProducers);
  for (size_t p = 0; p < kProducers; ++p) {
    prod_threads.emplace_back([&, p]() {
      for (size_t i = p; i < kNumTasks; i += kProducers) {
        while (!q.try_enqueue(i)) {
          std::this_thread::yield();
        }
      }
    });
  }

  std::vector<std::thread> cons_threads;
  cons_threads.reserve(kConsumers);
  for (size_t c = 0; c < kConsumers; ++c) {
    cons_threads.emplace_back([&]() {
      uint64_t v = 0;
      while (consumed.load(std::memory_order_relaxed) < kNumTasks) {
        if (q.try_dequeue(v)) {
          seen[v].store(1, std::memory_order_relaxed);
          consumed.fetch_add(1, std::memory_order_relaxed);
        } else {
          std::this_thread::yield();
        }
      }
    });
  }

  for (auto& t : prod_threads) t.join();
  for (auto& t : cons_threads) t.join();

  EXPECT_EQ(consumed.load(), kNumTasks);
  for (size_t i = 0; i < kNumTasks; ++i) {
    EXPECT_EQ(seen[i].load(std::memory_order_relaxed), 1u)
        << "值 " << i << " 未被消费";
  }
}

// 非法容量（非 2 的幂）属编程错误：Debug 下 assert 直接终止。
#ifdef NDEBUG
TEST(MpmcQueueTest, InvalidCapacity) {
  GTEST_SKIP() << "assert 在 NDEBUG（Release）下被禁用，跳过死亡测试";
}
#else
TEST(MpmcQueueTest, InvalidCapacity) {
  EXPECT_DEATH(util::mpmc_queue<int> q(3), "capacity must be a power");
  EXPECT_DEATH(util::mpmc_queue<int> q(0), "capacity must be a power");
  EXPECT_DEATH(util::mpmc_queue<int> q(1), "capacity must be a power");
}
#endif

// mpmc_queue 自定义分配器：分配/释放对称且全部来自指定 resource。
TEST(MpmcQueueTest, CustomAllocator) {
  CountingResource res;
  {
    util::mpmc_queue<int> q(16, &res);
    for (int i = 0; i < 16; ++i) {
      EXPECT_TRUE(q.try_enqueue(i));
    }
    EXPECT_FALSE(q.try_enqueue(16)) << "容量 16，第 17 个元素应入队失败";
    int v = 0;
    for (int i = 0; i < 16; ++i) {
      ASSERT_TRUE(q.try_dequeue(v));
      EXPECT_EQ(v, i);
    }
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(res.alloc_count, 1u) << "buffer 应恰好分配一次";
  }
  EXPECT_EQ(res.alloc_count, res.dealloc_count);
  EXPECT_EQ(res.alloc_bytes, res.dealloc_bytes);
}

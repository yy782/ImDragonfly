#include "util/intrusive_ptr.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace util;

namespace {

// 追踪型对象：通过静态计数器验证对象的构造/析构是否精确发生一次。
class Tracked : public intrusive_ref_counter<Tracked> {
 public:
  static int alive;
  static int constructed;
  static int destructed;

  Tracked() {
    ++alive;
    ++constructed;
  }
  ~Tracked() {
    --alive;
    ++destructed;
  }
};

int Tracked::alive = 0;
int Tracked::constructed = 0;
int Tracked::destructed = 0;

// 线程安全计数器版本，用于并发场景下的释放测试。
class TrackedThreadSafe
    : public intrusive_ref_counter<TrackedThreadSafe, thread_safe_counter> {
 public:
  static std::atomic<int> alive;
  static std::atomic<int> constructed;
  static std::atomic<int> destructed;

  TrackedThreadSafe() {
    ++alive;
    ++constructed;
  }
  ~TrackedThreadSafe() {
    --alive;
    ++destructed;
  }
};

std::atomic<int> TrackedThreadSafe::alive{0};
std::atomic<int> TrackedThreadSafe::constructed{0};
std::atomic<int> TrackedThreadSafe::destructed{0};

class IntrusivePtrTest : public ::testing::Test {
 protected:
  void SetUp() override {
    Tracked::alive = 0;
    Tracked::constructed = 0;
    Tracked::destructed = 0;
  }
  void TearDown() override {
    EXPECT_EQ(Tracked::alive, 0);
    EXPECT_EQ(Tracked::constructed, Tracked::destructed);
  }
};

TEST_F(IntrusivePtrTest, SingleOwnerDeletesOnScopeExit) {
  {
    intrusive_ptr<Tracked> p(new Tracked());
    EXPECT_EQ(Tracked::alive, 1);
    EXPECT_EQ(Tracked::destructed, 0);
    EXPECT_EQ(p->use_count(), 1u);
  }
  // 出作用域后应立即释放。
  EXPECT_EQ(Tracked::alive, 0);
  EXPECT_EQ(Tracked::destructed, 1);
}

TEST_F(IntrusivePtrTest, CopyIncrementsCountAndDefersDelete) {
  intrusive_ptr<Tracked> p(new Tracked());
  EXPECT_EQ(Tracked::alive, 1);

  {
    intrusive_ptr<Tracked> q(p);  // 拷贝，计数 +1。
    EXPECT_EQ(p->use_count(), 2u);
    EXPECT_EQ(q->use_count(), 2u);
    EXPECT_EQ(Tracked::alive, 1);
    // q 出作用域，计数回到 1，对象仍存活。
  }
  EXPECT_EQ(Tracked::alive, 1);
  EXPECT_EQ(p->use_count(), 1u);
  EXPECT_EQ(Tracked::destructed, 0);
}

TEST_F(IntrusivePtrTest, MoveTransfersOwnershipWithoutCountChange) {
  intrusive_ptr<Tracked> p(new Tracked());
  EXPECT_EQ(p->use_count(), 1u);

  intrusive_ptr<Tracked> q(std::move(p));
  EXPECT_EQ(p.get(), nullptr);
  EXPECT_FALSE(p);
  EXPECT_EQ(q->use_count(), 1u);  // 移动不增减计数。
  EXPECT_EQ(Tracked::alive, 1);

  q.reset();
  EXPECT_EQ(Tracked::alive, 0);
  EXPECT_EQ(Tracked::destructed, 1);
}

TEST_F(IntrusivePtrTest, ResetReleasesReference) {
  intrusive_ptr<Tracked> p(new Tracked());
  intrusive_ptr<Tracked> q(p);
  EXPECT_EQ(p->use_count(), 2u);

  p.reset();
  EXPECT_FALSE(p);
  EXPECT_EQ(Tracked::alive, 1);
  EXPECT_EQ(q->use_count(), 1u);

  q.reset();
  EXPECT_EQ(Tracked::alive, 0);
}

TEST_F(IntrusivePtrTest, ResetWithNewPointerReleasesOld) {
  intrusive_ptr<Tracked> p(new Tracked());
  Tracked* old = p.get();

  p.reset(new Tracked());  // 释放旧对象，接管新对象。
  EXPECT_EQ(Tracked::alive, 1);
  EXPECT_EQ(Tracked::destructed, 1);
  EXPECT_NE(p.get(), old);
  EXPECT_EQ(p->use_count(), 1u);
}

TEST_F(IntrusivePtrTest, CopyAssignmentIsCopyAndSwap) {
  intrusive_ptr<Tracked> p(new Tracked());
  intrusive_ptr<Tracked> q;
  q = p;
  EXPECT_EQ(p->use_count(), 2u);
  EXPECT_EQ(q->use_count(), 2u);
  EXPECT_EQ(p.get(), q.get());

  q = intrusive_ptr<Tracked>(new Tracked());  // 移动赋值，q 换绑。
  EXPECT_EQ(p->use_count(), 1u);
  EXPECT_NE(p.get(), q.get());
}

TEST_F(IntrusivePtrTest, MoveAssignmentReleasesOldOwner) {
  intrusive_ptr<Tracked> p(new Tracked());
  intrusive_ptr<Tracked> q(new Tracked());
  EXPECT_EQ(Tracked::alive, 2);

  q = std::move(p);  // 释放 q 原有对象。
  EXPECT_FALSE(p);
  EXPECT_EQ(Tracked::alive, 1);
  EXPECT_EQ(Tracked::destructed, 1);
  EXPECT_EQ(q->use_count(), 1u);
}

TEST_F(IntrusivePtrTest, IntrusivePtrFromThisAddsRef) {
  intrusive_ptr<Tracked> p(new Tracked());
  EXPECT_EQ(p->use_count(), 1u);

  intrusive_ptr<Tracked> q = p->intrusive_ptr_from_this();
  EXPECT_EQ(p->use_count(), 2u);
  EXPECT_EQ(p.get(), q.get());

  q.reset();
  EXPECT_EQ(Tracked::alive, 1);
  EXPECT_EQ(p->use_count(), 1u);
}

TEST_F(IntrusivePtrTest, ConstructorAddRefFalseTakesOverExistingReference) {
  Tracked* raw = new Tracked();
  intrusive_ptr_add_ref(raw);  // 模拟外部已持有引用：计数 0 -> 1。
  EXPECT_EQ(raw->use_count(), 1u);

  {
    intrusive_ptr<Tracked> p(raw, /*add_ref=*/false);  // 接管已有引用。
    EXPECT_EQ(raw->use_count(), 1u);                   // 不额外增加。
    EXPECT_EQ(Tracked::alive, 1);
  }
  // p 出作用域，计数 1 -> 0，正确释放一次。
  EXPECT_EQ(Tracked::alive, 0);
  EXPECT_EQ(Tracked::destructed, 1);
}

TEST_F(IntrusivePtrTest, NullPointerSafe) {
  intrusive_ptr<Tracked> p;
  EXPECT_FALSE(p);
  EXPECT_EQ(p.get(), nullptr);

  p.reset();
  p.reset(nullptr);
  intrusive_ptr<Tracked> q(nullptr);
  EXPECT_FALSE(q);

  // 对空指针的拷贝/赋值/移动不应崩溃。
  intrusive_ptr<Tracked> r(p);
  r = p;
  r = std::move(q);
  EXPECT_EQ(Tracked::alive, 0);
  EXPECT_EQ(Tracked::destructed, 0);
}

TEST_F(IntrusivePtrTest, SwapSwapsPointers) {
  intrusive_ptr<Tracked> p(new Tracked());
  intrusive_ptr<Tracked> q(new Tracked());
  Tracked* pa = p.get();
  Tracked* qa = q.get();

  p.swap(q);
  EXPECT_EQ(p.get(), qa);
  EXPECT_EQ(q.get(), pa);
  EXPECT_EQ(Tracked::alive, 2);
}

TEST_F(IntrusivePtrTest, DerivedToBaseConversionSharesCount) {
  struct Base : intrusive_ref_counter<Base> {
    virtual ~Base() = default;
  };
  struct Derived : Base {};

  intrusive_ptr<Derived> d(new Derived());
  EXPECT_EQ(d->use_count(), 1u);

  intrusive_ptr<Base> b(d);  // Derived* -> Base*，共享同一计数。
  EXPECT_EQ(d.get(), b.get());
  EXPECT_EQ(d->use_count(), 2u);

  d.reset();
  EXPECT_EQ(b->use_count(), 1u);

  b.reset();
  SUCCEED();
}

TEST(IntrusivePtrThreadSafeTest, ConcurrentReleaseDeletesExactlyOnce) {
  TrackedThreadSafe::alive = 0;
  TrackedThreadSafe::constructed = 0;
  TrackedThreadSafe::destructed = 0;

  intrusive_ptr<TrackedThreadSafe> p(new TrackedThreadSafe());
  constexpr int kCopies = 100;
  std::vector<intrusive_ptr<TrackedThreadSafe>> copies(kCopies, p);

  EXPECT_EQ(p->use_count(), static_cast<unsigned>(kCopies + 1));

  std::atomic<bool> start{false};
  std::vector<std::thread> threads;
  for (int i = 0; i < kCopies; ++i) {
    threads.emplace_back([&, i]() {
      while (!start.load(std::memory_order_acquire)) {
      }
      copies[i].reset();
    });
  }

  start.store(true, std::memory_order_release);
  p.reset();  // 同时释放最后一个根引用。
  for (auto& t : threads) t.join();

  EXPECT_EQ(TrackedThreadSafe::alive.load(), 0);
  EXPECT_EQ(TrackedThreadSafe::destructed.load(), 1);
  EXPECT_EQ(TrackedThreadSafe::constructed.load(), 1);
}

}  // namespace

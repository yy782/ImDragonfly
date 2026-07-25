#include "src/sharding/db_slice.hpp"
#include "src/sharding/engine_shard.hpp"
#include <gtest/gtest.h>

using namespace dfly;
using namespace yy::net;
class IntentLockTest : public ::testing::Test {
 protected:
  void SetUp() override {
    EngineShard::InitThreadLocal(&loop_);
    slice_ = std::make_unique<DbSlice>(0, false, EngineShard::tlocal());
  }

  void TearDown() override { slice_.reset(); }

  // 验证：持有方的锁释放后，另一方可以获取同样的锁
  void ExpectReleaseThenReacquire(IntentLock::Mode mode, const KeyLockArgs& args) {
    EXPECT_TRUE(slice_->Acquire(mode, args));
    slice_->Release(mode, args);
    EXPECT_TRUE(slice_->Acquire(mode, args));
    slice_->Release(mode, args);
  }
  EventLoop loop_;
  std::unique_ptr<DbSlice> slice_;
};

// 测试1: 多 fp 锁获取 → 释放 → 再加锁 (读写锁都需要测试)
TEST_F(IntentLockTest, AcquireReleaseReacquire) {
  // 每轮都用多 fp 的 KeyLockArgs，覆盖各种组合
  KeyLockArgs args_ab;
  args_ab.db_index = 0;
  args_ab.fps = {1, 2};

  KeyLockArgs args_abc;
  args_abc.db_index = 0;
  args_abc.fps = {10, 20, 30};

  KeyLockArgs args_abcd;
  args_abcd.db_index = 0;
  args_abcd.fps = {100, 200, 300, 400};

  // 共享锁 — 2/3/4 个 fp 各测一轮
  ExpectReleaseThenReacquire(IntentLock::SHARED, args_ab);
  ExpectReleaseThenReacquire(IntentLock::SHARED, args_abc);
  ExpectReleaseThenReacquire(IntentLock::SHARED, args_abcd);

  // 排他锁 — 2/3/4 个 fp 各测一轮
  ExpectReleaseThenReacquire(IntentLock::EXCLUSIVE, args_ab);
  ExpectReleaseThenReacquire(IntentLock::EXCLUSIVE, args_abc);
  ExpectReleaseThenReacquire(IntentLock::EXCLUSIVE, args_abcd);
}

// 测试2: 多 fp 读写互斥
TEST_F(IntentLockTest, SharedExclusiveMutualExclusion) {
  // 2 个 fp
  {
    KeyLockArgs args = {0, {10, 20}};
    EXPECT_TRUE(slice_->Acquire(IntentLock::SHARED, args));
    EXPECT_FALSE(slice_->Acquire(IntentLock::EXCLUSIVE, args));
    slice_->Release(IntentLock::SHARED, args);

    EXPECT_TRUE(slice_->Acquire(IntentLock::EXCLUSIVE, args));
    EXPECT_FALSE(slice_->Acquire(IntentLock::SHARED, args));
    slice_->Release(IntentLock::EXCLUSIVE, args);
  }

  // 3 个 fp
  {
    KeyLockArgs args = {0, {100, 200, 300}};
    EXPECT_TRUE(slice_->Acquire(IntentLock::SHARED, args));
    EXPECT_FALSE(slice_->Acquire(IntentLock::EXCLUSIVE, args));
    slice_->Release(IntentLock::SHARED, args);

    EXPECT_TRUE(slice_->Acquire(IntentLock::EXCLUSIVE, args));
    EXPECT_FALSE(slice_->Acquire(IntentLock::SHARED, args));
    slice_->Release(IntentLock::EXCLUSIVE, args);
  }

  // 4 个 fp
  {
    KeyLockArgs args = {0, {1000, 2000, 3000, 4000}};
    EXPECT_TRUE(slice_->Acquire(IntentLock::SHARED, args));
    EXPECT_FALSE(slice_->Acquire(IntentLock::EXCLUSIVE, args));
    slice_->Release(IntentLock::SHARED, args);

    EXPECT_TRUE(slice_->Acquire(IntentLock::EXCLUSIVE, args));
    EXPECT_FALSE(slice_->Acquire(IntentLock::SHARED, args));
    slice_->Release(IntentLock::EXCLUSIVE, args);
  }
}

// 测试3: 部分锁冲突 —— A 持有部分 key，B 尝试获取有重叠的 key 集合
//         验证冲突时 Acquire 的 all-or-nothing 语义：失败的 Acquire 不泄漏锁
TEST_F(IntentLockTest, PartialLockConflict) {
  // --- 场景1: A:{a,b}排他, B:{b,c}排他 → B失败, c 不能被泄漏 ---
  {
    // A 的 key 集合: {1, 2, 3, 4}   取了 a,b
    // B 的 key 集合: {3, 4, 5, 6} 和 A 有重叠 3,4
    KeyLockArgs A_args = {0, {1, 2, 3, 4}};
    KeyLockArgs B_args = {0, {3, 4, 5, 6}};

    EXPECT_TRUE(slice_->Acquire(IntentLock::EXCLUSIVE, A_args));
    EXPECT_FALSE(slice_->Acquire(IntentLock::EXCLUSIVE, B_args));

    // B 失败后, fp=5,6 不能残留锁: A 可以直接获取它们
    KeyLockArgs test_5 = {0, {5}};
    KeyLockArgs test_6 = {0, {6}};
    EXPECT_TRUE(slice_->Acquire(IntentLock::EXCLUSIVE, test_5));
    EXPECT_TRUE(slice_->Acquire(IntentLock::EXCLUSIVE, test_6));
    slice_->Release(IntentLock::EXCLUSIVE, test_5);
    slice_->Release(IntentLock::EXCLUSIVE, test_6);

    slice_->Release(IntentLock::EXCLUSIVE, A_args);
  }

  // --- 场景2: A:{a,b,c}共享, B:{c,d}排他 → B失败, d 不能被泄漏 ---
  {
    KeyLockArgs A_args = {0, {10, 20, 30}};
    KeyLockArgs B_args = {0, {30, 40}};

    EXPECT_TRUE(slice_->Acquire(IntentLock::SHARED, A_args));
    EXPECT_FALSE(slice_->Acquire(IntentLock::EXCLUSIVE, B_args));

    // B 失败后, fp=40 不能残留: 应可独立获取
    KeyLockArgs test_40 = {0, {40}};
    EXPECT_TRUE(slice_->Acquire(IntentLock::EXCLUSIVE, test_40));
    slice_->Release(IntentLock::EXCLUSIVE, test_40);

    slice_->Release(IntentLock::SHARED, A_args);
  }

  // --- 场景3: A:{a,b}排他, B:{c,d,e}排他 → 完全不重叠, B 应成功 ---
  {
    KeyLockArgs A_args = {0, {100, 200}};
    KeyLockArgs B_args = {0, {300, 400, 500}};

    EXPECT_TRUE(slice_->Acquire(IntentLock::EXCLUSIVE, A_args));
    EXPECT_TRUE(slice_->Acquire(IntentLock::EXCLUSIVE, B_args));

    slice_->Release(IntentLock::EXCLUSIVE, B_args);
    slice_->Release(IntentLock::EXCLUSIVE, A_args);
  }

  // --- 场景4: A:{a,b,c,d}共享, B:{d,e,f}共享 → 全部共享, B 应成功 ---
  {
    KeyLockArgs A_args = {0, {111, 222, 333, 444}};
    KeyLockArgs B_args = {0, {444, 555, 666}};

    EXPECT_TRUE(slice_->Acquire(IntentLock::SHARED, A_args));
    EXPECT_TRUE(slice_->Acquire(IntentLock::SHARED, B_args));

    slice_->Release(IntentLock::SHARED, B_args);
    slice_->Release(IntentLock::SHARED, A_args);
  }

  // --- 场景5: A:{a,b}排他, B:{b,c,d}排他 → B失败, 验证 c,d 都不泄漏 ---
  //          释放A全部后, B 再试应成功
  {
    KeyLockArgs A_args = {0, {777, 888}};
    KeyLockArgs B_args = {0, {888, 999, 1111}};

    EXPECT_TRUE(slice_->Acquire(IntentLock::EXCLUSIVE, A_args));
    EXPECT_FALSE(slice_->Acquire(IntentLock::EXCLUSIVE, B_args));

    // 验证 fp=999,1111 未被泄漏
    KeyLockArgs test_c = {0, {999}};
    KeyLockArgs test_d = {0, {1111}};
    EXPECT_TRUE(slice_->Acquire(IntentLock::SHARED, test_c));
    EXPECT_TRUE(slice_->Acquire(IntentLock::SHARED, test_d));
    slice_->Release(IntentLock::SHARED, test_c);
    slice_->Release(IntentLock::SHARED, test_d);

    // A 释放后, B 再试应成功
    slice_->Release(IntentLock::EXCLUSIVE, A_args);
    EXPECT_TRUE(slice_->Acquire(IntentLock::EXCLUSIVE, B_args));
    slice_->Release(IntentLock::EXCLUSIVE, B_args);
  }

  // --- 场景6: 单 key 冲突导致整个多 key 集合失败, 且冲突 key 锁计数不膨胀 ---
  {
    KeyLockArgs A_args = {0, {11, 22, 33}};

    // A 拿共享锁
    EXPECT_TRUE(slice_->Acquire(IntentLock::SHARED, A_args));

    // B 尝试拿排他锁, 因 11/22/33 均有共享持有者, 应失败
    KeyLockArgs B_args = {0, {11, 44, 55}};
    EXPECT_FALSE(slice_->Acquire(IntentLock::EXCLUSIVE, B_args));

    // 冲突失败后 11 的锁计数不能膨胀: 释放一次共享就能让 11 的排他锁可获取
    slice_->Release(IntentLock::SHARED, A_args);

    // 现在 {11} 的排他锁应可直接获取 (证明 B 失败没有在 11 上残留计数)
    KeyLockArgs test_11 = {0, {11}};
    EXPECT_TRUE(slice_->Acquire(IntentLock::EXCLUSIVE, test_11));
    slice_->Release(IntentLock::EXCLUSIVE, test_11);
  }
}

// 附加: 多个共享锁可以共存 (多 fp)
TEST_F(IntentLockTest, MultipleSharedLocks) {
  KeyLockArgs args = {0, {1, 2, 3}};

  EXPECT_TRUE(slice_->Acquire(IntentLock::SHARED, args));
  EXPECT_TRUE(slice_->Acquire(IntentLock::SHARED, args));
  EXPECT_TRUE(slice_->Acquire(IntentLock::SHARED, args));

  // 共享锁仍在持有，排他锁应失败
  EXPECT_FALSE(slice_->Acquire(IntentLock::EXCLUSIVE, args));

  slice_->Release(IntentLock::SHARED, args);
  slice_->Release(IntentLock::SHARED, args);
  slice_->Release(IntentLock::SHARED, args);
}

// 附加: 不同 fp 集合的锁互不干扰
TEST_F(IntentLockTest, DifferentFpIsolation) {
  KeyLockArgs args_a = {0, {42, 43}};
  KeyLockArgs args_b = {0, {99, 100}};

  // fp={42,43} 加排他锁, 不影响 fp={99,100} 的共享锁
  EXPECT_TRUE(slice_->Acquire(IntentLock::EXCLUSIVE, args_a));
  EXPECT_TRUE(slice_->Acquire(IntentLock::SHARED, args_b));
  EXPECT_TRUE(slice_->Acquire(IntentLock::SHARED, args_b));  // 同集合共享可重入

  // fp={42,43} 仍被排他锁持有, 其共享锁应失败
  EXPECT_FALSE(slice_->Acquire(IntentLock::SHARED, args_a));

  slice_->Release(IntentLock::SHARED, args_b);
  slice_->Release(IntentLock::SHARED, args_b);
  slice_->Release(IntentLock::EXCLUSIVE, args_a);
}



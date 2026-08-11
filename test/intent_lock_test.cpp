#include <gtest/gtest.h>

#include "src/sharding/db_slice.hpp"
#include "src/sharding/engine_shard.hpp"

using namespace dfly;

class IntentLockTest : public ::testing::Test {
 protected:
  void SetUp() override {
    EngineShard::InitThreadLocal(&loop_);
    slice_ = std::make_unique<DbSlice>(0, false, EngineShard::tlocal());
  }

  void TearDown() override { EngineShard::DestroyThreadLocal(); }

  bool Acquire(IntentLock::Mode mode, const KeyLockArgs& args) {
    if (!slice_->Acquire(mode, args)) {
      slice_->Release(mode, args);
      return false;
    }
    return true;
  }
  base::UringProactor loop_;
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
  for (auto* args : {&args_ab, &args_abc, &args_abcd}) {
    EXPECT_TRUE(Acquire(IntentLock::SHARED, *args));
    slice_->Release(IntentLock::SHARED, *args);
    EXPECT_TRUE(Acquire(IntentLock::SHARED, *args));
    slice_->Release(IntentLock::SHARED, *args);
  }

  // 排他锁 — 2/3/4 个 fp 各测一轮
  for (auto* args : {&args_ab, &args_abc, &args_abcd}) {
    EXPECT_TRUE(Acquire(IntentLock::EXCLUSIVE, *args));
    slice_->Release(IntentLock::EXCLUSIVE, *args);
    EXPECT_TRUE(Acquire(IntentLock::EXCLUSIVE, *args));
    slice_->Release(IntentLock::EXCLUSIVE, *args);
  }
}

// 测试2: 多 fp 读写互斥
TEST_F(IntentLockTest, SharedExclusiveMutualExclusion) {
  // 2 个 fp
  {
    KeyLockArgs args = {0, {10, 20}};
    EXPECT_TRUE(Acquire(IntentLock::SHARED, args));
    EXPECT_FALSE(Acquire(IntentLock::EXCLUSIVE, args));
    slice_->Release(IntentLock::SHARED, args);

    EXPECT_TRUE(Acquire(IntentLock::EXCLUSIVE, args));
    EXPECT_FALSE(Acquire(IntentLock::SHARED, args));
    slice_->Release(IntentLock::EXCLUSIVE, args);
  }

  // 3 个 fp
  {
    KeyLockArgs args = {0, {100, 200, 300}};
    EXPECT_TRUE(Acquire(IntentLock::SHARED, args));
    EXPECT_FALSE(Acquire(IntentLock::EXCLUSIVE, args));
    slice_->Release(IntentLock::SHARED, args);

    EXPECT_TRUE(Acquire(IntentLock::EXCLUSIVE, args));
    EXPECT_FALSE(Acquire(IntentLock::SHARED, args));
    slice_->Release(IntentLock::EXCLUSIVE, args);
  }

  // 4 个 fp
  {
    KeyLockArgs args = {0, {1000, 2000, 3000, 4000}};
    EXPECT_TRUE(Acquire(IntentLock::SHARED, args));
    EXPECT_FALSE(Acquire(IntentLock::EXCLUSIVE, args));
    slice_->Release(IntentLock::SHARED, args);

    EXPECT_TRUE(Acquire(IntentLock::EXCLUSIVE, args));
    EXPECT_FALSE(Acquire(IntentLock::SHARED, args));
    slice_->Release(IntentLock::EXCLUSIVE, args);
  }
}

// 测试3: 部分锁冲突 —— 验证 Acquire 的 all-or-nothing 语义
//         关键: B_args 前几个 fp 不与 A 冲突，冲突出现在 i>0 位置，
//         这样才能真正测试到之前已获取锁的回滚路径。
TEST_F(IntentLockTest, PartialLockConflict) {
  // --- 场景1: EXCLUSIVE 冲突在 i=2, 回滚之前已获取的 fp ---
  //          A 持有 {1, 2} 排他, B 尝试 {3, 4, 2, 5} 排他
  //          B 先成功获取 3,4，到 2 时冲突(i=2), 需回滚 3,4
  {
    KeyLockArgs A_args = {0, {1, 2}};
    KeyLockArgs B_args = {0, {3, 4, 2, 5}};  // 冲突在 index=2

    EXPECT_TRUE(Acquire(IntentLock::EXCLUSIVE, A_args));
    EXPECT_FALSE(Acquire(IntentLock::EXCLUSIVE, B_args));

    // B 失败后, fp=3,4 必须被回滚, fp=5 未被触及: 三者都应可直接获取
    for (LockFp fp : {3ULL, 4ULL, 5ULL}) {
      KeyLockArgs test = {0, {fp}};
      EXPECT_TRUE(Acquire(IntentLock::EXCLUSIVE, test))
          << "fp=" << fp << " should be free after rollback";
      slice_->Release(IntentLock::EXCLUSIVE, test);
    }

    slice_->Release(IntentLock::EXCLUSIVE, A_args);
  }

  // --- 场景2: SHARED 持有者阻挡 EXCLUSIVE, 冲突在 i=2 ---
  //          A 持有 {11, 22} 共享, B 尝试 {33, 44, 11} 排他
  //          B 先成功获取 33,44 的排他锁, 到 11 时冲突(i=2), 需回滚 33,44
  {
    KeyLockArgs A_args = {0, {11, 22}};
    KeyLockArgs B_args = {0, {33, 44, 11}};  // 冲突在 index=2

    EXPECT_TRUE(Acquire(IntentLock::SHARED, A_args));
    EXPECT_FALSE(Acquire(IntentLock::EXCLUSIVE, B_args));

    // fp=33,44 必须被回滚
    for (LockFp fp : {33ULL, 44ULL}) {
      KeyLockArgs test = {0, {fp}};
      EXPECT_TRUE(Acquire(IntentLock::EXCLUSIVE, test))
          << "fp=" << fp << " should be free after rollback";
      slice_->Release(IntentLock::EXCLUSIVE, test);
    }

    slice_->Release(IntentLock::SHARED, A_args);
  }

  // --- 场景3: 完全不重叠, B 应成功 (无冲突路径) ---
  {
    KeyLockArgs A_args = {0, {100, 200}};
    KeyLockArgs B_args = {0, {300, 400, 500}};

    EXPECT_TRUE(Acquire(IntentLock::EXCLUSIVE, A_args));
    EXPECT_TRUE(Acquire(IntentLock::EXCLUSIVE, B_args));

    slice_->Release(IntentLock::EXCLUSIVE, B_args);
    slice_->Release(IntentLock::EXCLUSIVE, A_args);
  }

  // --- 场景4: 共享锁重叠, 双方都成功 ---
  {
    KeyLockArgs A_args = {0, {111, 222, 333}};
    KeyLockArgs B_args = {0, {333, 444, 555}};

    EXPECT_TRUE(Acquire(IntentLock::SHARED, A_args));
    EXPECT_TRUE(Acquire(IntentLock::SHARED, B_args));

    slice_->Release(IntentLock::SHARED, B_args);
    slice_->Release(IntentLock::SHARED, A_args);
  }

  // --- 场景5: EXCLUSIVE 冲突在 i=3, 回滚后释放 A, B 重试成功 ---
  //          A 持有 {50, 60} 排他, B 尝试 {70, 80, 90, 60} 排他
  //          B 先成功 70,80,90(i=0,1,2), 到 60 冲突(i=3), 回滚 70,80,90
  {
    KeyLockArgs A_args = {0, {50, 60}};
    KeyLockArgs B_args = {0, {70, 80, 90, 60}};  // 冲突在 index=3

    EXPECT_TRUE(Acquire(IntentLock::EXCLUSIVE, A_args));
    EXPECT_FALSE(Acquire(IntentLock::EXCLUSIVE, B_args));

    // 验证 70,80,90 全部回滚干净
    for (LockFp fp : {70ULL, 80ULL, 90ULL}) {
      KeyLockArgs test = {0, {fp}};
      EXPECT_TRUE(Acquire(IntentLock::SHARED, test))
          << "fp=" << fp << " should be free after rollback";
      slice_->Release(IntentLock::SHARED, test);
    }

    // A 释放后, B 重试应成功
    slice_->Release(IntentLock::EXCLUSIVE, A_args);
    EXPECT_TRUE(Acquire(IntentLock::EXCLUSIVE, B_args));
    slice_->Release(IntentLock::EXCLUSIVE, B_args);
  }

  // --- 场景6: 冲突在 i=0, 验证冲突 fp 计数不膨胀 ---
  //          A 持有 {201, 202, 203} 共享, B 尝试 {201, 204, 205} 排他
  //          冲突在 index=0, 仅需回滚 fp=201
  {
    KeyLockArgs A_args = {0, {201, 202, 203}};
    KeyLockArgs B_args = {0, {201, 204, 205}};  // 冲突在 index=0

    EXPECT_TRUE(Acquire(IntentLock::SHARED, A_args));
    EXPECT_FALSE(Acquire(IntentLock::EXCLUSIVE, B_args));

    // fp=204,205 未被触及, 应可直接获取
    for (LockFp fp : {204ULL, 205ULL}) {
      KeyLockArgs test = {0, {fp}};
      EXPECT_TRUE(Acquire(IntentLock::EXCLUSIVE, test))
          << "fp=" << fp << " should be free";
      slice_->Release(IntentLock::EXCLUSIVE, test);
    }

    // fp=201 的共享计数不应膨胀: 释放一次共享后即可获取排他锁
    slice_->Release(IntentLock::SHARED, A_args);

    KeyLockArgs test_201 = {0, {201}};
    EXPECT_TRUE(Acquire(IntentLock::EXCLUSIVE, test_201))
        << "fp=201 exclusive should succeed after single shared release";
    slice_->Release(IntentLock::EXCLUSIVE, test_201);
  }
}

// 附加: 多个共享锁可以共存 (多 fp)
TEST_F(IntentLockTest, MultipleSharedLocks) {
  KeyLockArgs args = {0, {1, 2, 3}};

  EXPECT_TRUE(Acquire(IntentLock::SHARED, args));
  EXPECT_TRUE(Acquire(IntentLock::SHARED, args));
  EXPECT_TRUE(Acquire(IntentLock::SHARED, args));

  // 共享锁仍在持有，排他锁应失败
  EXPECT_FALSE(Acquire(IntentLock::EXCLUSIVE, args));

  slice_->Release(IntentLock::SHARED, args);
  slice_->Release(IntentLock::SHARED, args);
  slice_->Release(IntentLock::SHARED, args);
}

// 附加: 不同 fp 集合的锁互不干扰
TEST_F(IntentLockTest, DifferentFpIsolation) {
  KeyLockArgs args_a = {0, {42, 43}};
  KeyLockArgs args_b = {0, {99, 100}};

  // fp={42,43} 加排他锁, 不影响 fp={99,100} 的共享锁
  EXPECT_TRUE(Acquire(IntentLock::EXCLUSIVE, args_a));
  EXPECT_TRUE(Acquire(IntentLock::SHARED, args_b));
  EXPECT_TRUE(Acquire(IntentLock::SHARED, args_b));  // 同集合共享可重入

  // fp={42,43} 仍被排他锁持有, 其共享锁应失败
  EXPECT_FALSE(Acquire(IntentLock::SHARED, args_a));

  slice_->Release(IntentLock::SHARED, args_b);
  slice_->Release(IntentLock::SHARED, args_b);
  slice_->Release(IntentLock::EXCLUSIVE, args_a);
}

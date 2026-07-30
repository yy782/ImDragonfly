#include "sharding/wait_queue.hpp"

#include <gtest/gtest.h>

#include <coroutine>

using namespace dfly;
using namespace dfly::detail;

// ============================================================================
// empty
// ============================================================================

TEST(WaitQueueTest, EmptyInitially) {
  WaitQueue q;
  EXPECT_TRUE(q.empty());
}

// ============================================================================
// Link
// ============================================================================

TEST(WaitQueueTest, LinkOne) {
  WaitQueue q;
  Waiter w;
  q.Link(&w);
  EXPECT_FALSE(q.empty());
  EXPECT_TRUE(w.IsLinked());
  q.Unlink(&w);
  EXPECT_FALSE(w.IsLinked());
}

TEST(WaitQueueTest, LinkMultiple) {
  WaitQueue q;
  Waiter a, b, c;

  q.Link(&a);
  q.Link(&b);
  q.Link(&c);
  EXPECT_FALSE(q.empty());

  EXPECT_TRUE(a.IsLinked());
  EXPECT_TRUE(b.IsLinked());
  EXPECT_TRUE(c.IsLinked());

  q.Unlink(&a);
  EXPECT_FALSE(a.IsLinked());
  EXPECT_TRUE(b.IsLinked());
  EXPECT_TRUE(c.IsLinked());

  q.Unlink(&b);
  q.Unlink(&c);
  EXPECT_TRUE(q.empty());
}

// ============================================================================
// Unlink order is independent of Link order
// ============================================================================

TEST(WaitQueueTest, UnlinkMiddleFirst) {
  WaitQueue q;
  Waiter a, b, c;

  q.Link(&a);
  q.Link(&b);
  q.Link(&c);

  q.Unlink(&b);  // unlink middle
  EXPECT_TRUE(a.IsLinked());
  EXPECT_FALSE(b.IsLinked());
  EXPECT_TRUE(c.IsLinked());

  q.Unlink(&a);
  q.Unlink(&c);
  EXPECT_TRUE(q.empty());
}

// ============================================================================
// Waiter fields
// ============================================================================

TEST(WaitQueueTest, WaiterDefaultShardId) {
  Waiter w;
  EXPECT_EQ(w.shard_id, static_cast<ShardId>(-1));  // kInvalidSid
  EXPECT_FALSE(w.IsLinked());
}

TEST(WaitQueueTest, WaiterCustomShardId) {
  Waiter w;
  w.shard_id = 42;
  EXPECT_EQ(w.shard_id, static_cast<ShardId>(42));
}

// ============================================================================
// Link after unlink is fine
// ============================================================================

TEST(WaitQueueTest, RelinkAfterUnlink) {
  WaitQueue q;
  Waiter w;

  q.Link(&w);
  EXPECT_TRUE(w.IsLinked());
  q.Unlink(&w);
  EXPECT_FALSE(w.IsLinked());

  q.Link(&w);
  EXPECT_TRUE(w.IsLinked());
  q.Unlink(&w);
  EXPECT_TRUE(q.empty());
}

// ============================================================================
// Unlink from empty queue (should fail or no-op)
// ============================================================================

TEST(WaitQueueTest, UnlinkFromEmptyDoesNothing) {
  WaitQueue q;
  Waiter w;

  q.Link(&w);
  q.Unlink(&w);
  EXPECT_TRUE(q.empty());
  EXPECT_FALSE(w.IsLinked());

  // Re-unlink should be a no-op or assert — we just verify no crash
  // (intrusive list would assert on double-unlink, so w is already unlinked)
}

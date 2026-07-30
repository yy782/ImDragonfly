#include "redis/skiplist.hpp"

#include <gtest/gtest.h>

#include <random>
#include <string>
#include <utility>
#include <vector>

using namespace dfly;

using KV = std::pair<int, std::string>;

// ============================================================================
// Basic Insert / Find
// ============================================================================

TEST(SkipListTest, EmptySkipList) {
  SkipList<int> sl;
  EXPECT_EQ(sl.Length(), 0u);
  EXPECT_TRUE(sl.Empty());
}

TEST(SkipListTest, InsertSingle) {
  SkipList<int> sl;
  EXPECT_TRUE(sl.Insert(10));
  EXPECT_EQ(sl.Length(), 1u);
}

TEST(SkipListTest, InsertDuplicate) {
  SkipList<int> sl;
  EXPECT_TRUE(sl.Insert(10));
  EXPECT_FALSE(sl.Insert(10));
  EXPECT_EQ(sl.Length(), 1u);
}

TEST(SkipListTest, FindFound) {
  SkipList<int> sl;
  sl.Insert(10);
  auto node = sl.Find(10);
  EXPECT_TRUE(node.has_value());
  EXPECT_EQ((*node)->data, 10);
}

TEST(SkipListTest, FindNotFound) {
  SkipList<int> sl;
  sl.Insert(10);
  auto node = sl.Find(5);
  EXPECT_FALSE(node.has_value());
}

TEST(SkipListTest, InsertMultipleOrdered) {
  SkipList<int> sl;
  sl.Insert(1);
  sl.Insert(2);
  sl.Insert(3);
  EXPECT_EQ(sl.Length(), 3u);

  EXPECT_TRUE(sl.Find(1).has_value());
  EXPECT_TRUE(sl.Find(2).has_value());
  EXPECT_TRUE(sl.Find(3).has_value());
}

TEST(SkipListTest, InsertReverseOrder) {
  SkipList<int> sl;
  sl.Insert(3);
  sl.Insert(2);
  sl.Insert(1);
  EXPECT_EQ(sl.Length(), 3u);

  EXPECT_TRUE(sl.Find(1).has_value());
  EXPECT_TRUE(sl.Find(2).has_value());
  EXPECT_TRUE(sl.Find(3).has_value());
}

TEST(SkipListTest, InsertRandomOrder) {
  SkipList<int> sl;
  sl.Insert(5);
  sl.Insert(1);
  sl.Insert(3);
  sl.Insert(2);
  sl.Insert(4);
  EXPECT_EQ(sl.Length(), 5u);

  for (int i = 1; i <= 5; ++i) {
    EXPECT_TRUE(sl.Find(i).has_value());
  }
}

// ============================================================================
// Remove
// ============================================================================

TEST(SkipListTest, RemoveExisting) {
  SkipList<int> sl;
  sl.Insert(1);
  sl.Insert(2);
  sl.Insert(3);
  EXPECT_EQ(sl.Length(), 3u);

  EXPECT_TRUE(sl.Remove(2));
  EXPECT_EQ(sl.Length(), 2u);

  EXPECT_FALSE(sl.Find(2).has_value());
  EXPECT_TRUE(sl.Find(1).has_value());
  EXPECT_TRUE(sl.Find(3).has_value());
}

TEST(SkipListTest, RemoveNonExisting) {
  SkipList<int> sl;
  sl.Insert(1);
  EXPECT_EQ(sl.Length(), 1u);

  EXPECT_FALSE(sl.Remove(99));
  EXPECT_EQ(sl.Length(), 1u);
}

TEST(SkipListTest, RemoveFromEmpty) {
  SkipList<int> sl;
  EXPECT_FALSE(sl.Remove(1));
  EXPECT_EQ(sl.Length(), 0u);
}

TEST(SkipListTest, RemoveAllThenCheck) {
  SkipList<int> sl;
  for (int i = 0; i < 10; ++i) {
    sl.Insert(i);
  }
  EXPECT_EQ(sl.Length(), 10u);

  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(sl.Remove(i));
  }
  EXPECT_EQ(sl.Length(), 0u);
  EXPECT_TRUE(sl.Empty());
}

// ============================================================================
// Rank / GetByRank
// ============================================================================

TEST(SkipListTest, Rank) {
  SkipList<int> sl;
  sl.Insert(5);
  sl.Insert(1);
  sl.Insert(3);

  EXPECT_EQ(sl.Rank(1), 0);
  EXPECT_EQ(sl.Rank(3), 1);
  EXPECT_EQ(sl.Rank(5), 2);
  EXPECT_EQ(sl.Rank(99), -1);
}

TEST(SkipListTest, GetByRank) {
  SkipList<int> sl;
  sl.Insert(5);
  sl.Insert(1);
  sl.Insert(3);

  auto n0 = sl.GetByRank(0);
  EXPECT_TRUE(n0.has_value());
  EXPECT_EQ((*n0)->data, 1);

  auto n1 = sl.GetByRank(1);
  EXPECT_TRUE(n1.has_value());
  EXPECT_EQ((*n1)->data, 3);

  auto n2 = sl.GetByRank(2);
  EXPECT_TRUE(n2.has_value());
  EXPECT_EQ((*n2)->data, 5);

  EXPECT_FALSE(sl.GetByRank(3).has_value());
  EXPECT_FALSE(sl.GetByRank(-1).has_value());
}

// ============================================================================
// Range / RevRange
// ============================================================================

TEST(SkipListTest, Range) {
  SkipList<int> sl;
  sl.Insert(1);
  sl.Insert(2);
  sl.Insert(3);

  auto r = sl.Range(0, 1);
  ASSERT_EQ(r.size(), 2u);
  EXPECT_EQ(r[0]->data, 1);
  EXPECT_EQ(r[1]->data, 2);
}

TEST(SkipListTest, RangeNegativeIndex) {
  SkipList<int> sl;
  sl.Insert(1);
  sl.Insert(2);
  sl.Insert(3);

  auto r = sl.Range(-1, -1);
  ASSERT_EQ(r.size(), 1u);
  EXPECT_EQ(r[0]->data, 3);
}

TEST(SkipListTest, RevRange) {
  SkipList<int> sl;
  sl.Insert(1);
  sl.Insert(2);
  sl.Insert(3);

  auto r = sl.RevRange(0, 2);
  ASSERT_EQ(r.size(), 3u);
  EXPECT_EQ(r[0]->data, 3);
  EXPECT_EQ(r[1]->data, 2);
  EXPECT_EQ(r[2]->data, 1);
}

// ============================================================================
// Large Scale Test
// ============================================================================

TEST(SkipListTest, LargeScaleInsertAndFind) {
  SkipList<int> sl;
  constexpr int kN = 1000;

  for (int i = 0; i < kN; ++i) {
    sl.Insert(i);
  }
  EXPECT_EQ(sl.Length(), static_cast<size_t>(kN));

  for (int i = 0; i < kN; ++i) {
    EXPECT_TRUE(sl.Find(i).has_value());
  }
}

TEST(SkipListTest, RandomInsertFind) {
  SkipList<int> sl;
  std::mt19937 rng(42);
  std::vector<int> vals(500);
  for (int i = 0; i < 500; ++i) vals[i] = i;
  std::shuffle(vals.begin(), vals.end(), rng);

  for (int v : vals) {
    sl.Insert(v);
  }
  EXPECT_EQ(sl.Length(), 500u);

  for (int i = 0; i < 500; ++i) {
    EXPECT_TRUE(sl.Find(i).has_value());
  }
}

// ============================================================================
// String keys
// ============================================================================

TEST(SkipListTest, StringKeys) {
  SkipList<std::string> sl;
  sl.Insert("banana");
  sl.Insert("apple");
  sl.Insert("cherry");

  EXPECT_EQ(sl.Length(), 3u);

  EXPECT_TRUE(sl.Find("apple").has_value());
  EXPECT_TRUE(sl.Find("banana").has_value());
  EXPECT_TRUE(sl.Find("cherry").has_value());

  auto r = sl.Range(0, 2);
  ASSERT_EQ(r.size(), 3u);
  EXPECT_EQ(r[0]->data, "apple");
  EXPECT_EQ(r[1]->data, "banana");
  EXPECT_EQ(r[2]->data, "cherry");
}

// ============================================================================
// Custom Compare (greater)
// ============================================================================

TEST(SkipListTest, GreaterCompareDescending) {
  SkipList<int, std::greater<int>> sl;
  sl.Insert(1);
  sl.Insert(2);
  sl.Insert(3);

  auto r = sl.Range(0, 2);
  ASSERT_EQ(r.size(), 3u);
  EXPECT_EQ(r[0]->data, 3);
  EXPECT_EQ(r[1]->data, 2);
  EXPECT_EQ(r[2]->data, 1);
}

TEST(SkipListTest, GreaterCompareRank) {
  SkipList<int, std::greater<int>> sl;
  sl.Insert(10);
  sl.Insert(20);
  sl.Insert(30);

  EXPECT_EQ(sl.Rank(30), 0);
  EXPECT_EQ(sl.Rank(20), 1);
  EXPECT_EQ(sl.Rank(10), 2);
}

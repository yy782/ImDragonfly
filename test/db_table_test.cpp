#include <gtest/gtest.h>
#include "src/sharding/db_table.hpp"
// ./db_table_test
namespace dfly {

class DbTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    table_ = std::make_unique<DbTable>(PMR_NS::get_default_resource(), 0);
  }

  void TearDown() override {
    table_.reset();
  }

  std::unique_ptr<DbTable> table_;
};

TEST_F(DbTableTest, InsertAndFind) {
  auto it = table_->prime().InsertNew("key1", PrimeValue{});
  EXPECT_FALSE(it.is_done());
  EXPECT_EQ(it->first.ToString(), "key1");

  auto found_it = table_->prime().Find("key1");
  EXPECT_FALSE(found_it.is_done());
  EXPECT_EQ(found_it->first.ToString(), "key1");
}

TEST_F(DbTableTest, FindNonExistent) {
  auto it = table_->prime().Find("nonexistent");
  EXPECT_TRUE(it.is_done());
}

TEST_F(DbTableTest, Erase) {
  auto insert_it = table_->prime().InsertNew("key1", PrimeValue{});
  EXPECT_EQ(table_->prime().size(), 1);

  table_->prime().Erase(insert_it);
  EXPECT_EQ(table_->prime().size(), 0);

  auto it = table_->prime().Find("key1");
  EXPECT_TRUE(it.is_done());
}

TEST_F(DbTableTest, Clear) {
  for (int i = 0; i < 100; ++i) {
    table_->prime().InsertNew("key" + std::to_string(i), PrimeValue{});
  }
  EXPECT_EQ(table_->prime().size(), 100);

  table_->prime().Clear();
  EXPECT_EQ(table_->prime().size(), 0);
  EXPECT_TRUE(table_->prime().Empty());
}

TEST_F(DbTableTest, SizeGrowth) {
  EXPECT_EQ(table_->prime().size(), 0);

  for (int i = 0; i < 1000; ++i) {
    table_->prime().InsertNew("key" + std::to_string(i), PrimeValue{});
  }
  EXPECT_EQ(table_->prime().size(), 1000);
}

TEST_F(DbTableTest, IndexAccess) {
  EXPECT_EQ(table_->index(), 0);

  DbTable table2(PMR_NS::get_default_resource(), 5);
  EXPECT_EQ(table2.index(), 5);
}

TEST_F(DbTableTest, EmptyCheck) {
  EXPECT_TRUE(table_->prime().Empty());
  table_->prime().InsertNew("key1", PrimeValue{});
  EXPECT_FALSE(table_->prime().Empty());
}

TEST_F(DbTableTest, LargeInsert) {
  const int kNumElements = 10000;
  for (int i = 0; i < kNumElements; ++i) {
    table_->prime().InsertNew("key" + std::to_string(i), PrimeValue{});
  }
  EXPECT_EQ(table_->prime().size(), kNumElements);

  for (int i = 0; i < kNumElements; ++i) {
    auto it = table_->prime().Find("key" + std::to_string(i));
    EXPECT_FALSE(it.is_done());
  }
}

TEST_F(DbTableTest, MultipleOperations) {
  std::vector<PrimeIterator> iterators;
  for (int i = 0; i < 100; ++i) {
    auto it = table_->prime().InsertNew("key" + std::to_string(i), PrimeValue{});
    if (i % 2 == 0) {
      iterators.push_back(it);
    }
  }

  for (auto& it : iterators) {
    table_->prime().Erase(it);
  }
  EXPECT_EQ(table_->prime().size(), 50);

  for (int i = 1; i < 100; i += 2) {
    auto it = table_->prime().Find("key" + std::to_string(i));
    EXPECT_FALSE(it.is_done());
  }

  for (int i = 0; i < 100; i += 2) {
    auto it = table_->prime().Find("key" + std::to_string(i));
    EXPECT_TRUE(it.is_done());
  }
}

}  // namespace dfly

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

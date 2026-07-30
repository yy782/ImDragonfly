#include "sharding/op_status.hpp"

#include <gtest/gtest.h>

#include <string>
#include <string_view>

using namespace facade;

// ============================================================================
// OpResultBase
// ============================================================================

TEST(OpResultBaseTest, DefaultIsOk) {
  OpResultBase result;
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.status(), OpStatus::OK);
  EXPECT_TRUE(static_cast<bool>(result));
}

TEST(OpResultBaseTest, ConstructWithOk) {
  OpResultBase result(OpStatus::OK);
  EXPECT_TRUE(result.ok());
}

TEST(OpResultBaseTest, ConstructWithError) {
  OpResultBase result(OpStatus::KEY_NOTFOUND);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status(), OpStatus::KEY_NOTFOUND);
  EXPECT_FALSE(static_cast<bool>(result));
}

TEST(OpResultBaseTest, AllStatusCodes) {
  OpStatus codes[] = {OpStatus::OK,          OpStatus::KEY_NOTFOUND,
                      OpStatus::WRONG_TYPE,  OpStatus::SKIPPED,
                      OpStatus::NO_KEY,      OpStatus::OUT_OF_RANGE,
                      OpStatus::SYNTAX_ERROR};
  for (auto st : codes) {
    OpResultBase r(st);
    EXPECT_EQ(r.status(), st);
    if (st == OpStatus::OK)
      EXPECT_TRUE(r.ok());
    else
      EXPECT_FALSE(r.ok());
  }
}

TEST(OpResultBaseTest, CompareEquality) {
  OpResultBase r(OpStatus::KEY_NOTFOUND);
  EXPECT_TRUE(r == OpStatus::KEY_NOTFOUND);
  EXPECT_FALSE(r == OpStatus::OK);
  EXPECT_TRUE(r != OpStatus::OK);
}

TEST(OpResultBaseTest, ReverseCompareEquality) {
  OpResultBase r(OpStatus::WRONG_TYPE);
  EXPECT_TRUE(OpStatus::WRONG_TYPE == r);
  EXPECT_FALSE(OpStatus::OK == r);
}

// ============================================================================
// OpResult<T> — value holder
// ============================================================================

TEST(OpResultValueTest, DefaultConstructedIsOk) {
  OpResult<int> r{};
  EXPECT_TRUE(r.ok());
}

TEST(OpResultValueTest, ConstructFromStatus) {
  OpResult<int> r(OpStatus::KEY_NOTFOUND);
  EXPECT_FALSE(r.ok());
}

TEST(OpResultValueTest, ConstructFromValue) {
  OpResult<std::string> r(std::string("hello"));
  EXPECT_TRUE(r.ok());
  EXPECT_EQ(r.value(), "hello");
}

TEST(OpResultValueTest, ConstructFromConstRef) {
  std::string data = "world";
  OpResult<std::string> r(data);
  EXPECT_TRUE(r.ok());
  EXPECT_EQ(r.value(), "world");
}

TEST(OpResultValueTest, Dereference) {
  OpResult<std::string> r(std::string("test"));
  EXPECT_EQ(*r, "test");

  const OpResult<std::string>& cr = r;
  EXPECT_EQ(*cr, "test");
}

TEST(OpResultValueTest, ArrowOperator) {
  OpResult<std::string> r(std::string("arrow"));
  EXPECT_EQ(r->size(), 5u);
  EXPECT_EQ(r->at(0), 'a');
}

TEST(OpResultValueTest, ConstArrowOperator) {
  OpResult<std::string> r(std::string("const arrow"));
  const auto& cr = r;
  EXPECT_EQ(cr->size(), 11u);
}

TEST(OpResultValueTest, ValueOr) {
  OpResult<int> ok_result(42);
  EXPECT_EQ(ok_result.value_or(0), 42);

  OpResult<int> err_result(OpStatus::KEY_NOTFOUND);
  EXPECT_EQ(err_result.value_or(0), 0);
}

TEST(OpResultValueTest, MoveFromRvalue) {
  OpResult<std::string> r(std::string("movable"));
  std::string moved = *std::move(r);
  EXPECT_EQ(moved, "movable");
}

TEST(OpResultValueTest, BoolConversion) {
  OpResult<double> ok_val(3.14);
  EXPECT_TRUE(ok_val);

  OpResult<double> err_val(OpStatus::WRONG_TYPE);
  EXPECT_FALSE(err_val);
}

// ============================================================================
// OpResult<void>
// ============================================================================

TEST(OpResultVoidTest, DefaultIsOk) {
  OpResult<void> r;
  EXPECT_TRUE(r.ok());
  EXPECT_TRUE(static_cast<bool>(r));
}

TEST(OpResultVoidTest, ConstructWithStatus) {
  OpResult<void> ok(OpStatus::OK);
  EXPECT_TRUE(ok.ok());

  OpResult<void> err(OpStatus::OUT_OF_RANGE);
  EXPECT_FALSE(err.ok());
  EXPECT_EQ(err.status(), OpStatus::OUT_OF_RANGE);
}

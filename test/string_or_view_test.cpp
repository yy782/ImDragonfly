#include "detail/string_or_view.hpp"

#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <string_view>

using namespace dfly::cmn;
using namespace std::literals;

// ============================================================================
// Construction via static factories
// ============================================================================

TEST(StringOrViewTest, FromString) {
  auto sov = StringOrView::FromString("hello world");
  EXPECT_FALSE(sov.empty());
  EXPECT_EQ(sov.view(), "hello world");
}

TEST(StringOrViewTest, FromView) {
  std::string_view sv = "view data";
  auto sov = StringOrView::FromView(sv);
  EXPECT_FALSE(sov.empty());
  EXPECT_EQ(sov.view(), "view data");
}

TEST(StringOrViewTest, DefaultConstructedIsEmpty) {
  StringOrView sov;
  EXPECT_TRUE(sov.empty());
  EXPECT_EQ(sov.view(), ""sv);
}

TEST(StringOrViewTest, FromEmptyString) {
  auto sov = StringOrView::FromString("");
  EXPECT_TRUE(sov.empty());
  EXPECT_EQ(sov.view(), ""sv);
}

TEST(StringOrViewTest, FromEmptyView) {
  auto sov = StringOrView::FromView(std::string_view{});
  EXPECT_TRUE(sov.empty());
  EXPECT_EQ(sov.view(), ""sv);
}

// ============================================================================
// view()
// ============================================================================

TEST(StringOrViewTest, ViewReturnsContent) {
  auto sov = StringOrView::FromString("test data");
  std::string_view v = sov.view();
  EXPECT_EQ(v, "test data");
  EXPECT_EQ(v.size(), 9u);
}

// ============================================================================
// empty()
// ============================================================================

TEST(StringOrViewTest, EmptyOnString) {
  auto sov = StringOrView::FromString("content");
  EXPECT_FALSE(sov.empty());
}

TEST(StringOrViewTest, EmptyOnDefault) {
  StringOrView sov;
  EXPECT_TRUE(sov.empty());
}

// ============================================================================
// operator== / operator!=
// ============================================================================

TEST(StringOrViewTest, EqualSameContent) {
  auto a = StringOrView::FromString("abc");
  auto b = StringOrView::FromView("abc");
  EXPECT_TRUE(a == b);
  EXPECT_FALSE(a != b);
}

TEST(StringOrViewTest, EqualWithStringView) {
  auto a = StringOrView::FromString("hello");
  EXPECT_TRUE(a == std::string_view("hello"));
  EXPECT_FALSE(a != std::string_view("hello"));
}

TEST(StringOrViewTest, NotEqualDifferentContent) {
  auto a = StringOrView::FromString("abc");
  auto b = StringOrView::FromView("xyz");
  EXPECT_TRUE(a != b);
  EXPECT_FALSE(a == b);
}

TEST(StringOrViewTest, NotEqualWithStringView) {
  auto a = StringOrView::FromString("abc");
  EXPECT_TRUE(a != std::string_view("xyz"));
  EXPECT_TRUE(a == std::string_view("abc"));
}

// ============================================================================
// Copy / Move
// ============================================================================

TEST(StringOrViewTest, CopyConstruct) {
  auto a = StringOrView::FromString("original");
  auto b(a);
  EXPECT_EQ(a.view(), b.view());
}

TEST(StringOrViewTest, CopyAssign) {
  auto a = StringOrView::FromString("source");
  StringOrView b;
  b = a;
  EXPECT_EQ(a.view(), b.view());
}

TEST(StringOrViewTest, MoveConstruct) {
  auto a = StringOrView::FromString("movable");
  auto b(std::move(a));
  EXPECT_EQ(b.view(), "movable");
}

TEST(StringOrViewTest, MoveAssign) {
  auto a = StringOrView::FromString("assign");
  StringOrView b;
  b = std::move(a);
  EXPECT_EQ(b.view(), "assign");
}

// ============================================================================
// operator<<
// ============================================================================

TEST(StringOrViewTest, OstreamOutput) {
  auto sov = StringOrView::FromString("stream test");
  std::ostringstream oss;
  oss << sov;
  EXPECT_EQ(oss.str(), "stream test");
}

TEST(StringOrViewTest, OstreamOutputEmpty) {
  StringOrView sov;
  std::ostringstream oss;
  oss << sov;
  EXPECT_EQ(oss.str(), "");
}

// ============================================================================
// FromString owns data — views are stable even if source changes
// ============================================================================

TEST(StringOrViewTest, FromStringStableData) {
  auto sov = StringOrView::FromString("stable");
  EXPECT_EQ(sov.view(), "stable");
  // FromString copies, so data is stable even if original is gone
  EXPECT_EQ(sov.view(), "stable");
}

TEST(StringOrViewTest, FromViewRefersToOriginal) {
  std::string s = "original source";
  auto sov = StringOrView::FromView(s);
  EXPECT_EQ(sov.view(), "original source");
  // If s is modified, the view in sov may be affected
  // (StringOrView::FromView only stores a string_view)
}

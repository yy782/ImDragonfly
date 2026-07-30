#include "command_layer/cmd_arg_parser.hpp"

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "Strings.hpp"
#include "command_layer/cmn_types.hpp"

using namespace cmd;

// ============================================================================
// EqualsIgnoreCaseStd (from Strings.hpp, used internally by Check/MapNext)
// ============================================================================

TEST(CmdArgUtilTest, EqualsIgnoreCaseExact) {
  EXPECT_TRUE(base::EqualsIgnoreCaseStd(std::string_view("hello"),
                                        std::string("hello")));
}

TEST(CmdArgUtilTest, EqualsIgnoreCaseDifferentCase) {
  EXPECT_TRUE(base::EqualsIgnoreCaseStd(std::string_view("HELLO"),
                                        std::string("hello")));
}

TEST(CmdArgUtilTest, EqualsIgnoreCaseMismatch) {
  EXPECT_FALSE(base::EqualsIgnoreCaseStd(std::string_view("hello"),
                                         std::string("world")));
}

TEST(CmdArgUtilTest, EqualsIgnoreCaseDifferentLength) {
  EXPECT_FALSE(base::EqualsIgnoreCaseStd(std::string_view("hello"),
                                         std::string("helloo")));
}

// ============================================================================
// HasNext / HasError
// ============================================================================

TEST(CmdArgParserTest, EmptyArgSlice) {
  CmdArgParser parser(ArgSlice{});
  EXPECT_FALSE(parser.HasNext());
  EXPECT_FALSE(parser.HasError());
}

TEST(CmdArgParserTest, HasNextWithArgs) {
  std::string_view args[] = {"SET", "key", "value"};
  CmdArgParser parser(ArgSlice(args, 3));
  EXPECT_TRUE(parser.HasNext());
}

TEST(CmdArgParserTest, HasAtLeast) {
  std::string_view args[] = {"a", "b", "c"};
  CmdArgParser parser(ArgSlice(args, 3));
  EXPECT_TRUE(parser.HasAtLeast(3));
  EXPECT_FALSE(parser.HasAtLeast(4));
}

// ============================================================================
// Peek
// ============================================================================

TEST(CmdArgParserTest, PeekFirstArg) {
  std::string_view args[] = {"SET", "key"};
  CmdArgParser parser(ArgSlice(args, 2));
  EXPECT_EQ(parser.Peek(), "SET");
}

TEST(CmdArgParserTest, PeekDoesNotAdvance) {
  std::string_view args[] = {"SET", "key"};
  CmdArgParser parser(ArgSlice(args, 2));
  EXPECT_EQ(parser.Peek(), "SET");
  EXPECT_EQ(parser.Peek(), "SET");  // unchanged
}

// ============================================================================
// Next<T> — single value
// ============================================================================

TEST(CmdArgParserTest, NextSingleStringView) {
  std::string_view args[] = {"hello"};
  CmdArgParser parser(ArgSlice(args, 1));
  auto val = parser.Next<std::string_view>();
  EXPECT_EQ(val, "hello");
  EXPECT_FALSE(parser.HasNext());
}

TEST(CmdArgParserTest, NextSingleInt) {
  std::string_view args[] = {"42"};
  CmdArgParser parser(ArgSlice(args, 1));
  auto val = parser.Next<int64_t>();
  EXPECT_EQ(val, 42);
}

TEST(CmdArgParserTest, NextSingleNegativeInt) {
  std::string_view args[] = {"-10"};
  CmdArgParser parser(ArgSlice(args, 1));
  auto val = parser.Next<int64_t>();
  EXPECT_EQ(val, -10);
}

TEST(CmdArgParserTest, NextIntFailsOnNonNumeric) {
  std::string_view args[] = {"not_a_number"};
  CmdArgParser parser(ArgSlice(args, 1));
  (void)parser.Next<int64_t>();
  EXPECT_TRUE(parser.HasError());
}

// ============================================================================
// Next<T, Ts...> — tuple
// ============================================================================

TEST(CmdArgParserTest, NextTwoStrings) {
  std::string_view args[] = {"key", "value"};
  CmdArgParser parser(ArgSlice(args, 2));
  auto [k, v] = parser.Next<std::string_view, std::string_view>();
  EXPECT_EQ(k, "key");
  EXPECT_EQ(v, "value");
}

TEST(CmdArgParserTest, NextStringAndInt) {
  std::string_view args[] = {"count", "100"};
  CmdArgParser parser(ArgSlice(args, 2));
  auto [name, num] = parser.Next<std::string_view, int64_t>();
  EXPECT_EQ(name, "count");
  EXPECT_EQ(num, 100);
}

TEST(CmdArgParserTest, NextThreeValues) {
  std::string_view args[] = {"X", "10", "20"};
  CmdArgParser parser(ArgSlice(args, 3));
  auto [tag, a, b] = parser.Next<std::string_view, int64_t, int64_t>();
  EXPECT_EQ(tag, "X");
  EXPECT_EQ(a, 10);
  EXPECT_EQ(b, 20);
}

TEST(CmdArgParserTest, NextNotEnoughArgs) {
  std::string_view args[] = {"only_one"};
  CmdArgParser parser(ArgSlice(args, 1));
  (void)parser.Next<std::string_view, std::string_view>();
  EXPECT_TRUE(parser.HasError());
}

// ============================================================================
// NextOrDefault
// ============================================================================

TEST(CmdArgParserTest, NextOrDefaultHasValue) {
  std::string_view args[] = {"present"};
  CmdArgParser parser(ArgSlice(args, 1));
  auto val = parser.NextOrDefault<std::string_view>("default");
  EXPECT_EQ(val, "present");
}

TEST(CmdArgParserTest, NextOrDefaultFallsBack) {
  CmdArgParser parser(ArgSlice{});
  auto val = parser.NextOrDefault<std::string_view>("default");
  EXPECT_EQ(val, "default");
}

// ============================================================================
// Check
// ============================================================================

TEST(CmdArgParserTest, CheckMatch) {
  std::string_view args[] = {"NX"};
  CmdArgParser parser(ArgSlice(args, 1));
  EXPECT_TRUE(parser.Check("NX"));
  EXPECT_FALSE(parser.HasNext());
}

TEST(CmdArgParserTest, CheckCaseInsensitive) {
  std::string_view args[] = {"nx"};
  CmdArgParser parser(ArgSlice(args, 1));
  EXPECT_TRUE(parser.Check("NX"));
}

TEST(CmdArgParserTest, CheckMismatchDoesNotConsume) {
  std::string_view args[] = {"XX"};
  CmdArgParser parser(ArgSlice(args, 1));
  EXPECT_FALSE(parser.Check("NX"));
  EXPECT_TRUE(parser.HasNext());  // arg was NOT consumed
  EXPECT_EQ(parser.Peek(), "XX");
}

TEST(CmdArgParserTest, CheckEmptyArgs) {
  CmdArgParser parser(ArgSlice{});
  EXPECT_FALSE(parser.Check("NX"));
}

// ============================================================================
// Skip
// ============================================================================

TEST(CmdArgParserTest, SkipOne) {
  std::string_view args[] = {"skip_me", "target"};
  CmdArgParser parser(ArgSlice(args, 2));
  parser.Skip(1);
  EXPECT_EQ(parser.Peek(), "target");
}

TEST(CmdArgParserTest, SkipTooMany) {
  std::string_view args[] = {"only"};
  CmdArgParser parser(ArgSlice(args, 1));
  parser.Skip(5);
  EXPECT_TRUE(parser.HasError());
}

// ============================================================================
// Tail
// ============================================================================

TEST(CmdArgParserTest, TailAll) {
  std::string_view args[] = {"cmd", "sub"};
  CmdArgParser parser(ArgSlice(args, 2));
  auto tail = parser.Tail();
  ASSERT_EQ(tail.size(), 2u);
  EXPECT_EQ(tail[0], "cmd");
  EXPECT_EQ(tail[1], "sub");
}

TEST(CmdArgParserTest, TailAfterConsume) {
  std::string_view args[] = {"cmd", "key", "value1", "value2"};
  CmdArgParser parser(ArgSlice(args, 4));
  (void)parser.Next<std::string_view>();  // consume "cmd"
  auto tail = parser.Tail();
  ASSERT_EQ(tail.size(), 3u);
  EXPECT_EQ(tail[0], "key");
  EXPECT_EQ(tail[1], "value1");
  EXPECT_EQ(tail[2], "value2");
}

// ============================================================================
// MapNext
// ============================================================================

TEST(CmdArgParserTest, MapNextMatch) {
  std::string_view args[] = {"NX"};
  CmdArgParser parser(ArgSlice(args, 1));
  auto val = parser.MapNext("NX", 1, "XX", 2);
  EXPECT_EQ(val, 1);
}

TEST(CmdArgParserTest, MapNextSecondMatch) {
  std::string_view args[] = {"XX"};
  CmdArgParser parser(ArgSlice(args, 1));
  auto val = parser.MapNext("NX", 1, "XX", 2);
  EXPECT_EQ(val, 2);
}

TEST(CmdArgParserTest, MapNextNoMatch) {
  std::string_view args[] = {"unknown"};
  CmdArgParser parser(ArgSlice(args, 1));
  parser.MapNext("NX", 1, "XX", 2);
  EXPECT_TRUE(parser.HasError());
}

// ============================================================================
// TryMapNext
// ============================================================================

TEST(CmdArgParserTest, TryMapNextMatch) {
  std::string_view args[] = {"NX"};
  CmdArgParser parser(ArgSlice(args, 1));
  auto result = parser.TryMapNext("NX", 100, "XX", 200);
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(*result, 100);
}

TEST(CmdArgParserTest, TryMapNextNoMatch) {
  std::string_view args[] = {"unknown"};
  CmdArgParser parser(ArgSlice(args, 1));
  auto result = parser.TryMapNext("NX", 100, "XX", 200);
  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(parser.HasError());  // TryMapNext does NOT report error
}

// ============================================================================
// Chained usage
// ============================================================================

TEST(CmdArgParserTest, ChainedSkipAndCheck) {
  std::string_view args[] = {"SET", "key", "value", "NX", "EX", "10"};
  CmdArgParser parser(ArgSlice(args, 6));

  auto cmd = parser.Next<std::string_view>();
  EXPECT_EQ(cmd, "SET");

  auto key = parser.Next<std::string_view>();
  EXPECT_EQ(key, "key");

  auto val = parser.Next<std::string_view>();
  EXPECT_EQ(val, "value");

  bool has_nx = parser.Check("NX");
  EXPECT_TRUE(has_nx);

  bool has_ex = parser.Check("EX");
  EXPECT_TRUE(has_ex);

  auto ttl = parser.NextOrDefault<int64_t>(0);
  EXPECT_EQ(ttl, 10);

  EXPECT_FALSE(parser.HasNext());
}

// ============================================================================
// GetCurrentIndex
// ============================================================================

TEST(CmdArgParserTest, GetCurrentIndex) {
  std::string_view args[] = {"a", "b", "c"};
  CmdArgParser parser(ArgSlice(args, 3));
  EXPECT_EQ(parser.GetCurrentIndex(), 0u);
  (void)parser.Next<std::string_view>();
  EXPECT_EQ(parser.GetCurrentIndex(), 1u);
  (void)parser.Next<std::string_view>();
  EXPECT_EQ(parser.GetCurrentIndex(), 2u);
}

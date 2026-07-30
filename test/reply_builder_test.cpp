#include "redis/facade/reply_builder.hpp"

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

using namespace dfly;

// ============================================================================
// ReplyBuilder tests
// ============================================================================

namespace {

// Helper: capture the output sent via callback
struct Capture {
  std::string last;
  void reset() { last.clear(); }
  ReplyBuilder::SendCallback cb() {
    return [this](std::string&& s) { last = std::move(s); };
  }
};

}  // namespace

// ---- BuildNull ----

TEST(ReplyBuilderTest, BuildNull) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildNull();
  EXPECT_EQ(cap.last, "$-1\r\n");
}

// ---- BuildSimpleString ----

TEST(ReplyBuilderTest, BuildSimpleString) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildSimpleString("OK");
  EXPECT_EQ(cap.last, "+OK\r\n");
}

TEST(ReplyBuilderTest, BuildSimpleStringEmpty) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildSimpleString("");
  EXPECT_EQ(cap.last, "+\r\n");
}

// ---- BuildError ----

TEST(ReplyBuilderTest, BuildError) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildError("something went wrong");
  EXPECT_EQ(cap.last, "-ERR something went wrong\r\n");
}

TEST(ReplyBuilderTest, BuildErrorEmpty) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildError("");
  EXPECT_EQ(cap.last, "-ERR \r\n");
}

// ---- BuildInteger ----

TEST(ReplyBuilderTest, BuildInteger) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildInteger(42);
  EXPECT_EQ(cap.last, ":42\r\n");
}

TEST(ReplyBuilderTest, BuildIntegerNegative) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildInteger(-7);
  EXPECT_EQ(cap.last, ":-7\r\n");
}

TEST(ReplyBuilderTest, BuildIntegerZero) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildInteger(0);
  EXPECT_EQ(cap.last, ":0\r\n");
}

// ---- BuildBulkString ----

TEST(ReplyBuilderTest, BuildBulkString) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildBulkString("hello");
  EXPECT_EQ(cap.last, "$5\r\nhello\r\n");
}

TEST(ReplyBuilderTest, BuildBulkStringEmpty) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildBulkString("");
  EXPECT_EQ(cap.last, "$0\r\n\r\n");
}

// ---- BuildNullBulkString ----

TEST(ReplyBuilderTest, BuildNullBulkString) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildNullBulkString();
  EXPECT_EQ(cap.last, "$-1\r\n");
}

// ---- BuildArray ----

TEST(ReplyBuilderTest, BuildArray) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildArray({"a", "bb", "ccc"});
  EXPECT_EQ(cap.last, "*3\r\n$1\r\na\r\n$2\r\nbb\r\n$3\r\nccc\r\n");
}

TEST(ReplyBuilderTest, BuildArraySingle) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildArray({"only"});
  EXPECT_EQ(cap.last, "*1\r\n$4\r\nonly\r\n");
}

TEST(ReplyBuilderTest, BuildArrayEmpty) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildArray({});
  EXPECT_EQ(cap.last, "*0\r\n");
}

// ---- BuildMultiArray ----

TEST(ReplyBuilderTest, BuildMultiArray) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildMultiArray({":1\r\n", ":2\r\n", ":3\r\n"});
  EXPECT_EQ(cap.last, "*3\r\n:1\r\n:2\r\n:3\r\n");
}

TEST(ReplyBuilderTest, BuildMultiArrayEmpty) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildMultiArray({});
  EXPECT_EQ(cap.last, "*0\r\n");
}

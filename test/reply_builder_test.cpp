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

struct Capture {
  std::string last;
  void reset() { last.clear(); }
  ReplyBuilder::SendCallback cb() {
    return [this](std::vector<std::string>&& v) {
      last = v.empty() ? std::string() : std::move(v.back());
    };
  }
};

}  // namespace

TEST(ReplyBuilderTest, BuildSimpleString) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildSimpleString("OK");
  r.Flush();
  EXPECT_EQ(cap.last, "+OK\r\n");
}

TEST(ReplyBuilderTest, BuildSimpleStringEmpty) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildSimpleString("");
  r.Flush();
  EXPECT_EQ(cap.last, "+\r\n");
}

TEST(ReplyBuilderTest, BuildError) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildError("something went wrong");
  r.Flush();
  EXPECT_EQ(cap.last, "-ERR something went wrong\r\n");
}

TEST(ReplyBuilderTest, BuildErrorEmpty) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildError("");
  r.Flush();
  EXPECT_EQ(cap.last, "-ERR \r\n");
}

TEST(ReplyBuilderTest, BuildInteger) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildInteger(42);
  r.Flush();
  EXPECT_EQ(cap.last, ":42\r\n");
}

TEST(ReplyBuilderTest, BuildIntegerNegative) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildInteger(-7);
  r.Flush();
  EXPECT_EQ(cap.last, ":-7\r\n");
}

TEST(ReplyBuilderTest, BuildIntegerZero) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildInteger(0);
  r.Flush();
  EXPECT_EQ(cap.last, ":0\r\n");
}

TEST(ReplyBuilderTest, BuildBulkString) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildBulkString("hello");
  r.Flush();
  EXPECT_EQ(cap.last, "$5\r\nhello\r\n");
}

TEST(ReplyBuilderTest, BuildBulkStringEmpty) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildBulkString("");
  r.Flush();
  EXPECT_EQ(cap.last, "$0\r\n\r\n");
}

// ---- BuildNullBulkString ----

TEST(ReplyBuilderTest, BuildNullBulkString) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildNullBulkString();
  r.Flush();
  EXPECT_EQ(cap.last, "$-1\r\n");
}

// ---- BuildArray ----

TEST(ReplyBuilderTest, BuildArray) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildArray({"a", "bb", "ccc"});
  r.Flush();
  EXPECT_EQ(cap.last, "*3\r\n$1\r\na\r\n$2\r\nbb\r\n$3\r\nccc\r\n");
}

TEST(ReplyBuilderTest, BuildArraySingle) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildArray({"only"});
  r.Flush();
  EXPECT_EQ(cap.last, "*1\r\n$4\r\nonly\r\n");
}

TEST(ReplyBuilderTest, BuildArrayEmpty) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.BuildArray({});
  r.Flush();
  EXPECT_EQ(cap.last, "*0\r\n");
}

// ---- SendRaw（PipelineSquasher 回放路径） ----

TEST(ReplyBuilderTest, SendRaw) {
  Capture cap;
  ReplyBuilder r;
  r.SetSendCallback(cap.cb());
  r.SendRaw("*3\r\n:1\r\n:2\r\n:3\r\n");
  r.Flush();
  EXPECT_EQ(cap.last, "*3\r\n:1\r\n:2\r\n:3\r\n");
}

// ---- 回调捕获（squash 收集路径：回调绑定到收集器） ----

TEST(ReplyBuilderTest, CallbackCaptureCollectsMultipleReplies) {
  std::vector<std::string> out;
  ReplyBuilder r;
  r.SetSendCallback([&out](std::vector<std::string>&& v) {
    for (auto& s : v) out.push_back(std::move(s));
  });

  r.BuildSimpleString("OK");
  r.BuildInteger(42);
  r.BuildArray({"a", "b"});
  r.Flush();
  ASSERT_EQ(out.size(), 3u);
  EXPECT_EQ(out[0], "+OK\r\n");
  EXPECT_EQ(out[1], ":42\r\n");
  EXPECT_EQ(out[2], "*2\r\n$1\r\na\r\n$1\r\nb\r\n");
}

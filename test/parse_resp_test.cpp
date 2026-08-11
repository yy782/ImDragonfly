#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "redis/facade/ParseRESP.hpp"

using namespace dfly;

// ============================================================================
// ParseRESP tests — 验证 RESP 协议解析的正确性
// ============================================================================

// ---------- 正常场景 ----------

TEST(ParseRESPTest, SetCommand) {
  std::string buf = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  ASSERT_EQ(r.size(), 3u);
  EXPECT_EQ(r[0], "SET");
  EXPECT_EQ(r[1], "key");
  EXPECT_EQ(r[2], "value");
}

TEST(ParseRESPTest, GetCommand) {
  std::string buf = "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  ASSERT_EQ(r.size(), 2u);
  EXPECT_EQ(r[0], "GET");
  EXPECT_EQ(r[1], "key");
}

TEST(ParseRESPTest, DelCommand) {
  std::string buf = "*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  ASSERT_EQ(r.size(), 2u);
  EXPECT_EQ(r[0], "DEL");
  EXPECT_EQ(r[1], "key");
}

TEST(ParseRESPTest, PingCommand) {
  std::string buf = "*1\r\n$4\r\nPING\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  ASSERT_EQ(r.size(), 1u);
  EXPECT_EQ(r[0], "PING");
}

TEST(ParseRESPTest, MsetCommand) {
  std::string buf =
      "*5\r\n$4\r\nMSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  ASSERT_EQ(r.size(), 5u);
  EXPECT_EQ(r[0], "MSET");
  EXPECT_EQ(r[1], "k1");
  EXPECT_EQ(r[2], "v1");
  EXPECT_EQ(r[3], "k2");
  EXPECT_EQ(r[4], "v2");
}

TEST(ParseRESPTest, MgetCommand) {
  std::string buf = "*4\r\n$4\r\nMGET\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\nk3\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  ASSERT_EQ(r.size(), 4u);
  EXPECT_EQ(r[0], "MGET");
  EXPECT_EQ(r[1], "k1");
  EXPECT_EQ(r[2], "k2");
  EXPECT_EQ(r[3], "k3");
}

TEST(ParseRESPTest, EmptyArray) {
  std::string buf = "*0\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  EXPECT_EQ(r.size(), 0u);
}

TEST(ParseRESPTest, EmptyBulkStringElement) {
  std::string buf = "*2\r\n$5\r\nLPUSH\r\n$0\r\n\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  ASSERT_EQ(r.size(), 2u);
  EXPECT_EQ(r[0], "LPUSH");
  EXPECT_EQ(r[1], "");
}

TEST(ParseRESPTest, LongValue) {
  std::string val(1024, 'x');
  std::string buf = "*2\r\n$3\r\nSET\r\n$" + std::to_string(val.size()) +
                    "\r\n" + val + "\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  ASSERT_EQ(r.size(), 2u);
  EXPECT_EQ(r[0], "SET");
  EXPECT_EQ(r[1].size(), 1024u);
}

TEST(ParseRESPTest, LargeArray) {
  std::string buf = "*100\r\n";
  for (int i = 0; i < 100; i++) {
    std::string elem = std::to_string(i);
    buf += "$" + std::to_string(elem.size()) + "\r\n" + elem + "\r\n";
  }
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  ASSERT_EQ(r.size(), 100u);
  for (int i = 0; i < 100; i++) {
    EXPECT_EQ(r[i], std::to_string(i));
  }
}

TEST(ParseRESPTest, MultiDigitBulkLength) {
  std::string payload(1234, 'a');
  std::string buf = "*1\r\n$1234\r\n" + payload + "\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  ASSERT_EQ(r.size(), 1u);
  EXPECT_EQ(r[0].size(), 1234u);
}

// ---------- 连续解析 / 复用 parser ----------

TEST(ParseRESPTest, ConsecutiveParsing) {
  ParseRESP parser;

  std::string buf1 = "*2\r\n$3\r\nGET\r\n$1\r\na\r\n";
  auto& r1 = parser.Parse(buf1.data(), buf1.size());
  ASSERT_EQ(r1.size(), 2u);
  EXPECT_EQ(r1[0], "GET");
  EXPECT_EQ(r1[1], "a");

  std::string buf2 = "*2\r\n$3\r\nGET\r\n$1\r\nb\r\n";
  auto& r2 = parser.Parse(buf2.data(), buf2.size());
  ASSERT_EQ(r2.size(), 2u);
  EXPECT_EQ(r2[0], "GET");
  EXPECT_EQ(r2[1], "b");
}

// ---------- 非法 / 不完整输入 — 应返回空 ----------

TEST(ParseRESPTest, EmptyBuffer) {
  ParseRESP parser;
  auto& r = parser.Parse("", 0);
  EXPECT_EQ(r.size(), 0u);
}

TEST(ParseRESPTest, OnlyAsterisk) {
  std::string buf = "*";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  EXPECT_EQ(r.size(), 0u);
}

TEST(ParseRESPTest, NotArraySimpleString) {
  std::string buf = "+OK\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  EXPECT_EQ(r.size(), 0u);
}

TEST(ParseRESPTest, NotArrayInteger) {
  std::string buf = ":42\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  EXPECT_EQ(r.size(), 0u);
}

TEST(ParseRESPTest, NonDigitCount) {
  std::string buf = "*x\r\n$3\r\nFOO\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  EXPECT_EQ(r.size(), 0u);
}

TEST(ParseRESPTest, NegativeCount) {
  std::string buf = "*-1\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  EXPECT_EQ(r.size(), 0u);
}

TEST(ParseRESPTest, MissingCrLfAfterCount) {
  std::string buf = "*3X\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  EXPECT_EQ(r.size(), 0u);
}

TEST(ParseRESPTest, NoCrLfAfterCount) {
  std::string buf = "*3";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  EXPECT_EQ(r.size(), 0u);
}

TEST(ParseRESPTest, CountWithPartialCrLf) {
  std::string buf = "*3\r";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  EXPECT_EQ(r.size(), 0u);
}

// ---------- 不完整 bulk string ----------

TEST(ParseRESPTest, IncompleteBulkHeaderNoCrLf) {
  std::string buf = "*1\r\n$3";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  EXPECT_EQ(r.size(), 0u);
}

TEST(ParseRESPTest, IncompleteBulkHeaderPartialCrLf) {
  std::string buf = "*1\r\n$3\r";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  EXPECT_EQ(r.size(), 0u);
}

TEST(ParseRESPTest, IncompleteBulkDataShort) {
  std::string buf = "*1\r\n$5\r\nab\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  EXPECT_EQ(r.size(), 0u);
}

TEST(ParseRESPTest, IncompleteBulkDataMissingCrLf) {
  std::string buf = "*1\r\n$3\r\nfoo";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  EXPECT_EQ(r.size(), 0u);
}

TEST(ParseRESPTest, IncompleteBulkDataPartialCrLf) {
  std::string buf = "*1\r\n$3\r\nfoo\r";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  EXPECT_EQ(r.size(), 0u);
}

TEST(ParseRESPTest, NonDigitBulkLength) {
  std::string buf = "*1\r\n$abc\r\n";
  ParseRESP parser;
  auto& r = parser.Parse(buf.data(), buf.size());
  EXPECT_EQ(r.size(), 0u);
}

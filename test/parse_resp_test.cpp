#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "redis/facade/ParseRESP.hpp"

using namespace dfly;

// ============================================================================
// ParseRESP tests — 验证 RESP 协议解析（生产路径 ParseAll）的正确性
// ============================================================================

namespace {

// ParseAll 后断言恰好解析出一条命令，返回其参数列表。
std::vector<std::string_view> SingleCmd(ParseRESP& p, const char* data,
                                        size_t bytes) {
  auto pr = p.ParseAll(data, bytes);
  EXPECT_EQ(pr.cmds.size(), 1u);
  return pr.cmds.empty() ? std::vector<std::string_view>{} : pr.cmds[0];
}

// ParseAll 后断言没有解析出任何命令（格式错误或空命令）。
void ExpectNoCmd(ParseRESP& p, const char* data, size_t bytes) {
  auto pr = p.ParseAll(data, bytes);
  EXPECT_TRUE(pr.cmds.empty());
}

// ParseAll 后断言是半包：无命令，且 partial_offset 指向期望的保留偏移。
void ExpectHalfPacket(ParseRESP& p, const char* data, size_t bytes,
                      size_t offset) {
  auto pr = p.ParseAll(data, bytes);
  EXPECT_TRUE(pr.cmds.empty());
  EXPECT_EQ(pr.partial_offset, offset);
}

}  // namespace

// ---------- 正常场景 ----------

TEST(ParseRESPTest, SetCommand) {
  std::string buf = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
  ParseRESP parser;
  auto r = SingleCmd(parser, buf.data(), buf.size());
  ASSERT_EQ(r.size(), 3u);
  EXPECT_EQ(r[0], "SET");
  EXPECT_EQ(r[1], "key");
  EXPECT_EQ(r[2], "value");
}

TEST(ParseRESPTest, GetCommand) {
  std::string buf = "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
  ParseRESP parser;
  auto r = SingleCmd(parser, buf.data(), buf.size());
  ASSERT_EQ(r.size(), 2u);
  EXPECT_EQ(r[0], "GET");
  EXPECT_EQ(r[1], "key");
}

TEST(ParseRESPTest, DelCommand) {
  std::string buf = "*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
  ParseRESP parser;
  auto r = SingleCmd(parser, buf.data(), buf.size());
  ASSERT_EQ(r.size(), 2u);
  EXPECT_EQ(r[0], "DEL");
  EXPECT_EQ(r[1], "key");
}

TEST(ParseRESPTest, PingCommand) {
  std::string buf = "*1\r\n$4\r\nPING\r\n";
  ParseRESP parser;
  auto r = SingleCmd(parser, buf.data(), buf.size());
  ASSERT_EQ(r.size(), 1u);
  EXPECT_EQ(r[0], "PING");
}

TEST(ParseRESPTest, MsetCommand) {
  std::string buf =
      "*5\r\n$4\r\nMSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n";
  ParseRESP parser;
  auto r = SingleCmd(parser, buf.data(), buf.size());
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
  auto r = SingleCmd(parser, buf.data(), buf.size());
  ASSERT_EQ(r.size(), 4u);
  EXPECT_EQ(r[0], "MGET");
  EXPECT_EQ(r[1], "k1");
  EXPECT_EQ(r[2], "k2");
  EXPECT_EQ(r[3], "k3");
}

// *0\r\n 是合法 RESP 空数组：应被正确消费（partial_offset == bytes），
// 但不产生可执行命令。
TEST(ParseRESPTest, EmptyArrayIsDropped) {
  std::string buf = "*0\r\n";
  ParseRESP parser;
  auto pr = parser.ParseAll(buf.data(), buf.size());
  EXPECT_TRUE(pr.cmds.empty());
  EXPECT_EQ(pr.partial_offset, buf.size());
}

TEST(ParseRESPTest, EmptyBulkStringElement) {
  std::string buf = "*2\r\n$5\r\nLPUSH\r\n$0\r\n\r\n";
  ParseRESP parser;
  auto r = SingleCmd(parser, buf.data(), buf.size());
  ASSERT_EQ(r.size(), 2u);
  EXPECT_EQ(r[0], "LPUSH");
  EXPECT_EQ(r[1], "");
}

TEST(ParseRESPTest, LongValue) {
  std::string val(1024, 'x');
  std::string buf = "*2\r\n$3\r\nSET\r\n$" + std::to_string(val.size()) +
                    "\r\n" + val + "\r\n";
  ParseRESP parser;
  auto r = SingleCmd(parser, buf.data(), buf.size());
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
  auto r = SingleCmd(parser, buf.data(), buf.size());
  ASSERT_EQ(r.size(), 100u);
  for (int i = 0; i < 100; i++) {
    EXPECT_EQ(r[i], std::to_string(i));
  }
}

TEST(ParseRESPTest, MultiDigitBulkLength) {
  std::string payload(1234, 'a');
  std::string buf = "*1\r\n$1234\r\n" + payload + "\r\n";
  ParseRESP parser;
  auto r = SingleCmd(parser, buf.data(), buf.size());
  ASSERT_EQ(r.size(), 1u);
  EXPECT_EQ(r[0].size(), 1234u);
}

// ---------- 连续解析 / 复用 parser ----------

TEST(ParseRESPTest, ConsecutiveParsing) {
  ParseRESP parser;

  std::string buf1 = "*2\r\n$3\r\nGET\r\n$1\r\na\r\n";
  auto r1 = SingleCmd(parser, buf1.data(), buf1.size());
  ASSERT_EQ(r1.size(), 2u);
  EXPECT_EQ(r1[0], "GET");
  EXPECT_EQ(r1[1], "a");

  std::string buf2 = "*2\r\n$3\r\nGET\r\n$1\r\nb\r\n";
  auto r2 = SingleCmd(parser, buf2.data(), buf2.size());
  ASSERT_EQ(r2.size(), 2u);
  EXPECT_EQ(r2[0], "GET");
  EXPECT_EQ(r2[1], "b");
}

// ---------- 批量解析（ParseAll 核心能力） ----------

TEST(ParseRESPTest, MultiCommandBatch) {
  std::string buf = "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n";
  ParseRESP parser;
  auto pr = parser.ParseAll(buf.data(), buf.size());
  ASSERT_EQ(pr.cmds.size(), 2u);
  EXPECT_EQ(pr.cmds[0][0], "PING");
  ASSERT_EQ(pr.cmds[1].size(), 2u);
  EXPECT_EQ(pr.cmds[1][0], "GET");
  EXPECT_EQ(pr.cmds[1][1], "a");
  EXPECT_EQ(pr.partial_offset, buf.size());
}

// 批量中夹带 *0\r\n 空命令：空命令被丢弃，不影响后续命令解析。
TEST(ParseRESPTest, EmptyArrayInBatchIsDropped) {
  std::string buf = "*0\r\n*1\r\n$4\r\nPING\r\n";
  ParseRESP parser;
  auto pr = parser.ParseAll(buf.data(), buf.size());
  ASSERT_EQ(pr.cmds.size(), 1u);
  EXPECT_EQ(pr.cmds[0][0], "PING");
  EXPECT_EQ(pr.partial_offset, buf.size());
}

// 完整命令 + 尾部半包：解析出完整命令，partial_offset 指向半包起始处。
TEST(ParseRESPTest, BatchWithTrailingPartial) {
  // PING（14 字节）后接一条缺尾的 GET。
  std::string buf = "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\na";
  ParseRESP parser;
  auto pr = parser.ParseAll(buf.data(), buf.size());
  ASSERT_EQ(pr.cmds.size(), 1u);
  EXPECT_EQ(pr.cmds[0][0], "PING");
  EXPECT_EQ(pr.partial_offset, 14u);
}

// 前导格式错误字节被跳过，后续完整命令仍能解析出。
TEST(ParseRESPTest, FormatErrorSkippedThenResume) {
  std::string buf = "+BAD\r\n*1\r\n$4\r\nPING\r\n";
  ParseRESP parser;
  auto pr = parser.ParseAll(buf.data(), buf.size());
  ASSERT_EQ(pr.cmds.size(), 1u);
  EXPECT_EQ(pr.cmds[0][0], "PING");
  EXPECT_EQ(pr.partial_offset, buf.size());
}

// ---------- 非法 / 不完整输入 — 应无命令 ----------

TEST(ParseRESPTest, EmptyBuffer) {
  ParseRESP parser;
  ExpectNoCmd(parser, "", 0);
}

TEST(ParseRESPTest, OnlyAsterisk) {
  std::string buf = "*";
  ParseRESP parser;
  ExpectHalfPacket(parser, buf.data(), buf.size(), 0);
}

TEST(ParseRESPTest, NotArraySimpleString) {
  std::string buf = "+OK\r\n";
  ParseRESP parser;
  ExpectNoCmd(parser, buf.data(), buf.size());
}

TEST(ParseRESPTest, NotArrayInteger) {
  std::string buf = ":42\r\n";
  ParseRESP parser;
  ExpectNoCmd(parser, buf.data(), buf.size());
}

TEST(ParseRESPTest, NonDigitCount) {
  std::string buf = "*x\r\n$3\r\nFOO\r\n";
  ParseRESP parser;
  ExpectNoCmd(parser, buf.data(), buf.size());
}

TEST(ParseRESPTest, NegativeCount) {
  std::string buf = "*-1\r\n";
  ParseRESP parser;
  ExpectNoCmd(parser, buf.data(), buf.size());
}

TEST(ParseRESPTest, MissingCrLfAfterCount) {
  std::string buf = "*3X\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
  ParseRESP parser;
  ExpectNoCmd(parser, buf.data(), buf.size());
}

TEST(ParseRESPTest, NoCrLfAfterCount) {
  std::string buf = "*3";
  ParseRESP parser;
  ExpectHalfPacket(parser, buf.data(), buf.size(), 0);
}

TEST(ParseRESPTest, CountWithPartialCrLf) {
  std::string buf = "*3\r";
  ParseRESP parser;
  ExpectHalfPacket(parser, buf.data(), buf.size(), 0);
}

// ---------- 不完整 bulk string ----------

TEST(ParseRESPTest, IncompleteBulkHeaderNoCrLf) {
  std::string buf = "*1\r\n$3";
  ParseRESP parser;
  ExpectHalfPacket(parser, buf.data(), buf.size(), 0);
}

TEST(ParseRESPTest, IncompleteBulkHeaderPartialCrLf) {
  std::string buf = "*1\r\n$3\r";
  ParseRESP parser;
  ExpectHalfPacket(parser, buf.data(), buf.size(), 0);
}

TEST(ParseRESPTest, IncompleteBulkDataShort) {
  std::string buf = "*1\r\n$5\r\nab\r\n";
  ParseRESP parser;
  ExpectHalfPacket(parser, buf.data(), buf.size(), 0);
}

TEST(ParseRESPTest, IncompleteBulkDataMissingCrLf) {
  std::string buf = "*1\r\n$3\r\nfoo";
  ParseRESP parser;
  ExpectHalfPacket(parser, buf.data(), buf.size(), 0);
}

TEST(ParseRESPTest, IncompleteBulkDataPartialCrLf) {
  std::string buf = "*1\r\n$3\r\nfoo\r";
  ParseRESP parser;
  ExpectHalfPacket(parser, buf.data(), buf.size(), 0);
}

TEST(ParseRESPTest, NonDigitBulkLength) {
  std::string buf = "*1\r\n$abc\r\n";
  ParseRESP parser;
  ExpectNoCmd(parser, buf.data(), buf.size());
}

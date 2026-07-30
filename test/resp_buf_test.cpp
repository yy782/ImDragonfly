#include "redis/facade/resp_buf.hpp"

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "YY/net/TcpBuffer.h"

// ============================================================================
// RESP_Buf: ParseRESP tests
// ============================================================================

namespace {

// Helper: write RESP bulk string array into TcpBuffer and parse it.
// Uses only public TcpBuffer API: reset() / append() / readable_size()
dfly::RESP_Buf parser;

std::vector<std::string_view> ParseRespString(const std::string& raw) {
  yy::net::TcpBuffer buf(4096);
  buf.reset();                         // reset indices to 8
  buf.append(raw.data(), raw.size());  // write data, advances write_index_
  return parser.ParseRESP(buf);
}

std::string MakeArray(const std::vector<std::string>& elements) {
  std::string r;
  r += "*" + std::to_string(elements.size()) + "\r\n";
  for (const auto& e : elements) {
    r += "$" + std::to_string(e.size()) + "\r\n" + e + "\r\n";
  }
  return r;
}

}  // namespace

TEST(RESP_BufTest, ParseSimpleArray) {
  auto result = ParseRespString(MakeArray({"hello", "world"}));
  ASSERT_EQ(result.size(), 2u);
  EXPECT_EQ(result[0], "hello");
  EXPECT_EQ(result[1], "world");
}

TEST(RESP_BufTest, ParseSingleElement) {
  auto result = ParseRespString(MakeArray({"single"}));
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0], "single");
}

TEST(RESP_BufTest, ParseEmptyArray) {
  auto result = ParseRespString("*0\r\n");
  EXPECT_EQ(result.size(), 0u);
}

TEST(RESP_BufTest, ParseNumericElements) {
  auto result = ParseRespString(MakeArray({"100", "200", "300"}));
  ASSERT_EQ(result.size(), 3u);
  EXPECT_EQ(result[0], "100");
  EXPECT_EQ(result[1], "200");
  EXPECT_EQ(result[2], "300");
}

TEST(RESP_BufTest, ParseEmptyStringElement) {
  auto result = ParseRespString(MakeArray({""}));
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0], "");
}

TEST(RESP_BufTest, ParseManyElements) {
  std::vector<std::string> elems;
  for (int i = 0; i < 100; ++i) {
    elems.push_back("elem_" + std::to_string(i));
  }
  auto result = ParseRespString(MakeArray(elems));
  ASSERT_EQ(result.size(), 100u);
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(result[i], elems[i]);
  }
}

TEST(RESP_BufTest, InvalidFormatNoAsterisk) {
  yy::net::TcpBuffer buf(64);
  buf.reset();
  const char* raw = "+OK\r\n";
  buf.append(raw, 5);

  dfly::RESP_Buf p;
  auto result = p.ParseRESP(buf);
  EXPECT_TRUE(result.empty());
}

TEST(RESP_BufTest, InvalidFormatBadLength) {
  yy::net::TcpBuffer buf(64);
  buf.reset();
  // Missing length after $
  const char* raw = "*1\r\n$\r\n";
  buf.append(raw, 7);

  dfly::RESP_Buf p;
  auto result = p.ParseRESP(buf);
  EXPECT_TRUE(result.empty());
}

TEST(RESP_BufTest, IncompleteData) {
  yy::net::TcpBuffer buf(64);
  buf.reset();
  // Only *3\r\n but no actual elements
  const char* raw = "*3\r\n";
  buf.append(raw, 4);

  dfly::RESP_Buf p;
  auto result = p.ParseRESP(buf);
  EXPECT_TRUE(result.empty());
}

TEST(RESP_BufTest, NegativeElementCount) {
  yy::net::TcpBuffer buf(64);
  buf.reset();
  const char* raw = "*-1\r\n";
  buf.append(raw, 5);

  dfly::RESP_Buf p;
  auto result = p.ParseRESP(buf);
  EXPECT_TRUE(result.empty());
}

TEST(RESP_BufTest, BufferConsumedAfterParse) {
  yy::net::TcpBuffer buf(256);
  buf.reset();
  std::string raw = MakeArray({"a", "b"});
  buf.append(raw.data(), raw.size());

  dfly::RESP_Buf p;
  auto result = p.ParseRESP(buf);
  EXPECT_EQ(result.size(), 2u);
  // After successful parse, all data should be consumed
  EXPECT_EQ(buf.readable_size(), 0u);
}

TEST(RESP_BufTest, ParseThenReuseParser) {
  {
    auto result = ParseRespString(MakeArray({"first"}));
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0], "first");
  }

  {
    auto result = ParseRespString(MakeArray({"second"}));
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0], "second");
  }
}

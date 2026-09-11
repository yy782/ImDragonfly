#include "raft/raft_log_entry.hpp"

#include <immintrin.h>

#include <cstring>

namespace dfly {

std::string EncodeRespCommand(::dfly::CmdArgList args) {
  size_t cap = 16;
  for (const Arg a : args) cap += a.size() + 16;

  std::string out;
  out.reserve(cap);

  out += '*';
  out += std::to_string(args.size());
  out += "\r\n";
  for (const Arg a : args) {
    out += '$';
    out += std::to_string(a.size());
    out += "\r\n";
    out.append(a);
    out += "\r\n";
  }
  return out;
}

// 同上，但从 std::string 列表编码（raft 内部生成成员变更条目时用）。
std::string EncodeRespCommandFromStrings(const std::vector<std::string>& args) {
  size_t cap = 16;
  for (const auto& a : args) cap += a.size() + 16;

  std::string out;
  out.reserve(cap);

  out += '*';
  out += std::to_string(args.size());
  out += "\r\n";
  for (const auto& a : args) {
    out += '$';
    out += std::to_string(a.size());
    out += "\r\n";
    out += a;
    out += "\r\n";
  }
  return out;
}

uint32_t RaftCrc32(uint64_t index, uint64_t term, uint64_t start_ms,
                   std::string_view payload) {
  uint64_t crc = ~uint64_t{0};
  crc = _mm_crc32_u64(crc, index);
  crc = _mm_crc32_u64(crc, term);
  crc = _mm_crc32_u64(crc, start_ms);

  const char* p = payload.data();
  size_t n = payload.size();
  while (n >= 8) {
    uint64_t v;
    std::memcpy(&v, p, 8);
    crc = _mm_crc32_u64(crc, v);
    p += 8;
    n -= 8;
  }
  uint32_t c32 = static_cast<uint32_t>(crc);
  while (n-- > 0) c32 = _mm_crc32_u8(c32, static_cast<uint8_t>(*p++));
  return ~c32;
}

uint32_t RaftCrc32Raw(std::string_view data) {
  uint64_t crc = ~uint64_t{0};
  const char* p = data.data();
  size_t n = data.size();
  while (n >= 8) {
    uint64_t v;
    std::memcpy(&v, p, 8);
    crc = _mm_crc32_u64(crc, v);
    p += 8;
    n -= 8;
  }
  uint32_t c32 = static_cast<uint32_t>(crc);
  while (n-- > 0) c32 = _mm_crc32_u8(c32, static_cast<uint8_t>(*p++));
  return ~c32;
}

namespace {

template <typename T>
void PutLE(std::string* out, T v) {
  char buf[sizeof(T)];
  std::memcpy(buf, &v, sizeof(T));  // x86 天然小端
  out->append(buf, sizeof(T));
}

}  // namespace

void AppendRecord(const RaftLogEntry& e, std::string* out) {
  const uint32_t crc = RaftCrc32(e.index, e.term, e.start_ms, e.payload);

  out->reserve(out->size() + kRaftRecordHeaderSize + e.payload.size());
  PutLE<uint32_t>(out, static_cast<uint32_t>(e.payload.size()));
  PutLE<uint64_t>(out, e.index);
  PutLE<uint64_t>(out, e.term);
  PutLE<uint64_t>(out, e.start_ms);
  PutLE<uint32_t>(out, crc);
  out->append(e.payload);
}

}  // namespace dfly
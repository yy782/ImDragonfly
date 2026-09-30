#include "raft/raft_log_entry.hpp"

#include <immintrin.h>

#include <cstring>

namespace dfly {

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
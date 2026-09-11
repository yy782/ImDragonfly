#include "raft/raft_proto.hpp"

#include <cstring>

namespace dfly {

void PutU8(std::string* out, uint8_t v) {
  out->push_back(static_cast<char>(v));
}

void PutU32(std::string* out, uint32_t v) {
  char buf[4];
  std::memcpy(buf, &v, 4);
  out->append(buf, 4);
}

void PutU64(std::string* out, uint64_t v) {
  char buf[8];
  std::memcpy(buf, &v, 8);
  out->append(buf, 8);
}

uint8_t GetU8(const char* p) { return static_cast<uint8_t>(*p); }

uint32_t GetU32(const char* p) {
  uint32_t v;
  std::memcpy(&v, p, 4);
  return v;
}

uint64_t GetU64(const char* p) {
  uint64_t v;
  std::memcpy(&v, p, 8);
  return v;
}

void FrameMessage(std::string* out, RaftMsgType type, uint64_t seq,
                  std::string_view body) {
  const uint32_t len = static_cast<uint32_t>(1 + 8 + body.size());
  out->reserve(out->size() + kFrameLenSize + len);
  PutU32(out, len);
  PutU8(out, static_cast<uint8_t>(type));
  PutU64(out, seq);
  out->append(body);
}

}  // namespace dfly

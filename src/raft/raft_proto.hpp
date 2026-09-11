#pragma once

#include <netinet/in.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace dfly {

enum class RaftMsgType : uint8_t {
  kAppendEntries = 1,
  kAppendEntriesResp = 2,
  kRequestVote = 3,
  kRequestVoteResp = 4,
  kReadIndex = 5,
  kReadIndexResp = 6,
};

constexpr size_t kFrameLenSize = 4;
constexpr size_t kFrameHeaderSize = 4 + 1 + 8;
constexpr uint32_t kMaxFrameBody = 64u * 1024 * 1024;

constexpr size_t kAppendEntriesHeaderSize = 40;
constexpr size_t kEntryHeaderSize = 28;

struct AppendEntriesResp {
  uint64_t term = 0;
  bool success = false;
  uint64_t conflict_index = 0;
  uint64_t match_index = 0;
};

constexpr size_t kAppendRespBodySize = 25;

constexpr size_t kRequestVoteBodySize = 28;
constexpr size_t kVoteRespBodySize = 9;

struct RequestVoteResp {
  uint64_t term = 0;
  bool granted = false;
};

struct ReadIndexResp {
  uint64_t term = 0;
  bool success = false;
  uint64_t commit_index = 0;
};

constexpr size_t kReadIndexReqBodySize = 8;
constexpr size_t kReadIndexRespBodySize = 17;

void PutU8(std::string* out, uint8_t v);
void PutU32(std::string* out, uint32_t v);
void PutU64(std::string* out, uint64_t v);

uint8_t GetU8(const char* p);
uint32_t GetU32(const char* p);
uint64_t GetU64(const char* p);

void FrameMessage(std::string* out, RaftMsgType type, uint64_t seq,
                  std::string_view body);

}  // namespace dfly

// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "detail/cluster_support.hpp"

#include <glog/logging.h>

#include <cstdint>

namespace dfly {

namespace {

struct LockTagOptions {
  static LockTagOptions& instance() {
    static LockTagOptions opts;
    return opts;
  }
  bool enabled = false;

  std::string_view Tag(std::string_view key) const {
    auto start = key.find('{');
    if (start == std::string_view::npos) return key;
    auto end = key.find('}', start);
    if (end == std::string_view::npos) return key;
    return key.substr(start + 1, end - start - 1);
  }
};

// CRC16-XMODEM
uint16_t crc16(const char* data, size_t len) {
  uint16_t crc = 0;
  for (size_t i = 0; i < len; ++i) {
    crc ^= static_cast<uint16_t>(data[i]) << 8;
    for (int j = 0; j < 8; ++j) {
      crc = (crc & 0x8000) ? (crc << 1) ^ 0x1021 : crc << 1;
    }
  }
  return crc;
}
}  // namespace

void UniqueSlotChecker::Add(std::string_view key) {
  if (!IsClusterEnabled()) {
    return;
  }

  Add(KeySlot(key));
}

void UniqueSlotChecker::Add(SlotId slot_id) {
  if (!IsClusterEnabled()) {
    return;
  }

  if (slot_id_ == kNoSlotId) {
    slot_id_ = slot_id;
  } else if (slot_id_ != slot_id) {
    slot_id_ = kCrossSlot;
  }
}

std::optional<SlotId> UniqueSlotChecker::GetUniqueSlotId() const {
  return slot_id_ > kMaxSlotNum ? std::optional<SlotId>() : slot_id_;
}

using namespace detail;

void InitializeCluster() {
  cluster_mode = ClusterMode::kRealCluster;
  cluster_shard_by_slot = false;  // 不确定
}

SlotId KeySlot(std::string_view key) {
  std::string_view tag = LockTagOptions::instance().Tag(key);
  return crc16(tag.data(), tag.length()) & kMaxSlotNum;
}

bool IsClusterShardedByTag() {
  return IsClusterEnabledOrEmulated() || LockTagOptions::instance().enabled;
}

}  // namespace dfly

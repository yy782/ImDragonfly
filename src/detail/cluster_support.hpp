// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

#include "detail/common_types.hpp"

namespace dfly {

namespace detail {

enum class ClusterMode {
  kUninitialized,
  kNoCluster,
  kEmulatedCluster,
  kRealCluster,
};

inline ClusterMode cluster_mode = ClusterMode::kUninitialized;
inline bool cluster_shard_by_slot = false;

};  // namespace detail

constexpr SlotId kMaxSlotNum = 0x3FFF;

class UniqueSlotChecker {
 public:
  void Add(std::string_view key);
  void Add(SlotId slot_id);

  std::optional<SlotId> GetUniqueSlotId() const;

  bool IsCrossSlot() const { return slot_id_ == kCrossSlot; }

  void Reset() { slot_id_ = kNoSlotId; }

 private:
  static constexpr SlotId kNoSlotId = kMaxSlotNum + 1;
  static constexpr SlotId kCrossSlot = kNoSlotId + 1;

  SlotId slot_id_ = kNoSlotId;
};

SlotId KeySlot(std::string_view key);

void InitializeCluster();

inline bool IsClusterEnabled() {
  return detail::cluster_mode == detail::ClusterMode::kRealCluster;
}

inline bool IsClusterEmulated() {
  return detail::cluster_mode == detail::ClusterMode::kEmulatedCluster;
}

inline bool IsClusterEnabledOrEmulated() {
  return IsClusterEnabled() || IsClusterEmulated();
}

inline bool IsClusterShardedBySlot() { return detail::cluster_shard_by_slot; }

bool IsClusterShardedByTag();

}  // namespace dfly

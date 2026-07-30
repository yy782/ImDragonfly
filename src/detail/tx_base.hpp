// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once
#include <iostream>
#include <ranges>
#include <span>

#include "command_layer/cmn_types.hpp"
#include "detail/common_types.hpp"
#include "sharding/namespaces.hpp"
namespace dfly {

using LockFp = uint64_t;

struct KeyLockArgs {
  DbIndex db_index = 0;
  std::vector<LockFp> fps;
};

class LockTag {
  std::string_view str_;

 public:
  using is_stackonly = void;  // marks that this object does not use heap.

  LockTag() = default;
  explicit LockTag(std::string_view key);

  explicit operator std::string_view() const { return str_; }

  LockFp Fingerprint() const;

  // To make it hashable.
  template <typename H>
  friend H AbslHashValue(H h, const LockTag& tag) {
    return H::combine(std::move(h), tag.str_);
  }

  bool operator==(const LockTag& o) const { return str_ == o.str_; }
};

class DbContext {
 public:
  DbContext() = default;
  DbContext(const Namespace* ns, DbIndex index, uint64_t time_now_ms)
      : ns_(ns), db_index_(index), time_now_ms_(time_now_ms) {}
  DbContext(const DbContext& o) noexcept { *this = o; }
  DbContext& operator=(const DbContext& o) noexcept {
    ns_ = o.ns_;
    db_index_ = o.db_index_;
    time_now_ms_ = o.time_now_ms_;
    return *this;
  }
  DbSlice& GetDbSlice(ShardId shard_id) const {
    return ns_->GetDbSlice(shard_id);
  }
  const Namespace* GetNamespace() const { return ns_; }
  DbIndex GetDbIndex() const { return db_index_; }
  uint64_t GetTimeNowMs() const { return time_now_ms_; }

 private:
  const Namespace* ns_;
  DbIndex db_index_;
  uint64_t time_now_ms_;
};

struct KeyIndex {
  KeyIndex(unsigned start = 0, unsigned end = 0, unsigned step = 1)
      : start(start), end(end), step(step) {}

  using iterator_category = std::forward_iterator_tag;
  using value_type = unsigned;
  using difference_type = std::ptrdiff_t;
  using pointer = value_type;
  using reference = value_type;

  unsigned operator*() const;
  KeyIndex& operator++();
  bool operator!=(const KeyIndex& ki) const;

  unsigned NumArgs() const { return (end - start + step - 1) / step; }

  auto Range() const {
    unsigned s = start, e = end, st = step; // 由ASAN报告，2026.7.30 -- 1 修改
    return std::views::iota(0u, NumArgs()) |
           std::views::transform(
               [s, st](unsigned i) { return s + i * st; });
  }

  auto Range(const cmn::ArgSlice& args) const {
    unsigned s = start, e = end, st = step; // 由ASAN报告，2026.7.30 -- 1 修改
    return std::views::iota(0u, NumArgs()) |
           std::views::transform(
               [s, st](unsigned i) { return s + i * st; }) |
           std::views::transform(
               [args](unsigned idx) { return args[idx]; });
  }

 public:
  unsigned start, end, step;
};
}  // namespace dfly
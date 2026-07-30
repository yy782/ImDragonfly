// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/node_hash_map.h>

#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "detail/common_types.hpp"

namespace dfly {

class DbSlice;
class EngineShard;

class Namespace {
 public:
  Namespace();

  DbSlice& GetCurrentDbSlice() const;

  DbSlice& GetDbSlice(ShardId sid) const;

 private:
  std::vector<std::unique_ptr<DbSlice>> shard_db_slices_;

  friend class Namespaces;
};

class Namespaces {
 public:
  Namespaces();
  ~Namespaces();
  void init();
  void Clear();

  Namespace& GetDefaultNamespace() const;  // No locks 专用方法（无锁，高性能）
  Namespace& GetOrInsert(std::string_view ns);  // 方式2：用空字符串获取

 private:
  std::shared_mutex rw_mutex_;
  absl::node_hash_map<std::string, Namespace> namespaces_;
  Namespace* default_namespace_ = nullptr;
};

}  // namespace dfly

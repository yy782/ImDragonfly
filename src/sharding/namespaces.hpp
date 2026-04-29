// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common_types.hpp"
#include "util/fibers/synchronization.h"

namespace dfly {

// class BlockingController;
class DbSlice;
class EngineShard;

class Namespace {
 public:
  Namespace();

  DbSlice& GetCurrentDbSlice();

  DbSlice& GetDbSlice(ShardId sid);
  // BlockingController* GetOrAddBlockingController(EngineShard* shard);
  // BlockingController* GetBlockingController(ShardId sid);

 private:
  std::vector<std::unique_ptr<DbSlice>> shard_db_slices_;
  // std::vector<std::unique_ptr<BlockingController>> shard_blocking_controller_;

  friend class Namespaces;
};

class Namespaces {
 public:
  Namespaces();
  ~Namespaces();

  void Clear();  // Thread unsafe, use in tear-down or tests

  Namespace& GetDefaultNamespace() const;  // No locks 专用方法（无锁，高性能）
  Namespace& GetOrInsert(std::string_view ns); // 方式2：用空字符串获取

 private:
  util::fb2::SharedMutex mu_{};
  std::unordered_map<std::string, Namespace> namespaces_ ;
  Namespace* default_namespace_ = nullptr;
};

}  // namespace dfly

// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
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

  // 持读锁遍历全部 namespace（含默认命名空间，名为空串），回调收到
  // (ns_name, Namespace&)。调用方不得在回调内再对 namespaces 加写锁
  // （如 GetOrInsert），否则死锁。
  template <typename F>
  void ForEach(F&& f) {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    for (auto& [name, ns] : namespaces_) {
      f(name, ns);
    }
  }

 private:
  std::shared_mutex rw_mutex_;
  std::unordered_map<std::string, Namespace> namespaces_;
  Namespace* default_namespace_ = nullptr;
};

}  // namespace dfly

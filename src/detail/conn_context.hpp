// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include <absl/container/flat_hash_set.h>
#include <assert.h>

#include <atomic>

#include "cppcoro/task.hpp"
#include "detail/common.hpp"
#include "sharding/db_slice.hpp"
#include "sharding/engine_shard_set.hpp"
#include "redis/facade/reply_builder.hpp"
namespace dfly {

class Connection;
class Transaction;

class ConnectionContext {
 public:
  ConnectionContext() = default;
  ConnectionContext& operator=(const ConnectionContext& o) {
    owner_ = o.owner_;
    ns_ = o.ns_;
    index_ = o.index_;
    return *this;
  }
  ~ConnectionContext();
  RedisSessionPtr owner() const { return owner_; }
  RedisSessionPtr& owner() { return owner_; }

  template <typename Cb>
  void AddWatchKey(std::string_view key, Cb&& cb);

  const Namespace* GetNamespace() const { return ns_; }
  DbIndex GetDbIndex() const { return index_; }

 private:
  friend class RedisSession;
  ConnectionContext(RedisSessionPtr owner, Namespace* ns, DbIndex index)
      : owner_(owner), ns_(ns), index_(index) {}
  RedisSessionPtr owner_;
  Namespace* ns_;
  DbIndex index_;
};

class CommandId;

class CommandContext {
 public:
  CommandContext() = default;

  CommandContext(Transaction* transaction, const CommandId* cid, ReplyBuilder* reply_builder, int fd)
      : transaction_(transaction), cid_(cid), reply_builder_(reply_builder), fd_(fd) {
        assert(reply_builder_);
      }
  const CommandId* cid() const { return cid_; }
  Transaction* tx() const { return transaction_; }
  ReplyBuilder* rb() {
    assert(reply_builder_); 
    return reply_builder_; 
  }

  int fd() const { return fd_; }
 private:
  Transaction* transaction_;
  const CommandId* cid_;
  ReplyBuilder* reply_builder_;
  int fd_;
};

}  // namespace dfly

#pragma once
#include <glog/logging.h>

#include <coroutine>
#include <memory>

#include "cppcoro/task.hpp"
#include "detail/common.hpp"
#include "net/uring_proactor.hpp"
#include "redis/facade/reply_builder.hpp"
#include "sharding/db_slice.hpp"
#include "sharding/engine_shard_set.hpp"
namespace dfly {

class Connection;
class Transaction;
class EngineShard;
class PipelineSquasher;

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

  CommandContext(std::shared_ptr<Transaction> transaction, const CommandId* cid,
                 ReplyBuilder* reply_builder)
      : transaction_(std::move(transaction)),
        cid_(cid),
        reply_builder_(reply_builder) {
    DCHECK(reply_builder_);
  }
  const CommandId* cid() const { return cid_; }
  std::shared_ptr<Transaction> tx() const { return transaction_; }
  ReplyBuilder* rb() {
    DCHECK(reply_builder_);
    return reply_builder_;
  }

  void Reset() noexcept { continuation_ = nullptr; }
  void SetContinuation(std::coroutine_handle<> h) noexcept {
    continuation_ = h;
  }
  std::coroutine_handle<> TakeContinuation() noexcept {
    return std::exchange(continuation_, nullptr);
  }

 private:
  std::shared_ptr<Transaction> transaction_;
  const CommandId* cid_;
  ReplyBuilder* reply_builder_;
  std::coroutine_handle<> continuation_{};
};

}  // namespace dfly

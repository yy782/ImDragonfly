#pragma once
#include <glog/logging.h>

#include <coroutine>
#include <memory>

#include "cppcoro/task.hpp"
#include "detail/common_types.hpp"
#include "io/uring_proactor.hpp"
#include "redis/facade/reply_builder.hpp"
#include "util/intrusive_ptr.hpp"
namespace dfly {

class Connection;
class Transaction;
class PipelineSquasher;

class ConnectionContext {
 public:
  ConnectionContext() = default;
  ConnectionContext& operator=(const ConnectionContext& o) {
    owner_ = o.owner_;
    index_ = o.index_;
    return *this;
  }
  ~ConnectionContext();
  RedisSessionPtr owner() const { return owner_; }
  RedisSessionPtr& owner() { return owner_; }

  template <typename Cb>
  void AddWatchKey(std::string_view key, Cb&& cb);

  DbIndex GetDbIndex() const { return index_; }

  void NotifyClose() { owner_.reset(); }

 private:
  friend class RedisSession;
  ConnectionContext(RedisSessionPtr owner, DbIndex index)
      : owner_(owner), index_(index) {}
  RedisSessionPtr owner_;
  DbIndex index_;
};

class CommandId;

class CommandContext {
 public:
  CommandContext() = default;
  ~CommandContext();

  CommandContext(util::intrusive_ptr<Transaction> transaction,
                 const CommandId* cid, ReplyBuilder* reply_builder);
  const CommandId* cid() const { return cid_; }
  util::intrusive_ptr<Transaction> tx() const;
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
  util::intrusive_ptr<Transaction> transaction_;
  const CommandId* cid_;
  ReplyBuilder* reply_builder_;
  std::coroutine_handle<> continuation_{};
};

}  // namespace dfly

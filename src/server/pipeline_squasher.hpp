#pragma once

#include <glog/logging.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "command_layer/cmd_support.hpp"
#include "command_layer/command_id.hpp"
#include "cppcoro/task.hpp"
#include "detail/common_types.hpp"
#include "detail/conn_context.hpp"
#include "io/uring_proactor.hpp"
#include "redis/facade/reply_builder.hpp"
#include "sharding/shard_pool.hpp"
#include "transaction_layer/transaction.hpp"

namespace dfly {

struct QCmd {
  const CommandId* cid;
  std::vector<std::string_view> args;
};

class PipelineSquasher {
 public:
  using SendCallback = ReplyBuilder::SendCallback;

  PipelineSquasher() : db_(0), proactor_(nullptr) {}

  void Init(DbIndex db, SendCallback send_cb, base::UringProactor* proactor) {
    db_ = db;
    proactor_ = proactor;
    DCHECK(shard_pool);
    dispatched_.resize(shard_pool->size());
    send_rb_.SetSendCallback(std::move(send_cb));
  }

  cppcoro::task<void> Run(std::vector<QCmd>&& cmds);

 private:
  struct ShardDispatch {
    struct Entry {
      const CommandId* cid;
      ::dfly::CmdArgList args;  // 指向接收缓冲区
      std::vector<std::string> replies;
      Entry(const CommandId* c, ::dfly::CmdArgList a) : cid(c), args(a) {}
      Entry(const Entry&) = default;
      Entry& operator=(const Entry&) = default;
      Entry(Entry&&) = default;
      Entry& operator=(Entry&&) = default;
    };
    std::vector<Entry> entries;
    size_t reply_id = 0;
    ReplyBuilder reply_builder;
    // 复用同一 Transaction，省去每条命令各 new 一个事务对象的内存开销。
    util::intrusive_ptr<Transaction> local_tx;

    ShardDispatch() = default;
    ShardDispatch(const ShardDispatch&) = default;
    ShardDispatch& operator=(const ShardDispatch&) = default;
    ShardDispatch(ShardDispatch&&) = default;
    ShardDispatch& operator=(ShardDispatch&&) = default;
  };

  bool TrySquash(const QCmd& q);
  cppcoro::task<void> ExecuteSquashed();
  cppcoro::task<void> ExecuteStandalone(const QCmd& q);
  cppcoro::task<void> SwitchToLoop();

  DbIndex db_;
  ReplyBuilder send_rb_;
  base::UringProactor* proactor_;

  std::vector<ShardDispatch> dispatched_;
  std::vector<ShardId> order_;
};

}  // namespace dfly

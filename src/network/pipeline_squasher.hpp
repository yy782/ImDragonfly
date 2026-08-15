

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
#include "net/uring_proactor.hpp"
#include "redis/facade/reply_builder.hpp"
#include "sharding/engine_shard_set.hpp"
#include "transaction_layer/transaction.hpp"

namespace dfly {

struct QCmd {
  const CommandId* cid;
  std::vector<std::string_view> args;
};

class PipelineSquasher {
 public:
  using SendCallback = ReplyBuilder::SendCallback;

  PipelineSquasher() : ns_(nullptr), db_(0), proactor_(nullptr) {}

  // 连接建立后一次性初始化（避免每批命令重建 dispatched_/ReplyBuilder）。
  // 注意必须用 Init 而非"默认构造+移动赋值"：构造函数中 send_rb_ 的 lambda
  // 捕获 this，移动赋值后 this 会指向已销毁的临时对象。
  void Init(const Namespace* ns, DbIndex db, SendCallback send_cb,
            base::UringProactor* proactor) {
    ns_ = ns;
    db_ = db;
    send_cb_ = std::move(send_cb);
    proactor_ = proactor;
    DCHECK(shard_set);
    dispatched_.resize(shard_set->size());
    send_rb_.SetSendCallback(
        [this](std::string&& s) { send_cb_(std::move(s)); });
  }

  cppcoro::task<void> Run(std::vector<QCmd>&& cmds);

 private:
  struct ShardDispatch {
    struct Entry {
      const CommandId* cid;
      ::cmn::CmdArgList args;  // 指向接收缓冲区
      std::shared_ptr<Transaction> tx;
      std::vector<std::string> replies;
      Entry(const CommandId* c, ::cmn::CmdArgList a,
            std::shared_ptr<Transaction> t)
          : cid(c), args(a), tx(std::move(t)) {}
    };
    std::vector<Entry> entries;
    size_t reply_id = 0;
    ReplyBuilder reply_builder;
  };

  bool TrySquash(const QCmd& q);
  cppcoro::task<void> ExecuteSquashed();
  cppcoro::task<void> ExecuteStandalone(const QCmd& q);
  cppcoro::task<void> SwitchToLoop();

  const Namespace* ns_;
  DbIndex db_;
  SendCallback send_cb_;
  ReplyBuilder send_rb_;
  base::UringProactor* proactor_;

  std::vector<ShardDispatch> dispatched_;
  std::vector<ShardId> order_;
};

}  // namespace dfly

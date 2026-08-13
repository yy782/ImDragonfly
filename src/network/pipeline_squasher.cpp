
#include "network/pipeline_squasher.hpp"

#include "detail/common.hpp"
#include "util/synchronization.hpp"
#include "util/thread.hpp"

namespace dfly {

namespace {

struct LoopSwitch {
  base::UringProactor* p;
  bool await_ready() const noexcept {
    return util::Thread::current_tid() == p->GetLoopThreadId();
  }
  void await_suspend(std::coroutine_handle<> h) noexcept {
    if (!p->DispatchBrief([h]() { h.resume(); })) {
      // 任务队列满（极端过载）：原地恢复会在非环线线程发起 io_uring 操作
      // （数据竞争）。记录 ERROR 后原地恢复，至少保证连接不永久挂死。
      LOG(ERROR) << "LoopSwitch queue full, resume on non-loop thread";
      h.resume();
    }
  }
  void await_resume() const noexcept {}
};

}  // namespace

bool PipelineSquasher::TrySquash(const QCmd& q) {
  const CommandId* cid = q.cid;
  // 全局事务 / 无 key 但需保序的命令不适合合并。
  if (cid->opt_mask() & (CO::GLOBAL_TRANS | CO::NO_KEY_TRANSACTIONAL))
    return false;

  ::cmn::CmdArgList args(q.args);
  if (args.empty()) return false;

  OpResult<KeyIndex> ki = DetermineKeys(cid, args);
  if (!ki || ki->NumArgs() == 0) return false;

  ShardId sid = kInvalidSid;
  for (std::string_view key : ki->Range(args)) {
    ShardId s = Shard(key, shard_set->size());
    if (sid == kInvalidSid)
      sid = s;
    else if (s != sid)
      return false;
  }
  if (sid == kInvalidSid) return false;

  auto tx = std::make_shared<Transaction>(cid);
  if (tx->InitByArgs(ns_, db_, args) != OpStatus::OK) return false;

  dispatched_[sid].entries.push_back(
      ShardDispatch::Entry(cid, args, std::move(tx)));
  order_.push_back(sid);
  return true;
}

cppcoro::task<void> PipelineSquasher::ExecuteSquashed() {
  if (order_.empty()) co_return;

  std::vector<ShardId> sids;
  for (size_t i = 0; i < dispatched_.size(); ++i) {
    if (!dispatched_[i].entries.empty()) sids.push_back(i);
  }

  BlockingCounter bc(sids.size());
  for (ShardId sid : sids) {
    ShardDispatch& sd = dispatched_[sid];
    shard_set->Add(sid, [&sd, &bc]() mutable {
      auto t = [](ShardDispatch& sd,
                  BlockingCounter& bc) -> cppcoro::AsyncTask {
        for (auto& e : sd.entries) {
          // 每命令重绑回调：本分片共享一个回复构造器，把回复收集到当前 Entry。
          sd.reply_builder.SetSendCallback(
              [&e](std::string&& s) { e.replies.push_back(std::move(s)); });
          CommandContext cmd_cntx(e.tx, e.cid, &sd.reply_builder);
          co_await e.cid->Invoke(&cmd_cntx, e.args);
        }
        bc->Dec();
        co_return;
      };
      t(sd, bc);
    });
  }
  co_await bc->Wait();

  // co_await SwitchToLoop();

  for (ShardId sid : order_) {
    ShardDispatch& sd = dispatched_[sid];
    auto& e = sd.entries[sd.reply_id++];
    for (auto& r : e.replies) send_rb_.SendRaw(std::move(r));
  }

  for (auto& sd : dispatched_) {
    sd.entries.clear();
    sd.reply_id = 0;  // 下一批命令回放游标从 0 重新开始
  }
  order_.clear();
  co_return;
}

cppcoro::task<void> PipelineSquasher::ExecuteStandalone(const QCmd& q) {
  ::cmn::CmdArgList args(q.args);
  auto tx = std::make_shared<Transaction>(q.cid);
  tx->InitByArgs(ns_, db_, args);
  CommandContext cmd_cntx(tx, q.cid, &send_rb_);
  co_await q.cid->Invoke(&cmd_cntx, args);
  co_return;
}

cppcoro::task<void> PipelineSquasher::Run(std::vector<QCmd>&& cmds) {
  for (const QCmd& q : cmds) {
    if (TrySquash(q)) continue;

    co_await ExecuteSquashed();
    co_await ExecuteStandalone(q);
    // co_await SwitchToLoop();
  }
  co_await ExecuteSquashed();
  co_await SwitchToLoop();
  co_return;
}

cppcoro::task<void> PipelineSquasher::SwitchToLoop() {
  co_await LoopSwitch{proactor_};
  co_return;
}

}  // namespace dfly


#include "network/pipeline_squasher.hpp"

#include "detail/common.hpp"
#include "sharding/synchronization.hpp"
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

  dispatched_[sid].entries.push_back(ShardDispatch::Entry(cid, args, *ki));
  order_.push_back(sid);
  return true;
}

cppcoro::task<void> PipelineSquasher::ExecuteSquashed() {
  if (order_.empty()) co_return;

  std::vector<ShardId> sids;
  for (size_t i = 0; i < dispatched_.size(); ++i) {
    if (!dispatched_[i].entries.empty()) sids.push_back(i);
  }

  dfly::BlockingCounter bc(sids.size());
  for (ShardId sid : sids) {
    ShardDispatch& sd = dispatched_[sid];
    shard_set->Add(sid, [this, sid, &sd, &bc]() mutable {
      auto t = [](ShardDispatch& sd, dfly::BlockingCounter& bc,
                  const Namespace* ns, DbIndex db,
                  ShardId sid) -> cppcoro::AsyncTask {
        for (auto& e : sd.entries) {
          if (!sd.local_tx)
            sd.local_tx.reset(new Transaction(e.cid));
          else
            sd.local_tx->PrepareForReuse(e.cid);
          sd.local_tx->InitByArgs(ns, db, e.args, e.key_index, sid);
          sd.reply_builder.SetSendCallback([&e](std::vector<std::string>&& v) {
            for (auto& s : v) e.replies.push_back(std::move(s));
          });
          CommandContext cmd_cntx(sd.local_tx, e.cid, &sd.reply_builder);
          co_await e.cid->Invoke(&cmd_cntx, e.args);
          sd.reply_builder.Flush();
        }
        bc->Dec();
        co_return;
      };
      t(sd, bc, ns_, db_, sid);
    });
  }
  co_await bc->Wait();

  // co_await SwitchToLoop();

  for (ShardId sid : order_) {
    ShardDispatch& sd = dispatched_[sid];
    auto& e = sd.entries[sd.reply_id++];
    for (auto& r : e.replies) send_rb_.SendRaw(std::move(r));
  }
  // 整批回放只 Flush 一次：把一批回复用一次回调、一次任务队列入队发出去。
  send_rb_.Flush();

  for (auto& sd : dispatched_) {
    sd.entries.clear();
    sd.reply_id = 0;  // 下一批命令回放游标从 0 重新开始
  }
  order_.clear();
  co_return;
}

cppcoro::task<void> PipelineSquasher::ExecuteStandalone(const QCmd& q) {
  ::cmn::CmdArgList args(q.args);
  util::intrusive_ptr<Transaction> tx{
      new Transaction(q.cid)};  // new 会抛异常，要注意一下
  tx->InitByArgs(ns_, db_, args);
  CommandContext cmd_cntx(tx, q.cid, &send_rb_);
  co_await q.cid->Invoke(&cmd_cntx, args);
  send_rb_.Flush();
  co_return;
}

cppcoro::task<void> PipelineSquasher::Run(std::vector<QCmd>&& cmds) {
  order_.reserve(cmds.size());
  const size_t per_shard =
      dispatched_.empty() ? 0 : cmds.size() / dispatched_.size();
  for (auto& sd : dispatched_)
    sd.entries.reserve(sd.entries.size() + per_shard);
  for (const QCmd& q : cmds) {
    if (TrySquash(q)) continue;

    co_await ExecuteSquashed();
    co_await ExecuteStandalone(q);
    // co_await SwitchToLoop();
  }
  co_await ExecuteSquashed();
  co_await LoopSwitch{proactor_};
  co_return;
}

}  // namespace dfly

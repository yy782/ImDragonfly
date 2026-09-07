#include "server/pipeline_squasher.hpp"

#include <new>

#include "detail/common_types.hpp"
#include "sharding/synchronization.hpp"
#include "util/thread.hpp"

namespace dfly {

bool PipelineSquasher::TrySquash(const QCmd& q) {
  const CommandId* cid = q.cid;
  // 全局事务 / 无 key 但需保序的命令不适合合并。
  if (cid->opt_mask() & (CO::GLOBAL_TRANS | CO::NO_KEY_TRANSACTIONAL))
    return false;

  ::dfly::CmdArgList args(q.args);

  DCHECK(!args.empty());

  if (cid->first_key_pos() <= 0 || cid->first_key_pos() != cid->last_key_pos())
    return false;

  ShardId sid = kInvalidSid;
  for (const auto& kv : cid->Keys(args)) {
    ShardId s = ShardIndex(kv.key, shard_pool->size());
    if (sid == kInvalidSid)
      sid = s;
    else if (s != sid)
      return false;
  }
  if (sid == kInvalidSid) return false;

  dispatched_[sid].entries.push_back(ShardDispatch::Entry(cid, args));
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
  auto make_cb = [this, bc](ShardDispatch& sd) {
    return [this, &sd, bc]() mutable {
      auto t = [](ShardDispatch& sd, dfly::BlockingCounter bc,
                  DbIndex db) -> cppcoro::AsyncTask {
        sd.local_tx.reset(new Transaction(
            nullptr));  // 事务的生命周期明确，考虑移除intrusive_ptr保护
        for (auto& e : sd.entries) {
          sd.local_tx->ResetForReuse(e.cid);
          sd.local_tx->Init(db, e.args);
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
      t(sd, std::move(bc), db_);
    };
  };

  for (ShardId sid : sids) {
    ShardDispatch& sd = dispatched_[sid];
    shard_pool->Post(sid, make_cb(sd));
  }
  co_await bc->Wait();

  for (ShardId sid : order_) {  // 这里可能不好理解哦
    ShardDispatch& sd = dispatched_[sid];
    auto& e = sd.entries[sd.reply_id++];
    for (auto& r : e.replies) send_rb_.SendRaw(std::move(r));
  }
  send_rb_.Flush();

  for (auto& sd : dispatched_) {
    sd.entries.clear();
    sd.reply_id = 0;
  }
  order_.clear();
  co_return;
}

cppcoro::task<void> PipelineSquasher::ExecuteStandalone(const QCmd& q) {
  ::dfly::CmdArgList args(q.args);
  util::intrusive_ptr<Transaction> tx{new Transaction(q.cid)};
  tx->Init(db_, args);
  CommandContext cmd_cntx(tx, q.cid, &send_rb_);
  co_await q.cid->Invoke(&cmd_cntx, args);
  send_rb_.Flush();
  co_return;
}

cppcoro::task<void> PipelineSquasher::Run(std::vector<QCmd>&& cmds) {
  order_.reserve(cmds.size());
  DCHECK(!dispatched_.empty());
  const size_t per_shard = cmds.size() / dispatched_.size();
  for (auto& sd : dispatched_)
    sd.entries.reserve(sd.entries.size() + per_shard);
  for (const QCmd& q : cmds) {
    if (TrySquash(q)) continue;
    co_await ExecuteSquashed();
    co_await ExecuteStandalone(q);
  }
  co_await ExecuteSquashed();
  co_return;
}

}  // namespace dfly

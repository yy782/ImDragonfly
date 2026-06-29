#include <set>
#include <unordered_set>
#include <iostream>
#include <span>

#include "transaction.hpp"
#include "sharding/engine_shard_set.hpp"
#include "sharding/db_slice.hpp"
#include "command_layer/cmn_types.hpp"
#include "detail/tx_base.hpp"

namespace dfly{

using facade::kInvalidKeysStart;
using facade::kInvalidKeysNum;
using facade::kInvalidKeysOffset;

std::atomic<uint64_t> txid_counter_{1};

inline uint64_t Fingerprint(std::string_view str) {
  uint64_t hash = 14695981039346656037ULL;
  for (char c : str) {
    hash ^= static_cast<uint8_t>(c);
    hash *= 1099511628211ULL;
  }
  return hash;
}

inline TxQueue::Iterator InsertQueue(Transaction* tx) {
  EngineShard* shard = EngineShard::tlocal();
  TxQueue* txq = shard->txq();
  if (!txq->Empty() && txq->Back()->txid() > tx->txid()) {
    return txq->InsertAfter(txq->Tail(), tx);
  } else {
    return txq->PushBack(tx);
  }
}

uint64_t LockTag::Fingerprint() const {
  return ::dfly::Fingerprint(str_);
}

LockTag::LockTag(std::string_view key) : str_(key) {}


Transaction::Transaction(const CommandId* cid) : cid_(cid) {
}

Transaction::~Transaction() {
}


void Transaction::InitByArgs(ConnectionContext* conn_cntx, CmdArgList args) {
  conn_cntx_ = conn_cntx;
  namespace_ = conn_cntx_->GetNamespace();
  full_args_ = args;
  db_cntx_ = DbContext(namespace_, conn_cntx_->GetDbIndex(), util::GetCurrentTimeMs());
  cmd_cntx_ = CommandContext(conn_cntx_, this, cid_);
  InitSlice();
  return;
}


void Transaction::InitSlice() {
  Slices_.resize(shard_set->size());
  for (ShardId i = 0; i < Slices_.size(); ++i) {
    Slices_[i].unique_shard_id = i;
    Slices_[i].tx = this;
  }
  for (size_t i = cid_->keys_start(); 
  i < full_args_.size() && i < cid_->keys_start() + cid_->keys_nums() * cid_->keys_offset(); i += cid_->keys_offset()) {
    std::string_view key = full_args_[i];
    ShardId sid = Shard(key, shard_set->size());
    auto& slice = Slices_[sid];
    slice.local_mask = ACTIVE;
    slice.keyIds.push_back(i);
    ++key_num_;
  }

  unique_shard_cnt_ = 0;
  for (ShardId sid = 0; sid < Slices_.size(); ++sid) {
    if (!Slices_[sid].keyIds.empty()) {
      ++unique_shard_cnt_;
      unique_shard_id_ = sid;
    }
  }
  for (ShardId sid = 0; sid < Slices_.size(); ++sid) {
    auto& slice = Slices_[sid];
    if (slice.keyIds.empty()) continue;
    slice.lists = CmdArgList(full_args_.begin(), full_args_.end());
  }
}



void Transaction::EnableAllShards() {
  Slices_.resize(shard_set->size());
  for (ShardId i = 0; i < Slices_.size(); ++i) {
    Slices_[i].unique_shard_id = i;
    Slices_[i].tx = this;
    Slices_[i].local_mask = ACTIVE;
  }
  unique_shard_cnt_ = shard_set->size();
}


cppcoro::AsyncTask Transaction::Scheduling(std::coroutine_handle<> handle, RunnableType&& cb) {
  coro_handle_ = handle;
  cb_ = std::move(cb);
  run_barrier_.store(unique_shard_cnt_, std::memory_order_release);
  coordinator_state_ |= COORD_CONCLUDING;
  co_await ScheduleInternal();
  co_return;
}

cppcoro::task<void> Transaction::ScheduleInternal() {
  coordinator_state_ |= COORD_SCHED;
  txid_ = txid_counter_.fetch_add(1, std::memory_order_relaxed);
  co_await IterateActiveShards([this](auto& sd, ShardId sid) {
    EngineShard* shard = EngineShard::tlocal(); 
    ScheduleInShard(shard, true);
  });
  co_return;
}

bool Transaction::ScheduleInShard(EngineShard* shard, bool execute_optimistic) {
  auto& sd = Slices_[SidToId(shard->shard_id())];

  if (!multi_ || multi_->mode == NON_ATOMIC) {
    KeyLockArgs lock_args = GetLockArgs(shard->shard_id());
    if (!LockMultiShardCb(lock_args, shard)) {
      UnlockMultiShardCb(lock_args, shard);
      return false;
    }
    sd.local_mask |= KEYLOCK_ACQUIRED;
  }
  sd.pq_pos = InsertQueue(this);
  if (execute_optimistic && !shard->txq()->Empty() && shard->txq()->Front() == this) {
    sd.local_mask |= OPTIMISTIC_EXECUTION;
  }

  bool can_execute = sd.local_mask & (OPTIMISTIC_EXECUTION | KEYLOCK_ACQUIRED);
  if (can_execute) RunInShard(shard, false);

  return true;
}

bool Transaction::RunInShard(EngineShard* shard, bool allow_q_removal) {
  ShardId sid = shard->shard_id();
  auto& sd = Slices_[SidToId(sid)];
  RunCallback(shard);
  FinishHop();
  return true;
}

void Transaction::RunCallback(EngineShard* shard) {
  if (!cb_) {
    return;
  }
  cb_(this, shard);
}

void Transaction::FinishHop() {
  uint32_t prev = run_barrier_.fetch_sub(1, std::memory_order_acq_rel);
  if (prev == 1) { // zhe li bu fang bian shi fang suo 
    if (coro_handle_) {
      coro_handle_.resume();
    }
  }
}

KeyLockArgs Transaction::GetLockArgs(ShardId sid) const {
  const auto& sd = Slices_[SidToId(sid)];
  std::vector<LockFp> fps;
  for (auto key_id : sd.keyIds)
  {
    fps.push_back(Fingerprint(full_args_[key_id]));
  }
  return {
    db_cntx_.GetDbIndex(),
    std::move(fps)
  };
}

bool Transaction::IsActive(ShardId sid) const {
  return Slices_[SidToId(sid)].local_mask & ACTIVE;
}


DbSlice& Transaction::GetDbSlice(ShardId sid) const {
  return namespace_->GetDbSlice(sid);
}

IntentLock::Mode Transaction::LockMode() const { // 仅支持独占锁
  return IntentLock::EXCLUSIVE;
}

void Transaction::QueueCommand(const CommandId* cid, CmdArgList args) {
  std::vector<std::string> VecArgs(args.begin(), args.end());
  std::vector<std::string_view> ViewArgs(VecArgs.begin(), VecArgs.end());
  queued_commands_.push_back({cid, std::move(VecArgs), std::move(ViewArgs)});
}


bool Transaction::LockMultiShardCb(const KeyLockArgs& lock_args, EngineShard* shard) {
  return GetDbSlice(shard->shard_id()).Acquire(LockMode(), lock_args);
}

void Transaction::UnlockMultiShardCb(const KeyLockArgs& lock_args, EngineShard* shard) {
  GetDbSlice(shard->shard_id()).Release(LockMode(), lock_args);
}


cppcoro::task<void> Transaction::Finish() {
co_await IterateActiveShards([this](auto& sd, ShardId sid) {
    auto e = EngineShard::tlocal();
    UnlockMultiShardCb(GetLockArgs(sid), e);
    sd.local_mask &= ~KEYLOCK_ACQUIRED;
    sd.local_mask &= ~OPTIMISTIC_EXECUTION;
    e->txq()->Remove(sd.pq_pos);
  });
  co_return;
}

}

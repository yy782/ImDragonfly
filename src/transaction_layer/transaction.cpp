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
  for (size_t i = cid_->keys_start(); i < full_args_.size() && i < cid_->keys_start() + cid_->keys_nums() * cid_->keys_offset(); i += cid_->keys_offset()) {
    std::string_view key = full_args_[i];
    ShardId sid = Shard(key, shard_set->size());
    LockFp fp = Fingerprint(key);
    kv_fp_.push_back(fp);
    auto& slice = Slices_[sid];
    slice.local_mask = ACTIVE;
    slice.keyIds.push_back(i);
  }

  unique_shard_cnt_ = 0;
  for (ShardId sid = 0; sid < Slices_.size(); ++sid) {
    if (!Slices_[sid].keyIds.empty()) {
      ++unique_shard_cnt_;
      unique_shard_id_ = sid;
    }
  }
  uint32_t fp_idx = 0;
  for (ShardId sid = 0; sid < Slices_.size(); ++sid) {
    auto& slice = Slices_[sid];
    if (slice.keyIds.empty()) continue;

    slice.fp_start = fp_idx;
    slice.fp_count = slice.keyIds.size();
    fp_idx += slice.fp_count;

    slice.lists = CmdArgList(full_args_.begin() + slice.keyIds[0], 
                            full_args_.begin() + slice.keyIds.back() + 1);
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

void Transaction::Execute(RunnableType cb, bool conclude) {
  cb_ = cb;

  if (conclude) {
    coordinator_state_ |= COORD_CONCLUDING;
  }

  ScheduleInternal();
  DispatchHop();
}

void Transaction::Scheduling(std::coroutine_handle<> handle, RunnableType&& cb) {
  coro_handle_ = handle;
  cb_ = std::move(cb);
  run_barrier_.store(unique_shard_cnt_, std::memory_order_release);
  coordinator_state_ |= COORD_CONCLUDING;
  ScheduleInternal();
  // IterateActiveShards([this](auto& sd, ShardId sid) {
  //   if (!(sd.local_mask & ACTIVE)) {
  //     return;
  //   }
  //   sd.is_armed = true;
  //   shard_set->Add(sid, [this, sid]() {
  //     EngineShard* shard = EngineShard::tlocal();
  //     RunInShard(shard, true);
  //   });
  // });
}

void Transaction::ScheduleInternal() {
  coordinator_state_ |= COORD_SCHED;
  
  //if (txid_ == 0 && unique_shard_cnt_ > 1) {
  txid_ = txid_counter_.fetch_add(1, std::memory_order_relaxed);
  //}

  IterateActiveShards([this](auto& sd, ShardId sid) {
    if (!sd.is_armed) {
      EngineShard* shard = EngineShard::tlocal();
      ScheduleInShard(shard, true);
    }
  });
}

bool Transaction::ScheduleInShard(EngineShard* shard, bool execute_optimistic) {
  auto& sd = Slices_[SidToId(shard->shard_id())];

  if (!multi_ || multi_->mode == NON_ATOMIC) {
    KeyLockArgs lock_args = GetLockArgs(shard->shard_id());
    if (!LockMultiShardCb(lock_args.fps, shard)) {
      UnlockMultiShardCb(lock_args.fps, shard);
      return false;
    }
    sd.local_mask |= KEYLOCK_ACQUIRED;
  }
  sd.pq_pos = InsertQueue(this);
  if (execute_optimistic && !shard->txq()->Empty() && shard->txq()->Front() == this) {
    sd.local_mask |= OPTIMISTIC_EXECUTION;
    return true;
  }
  // if (txid_ == 0) {
  //   txid_ = txid_counter_.fetch_add(1, std::memory_order_relaxed);
  // }
  return true;
}

void Transaction::DispatchHop() {
  run_barrier_.store(unique_shard_cnt_, std::memory_order_release);

  IterateActiveShards([this](auto& sd, ShardId sid) {
    if (!(sd.local_mask & ACTIVE)) {
      return;
    }

    sd.is_armed = true;

    shard_set->Add(sid, [this, sid]() {
      EngineShard* shard = EngineShard::tlocal();
      RunInShard(shard, true);
    });
  });
}

bool Transaction::RunInShard(EngineShard* shard, bool allow_q_removal) {
  ShardId sid = shard->shard_id();
  auto& sd = Slices_[SidToId(sid)];

  if (!sd.is_armed) {
    return false;
  }

  sd.is_armed = false;

  RunCallback(shard);

  bool is_concluding = coordinator_state_ & COORD_CONCLUDING;

  if (sd.pq_pos != TxQueue::kEnd && (is_concluding || allow_q_removal)) {
    shard->txq()->Remove(sd.pq_pos);
    sd.pq_pos = TxQueue::kEnd;
  }

  if (is_concluding) {
    IntentLock::Mode mode = LockMode();

    if (sd.local_mask & KEYLOCK_ACQUIRED) {
      KeyLockArgs lock_args = GetLockArgs(sid);
      shard->GetDbSlice(sid)->Release(mode, lock_args);
      sd.local_mask &= ~KEYLOCK_ACQUIRED;
    }
  }

  FinishHop();

  return is_concluding;
}

void Transaction::RunCallback(EngineShard* shard) {
  if (!cb_) {
    return;
  }
  cb_(this, shard);
}

void Transaction::FinishHop() {
  uint32_t prev = run_barrier_.fetch_sub(1, std::memory_order_acq_rel);
  if (prev == 1) {
    
    if (coro_handle_) {
      coro_handle_.resume();
    }
  }
}

void Transaction::Conclude() {
  if (IsAtomicMulti()) {
    EngineShard* shard = EngineShard::tlocal();
    KeyLockArgs lock_args = GetLockArgs(shard->shard_id());
    UnlockMultiShardCb(lock_args.fps, shard);
    auto& sd = Slices_[SidToId(shard->shard_id())];
    sd.local_mask &= ~KEYLOCK_ACQUIRED;
  }
  coordinator_state_ &= ~COORD_SCHED;
  coordinator_state_ &= ~COORD_CONCLUDING;
}

// void Transaction::StartMultiLockedAhead(Namespace* ns, DbIndex dbid, CmdArgList keys,
//                                        bool skip_scheduling) {
//   namespace_ = ns;
//   db_cntx_ = DbContext(ns, dbid, time_now_ms_);
//   multi_ = std::make_unique<MultiData>();
//   multi_->mode = LOCK_AHEAD;

//   PrepareMultiFps(keys);

//   if (!skip_scheduling) {
//     ScheduleInternal();
//     DispatchHop();
//   }
// }

// void Transaction::StartMultiNonAtomic() {
//   multi_ = std::make_unique<MultiData>();
//   multi_->mode = NON_ATOMIC;
// }

// void Transaction::PrepareMultiFps(CmdArgList keys) {
//   for (size_t i = 0; i < keys.size(); ++i) {
//     std::string_view key = keys[i];
//     ShardId sid = Shard(key, shard_set->size());
//     LockFp fp = Fingerprint(key);
//     multi_->tag_fps.insert({sid, fp});
//   }
// }

// void Transaction::UnlockMulti(bool block) {
//   if (!multi_) {
//     return;
//   }

//   if (multi_->mode == GLOBAL) {
//     IterateActiveShards([this](auto& sd, ShardId sid) {
//       EngineShard* shard = EngineShard::tlocal();
//       shard->shard_lock()->Release(IntentLock::EXCLUSIVE);
//     });
//   } else if (multi_->mode == LOCK_AHEAD) {
//     IterateActiveShards([this](auto& sd, ShardId sid) {
//       if (sd.local_mask & KEYLOCK_ACQUIRED) {
//         EngineShard* shard = EngineShard::tlocal();
//         KeyLockArgs lock_args = GetLockArgs(sid);
//         shard->GetDbSlice(sid)->Release(LockMode(), lock_args);
//         sd.local_mask &= ~KEYLOCK_ACQUIRED;
//       }
//     });
//   }
// }

KeyLockArgs Transaction::GetLockArgs(ShardId sid) const {
  const auto& sd = Slices_[SidToId(sid)];
  return {
    db_cntx_.GetDbIndex(),
    std::span(kv_fp_.data() + sd.fp_start, sd.fp_count)
  };
}

// uint16_t Transaction::DisarmInShard(ShardId sid) {
//   auto& sd = Slices_[SidToId(sid)];
//   if (sd.is_armed) {
//     sd.is_armed = false;
//     return sd.local_mask | ACTIVE;
//   }
//   return 0;
// }

// std::pair<uint16_t, bool> Transaction::DisarmInShardWhen(ShardId sid, uint16_t req_flags) {
//   auto& sd = Slices_[SidToId(sid)];
//   if (!(sd.local_mask & req_flags)) {
//     return {sd.local_mask, false};
//   }

//   if (sd.is_armed) {
//     sd.is_armed = false;
//     return {sd.local_mask | ACTIVE, true};
//   }
//   return {sd.local_mask, false};
// }

bool Transaction::IsActive(ShardId sid) const {
  return Slices_[SidToId(sid)].local_mask & ACTIVE;
}


DbSlice& Transaction::GetDbSlice(ShardId sid) const {
  return namespace_->GetDbSlice(sid);
}

IntentLock::Mode Transaction::LockMode() const {
  if (!cid_) {
    return IntentLock::EXCLUSIVE;
  }
  return IntentLock::EXCLUSIVE;
}

std::string_view Transaction::Name() const {
  return cid_ ? cid_->name() : "";
}

ShardId Transaction::GetUniqueShard() const {
  return unique_shard_id_;
}

// void Transaction::Refurbish() {
//   Slices_.clear();
//   kv_fp_.clear();
//   cb_.reset();
//   coordinator_state_ = 0;
//   local_result_ = OpStatus::OK;
//   unique_shard_cnt_ = 0;
//   unique_shard_id_ = kInvalidSid;
//   global_ = false;
//   multi_.reset();
//   state_ = State::IDLE;
//   queued_commands_.clear();
//   MultiRes_.clear();
// }

// const std::set<std::pair<ShardId, LockFp>>& Transaction::GetMultiFps() const {
//   static const std::set<std::pair<ShardId, LockFp>> empty;
//   return multi_ ? multi_->tag_fps : empty;
// }

void Transaction::QueueCommand(const CommandId* cid, CmdArgList args) {
  std::vector<std::string> VecArgs(args.begin(), args.end());
  std::vector<std::string_view> ViewArgs(VecArgs.begin(), VecArgs.end());
  queued_commands_.push_back({cid, std::move(VecArgs), std::move(ViewArgs)});
}


bool Transaction::LockMultiShardCb(std::span<const LockFp> fps, EngineShard* shard) {
  return shard->GetDbSlice(shard->shard_id())->Acquire(LockMode(), KeyLockArgs{db_cntx_.GetDbIndex(), fps});
}

void Transaction::UnlockMultiShardCb(std::span<const LockFp> fps, EngineShard* shard) {
  shard->GetDbSlice(shard->shard_id())->Release(LockMode(), KeyLockArgs{db_cntx_.GetDbIndex(), fps});
}

// bool Transaction::CanRunInlined() const {
//   return !IsMulti() && unique_shard_cnt_ == 1;
// }

}

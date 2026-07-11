#include <set>
#include <iostream>
#include <span>

#include "transaction.hpp"
#include "sharding/engine_shard_set.hpp"
#include "sharding/db_slice.hpp"
#include "command_layer/cmn_types.hpp"
#include "detail/tx_base.hpp"
#include "network/redis_server.hpp"
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





Transaction::Transaction(const CommandId* cid) : cid_(cid) {
}

Transaction::~Transaction() {
  assert(std::uncaught_exceptions() == 0);
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
  for (size_t i = cid_->keys_start(); 
  i < full_args_.size() && i < cid_->keys_start() + cid_->keys_nums() * cid_->keys_offset(); i += cid_->keys_offset()) {
    std::string_view key = full_args_[i];
    ShardId sid = Shard(key, shard_set->size());
    auto& slice = Slices_[sid];
    if (slice.keyIds.empty()) {
      ++unique_shard_cnt_;
      unique_shard_id_ = sid;
      slice.lists = CmdArgList(full_args_.begin(), full_args_.end());
      Slices_[sid].unique_shard_id = sid;
      Slices_[sid].tx = this;
    }
    slice.local_mask = ACTIVE;
    slice.keyIds.push_back(i);
    ++key_num_;
  }

  if (unique_shard_cnt_ != 1) {
    unique_shard_id_ = kInvalidSid;
  }else {
    auto& slice = Slices_[unique_shard_id_];
    slice.local_mask |= OUT_OF_ORDER;
    EngineShard* shard = EngineShard::tlocal();
    if (shard->shard_id() == unique_shard_id_) {
      coordinator_state_ |= COORD_INLINE;
    }
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


bool Transaction::Scheduling(std::coroutine_handle<> handle, RunnableType&& cb) {
  coro_handle_ = handle;
  cb_ = std::move(cb);
  run_barrier_.store(unique_shard_cnt_, std::memory_order_release);
  coordinator_state_ |= COORD_CONCLUDING;
  if (isInline()) {
    bool executed = RunInShard(EngineShard::tlocal());
    return executed;
  }

  ScheduleInternal();
  DispatchHop();
  return false;

}



bool Transaction::isInline() {
  return (coordinator_state_ & COORD_INLINE);
}

cppcoro::AsyncTask Transaction::ScheduleInternal() {
  coordinator_state_ |= COORD_SCHED;
 while(true) {
    txid_ = txid_counter_.fetch_add(1, std::memory_order_relaxed);
    std::atomic<bool> done{true};
    co_await IterateActiveShards([this, &done](auto& sd, ShardId sid) -> cppcoro::task<void> { // 注意IterateActiveShards的逻辑，这里事务是被别的线程完后后面部分的
      EngineShard* shard = EngineShard::tlocal();
      bool execute_optimistic = unique_shard_cnt_ == 1;
      bool success = ScheduleInShard(shard, execute_optimistic);
      if (!success) {
        done.store(false, std::memory_order_relaxed); // 这里的内存顺序正确吗
      }
      co_return;
    });  
    if (done.load(std::memory_order_relaxed)) {
      break;
    }
  }
  co_return;
}

void Transaction::DispatchHop() {
    if (isInline()) {
       auto* e = EngineShard::tlocal();
       e->PollExecution(this);      
      return;
    }
    auto cb = [this] () {
       auto* e = EngineShard::tlocal();
       e->PollExecution(this);
    }; 
    if (unique_shard_cnt_ == 1) {
      shard_set->Add(unique_shard_id_, [this, cb]() mutable {
        cb();
      });
    } else {
      for (ShardId i = 0; i < Slices_.size(); ++i) {
        auto shard_id = Slices_[i].unique_shard_id;
        if (!(Slices_[i].local_mask & ACTIVE)) {
          continue;
        }
        shard_set->Add(shard_id, [this, cb, shard_id]() mutable {
          cb();
        });
      }
    }
}


bool Transaction::ScheduleInShard(EngineShard* shard, bool execute_optimistic) {
  auto& sd = Slices_[SidToId(shard->shard_id())];
  KeyLockArgs lock_args = GetLockArgs(shard->shard_id());
/*
- 拿到锁了 → 直接入队，排着等执行
- 没拿到锁，但 txid 比队尾大 → 也可以入队（排到队尾），等前面的事务释放锁后才有机会执行
- 没拿到锁，且 txid 比队尾小 → 不行，会破坏 FIFO 顺序，返回失败让协调器重试
*/

  if ((!(sd.local_mask & KEYLOCK_ACQUIRED)) && LockMultiShardCb(lock_args, shard)) {
    sd.local_mask |= KEYLOCK_ACQUIRED;
  }else {
    if (!shard->txq()->Empty() && shard->txq()->Back()->txid() > txid_) {
      return false;
    } 
  }    
  
  if (execute_optimistic) {
    sd.local_mask |= OUT_OF_ORDER;
  }
  bool can_execute = (sd.local_mask & KEYLOCK_ACQUIRED) && 
        (sd.local_mask & OUT_OF_ORDER);
  if (can_execute && RunInShard(shard)) return true; 
  else {
    sd.pq_pos = shard->txq()->SwapBack(this, sd.pq_pos);
  }
  return true;
}

bool Transaction::RunInShard(EngineShard* shard) {
  auto& sd = Slices_[SidToId(shard->shard_id())];
  if (!(sd.local_mask & KEYLOCK_ACQUIRED)) {
    if (!LockMultiShardCb(GetLockArgs(shard->shard_id()), shard)) {
      return false;
    }
    sd.local_mask |= KEYLOCK_ACQUIRED;    
  }
  ShardId sid = shard->shard_id();
  RunCallback(shard);
  FinishHop();
  return true;
}

void Transaction::RunCallback(EngineShard* shard) {
  assert(cb_);
  cb_(this, shard);
}

void Transaction::FinishHop() {

  uint32_t prev = run_barrier_.fetch_sub(1, std::memory_order_acq_rel);

  if (isInline()) {
      auto e = EngineShard::tlocal();
      ShardId sid = e->shard_id();
      auto& sd = Slices_[SidToId(sid)];
      UnlockMultiShardCb(GetLockArgs(sid), e);
      e->AddCommittedTxid(this);
      if (sd.pq_pos != TxQueue::kEnd) {
        e->txq()->Remove(sd.pq_pos);
      }
    return;
  }
  
  if (state_ == State::IDLE) {
    auto e = EngineShard::tlocal();
    ShardId sid = e->shard_id();
    auto& sd = Slices_[SidToId(sid)];
    UnlockMultiShardCb(GetLockArgs(sid), e);
    e->AddCommittedTxid(this);
    if (sd.pq_pos != TxQueue::kEnd) {
      e->txq()->Remove(sd.pq_pos);
    }
    if (prev == 1) {
      coordinator_state_ |= COORD_CONCLUDING;
      coordinator_state_ = COORD_CANCELLED;  
      assert(coro_handle_);
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

IntentLock::Mode Transaction::LockMode() const { 
  return (cid_->opt_mask() & CO::READABLE) ? IntentLock::Mode::SHARED : IntentLock::Mode::EXCLUSIVE;
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


// cppcoro::AsyncTask Transaction::Finish() {
//   co_await IterateActiveShards([this](auto& sd, ShardId sid) mutable -> cppcoro::task<void> {
//       auto e = EngineShard::tlocal();
//       UnlockMultiShardCb(GetLockArgs(sid), e);
//       if (sd.pq_pos != TxQueue::kEnd) {
//         LOG(ERROR) << "Transaction " << txid_ << " is finishing but has a non-empty queue position in shard " << sid;
//         e->txq()->Remove(sd.pq_pos);
//       }
//       co_return;
//     });
//   coordinator_state_ = COORD_CANCELLED;  
//   assert(coro_handle_);
//   coro_handle_.resume();
  
  
//   co_return;
// }

}





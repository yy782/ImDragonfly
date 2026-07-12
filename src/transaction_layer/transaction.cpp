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
  // 非原子事务，一次性事务调用
  pq_pos_.resize(shard_set->size());
  for (auto& pos : pq_pos_) {
    pos = TxQueue::kEnd;
  }
  lock_args_.resize(shard_set->size());
  for (auto& lock_args : lock_args_) {
    lock_args.db_index = conn_cntx->GetDbIndex();
  }
  lock_mode_ = (cid_->opt_mask() & CO::READABLE) ? 
       IntentLock::Mode::SHARED : IntentLock::Mode::EXCLUSIVE;
  SliceHopCount_.resize(shard_set->size());
  for (auto& hop : SliceHopCount_) {
    hop = 1; // 这里不严谨,不是每个分片hop  = 1
  } 
  conn_cntx_ = conn_cntx;
  namespace_ = conn_cntx_->GetNamespace();
  full_args_ = args;
  db_cntx_ = DbContext(namespace_, conn_cntx_->GetDbIndex(), util::GetCurrentTimeMs());
  cmd_cntx_ = CommandContext(conn_cntx_, this, cid_);
  InitSlice();
  return;
}
void Transaction::InitByArgs(const ComPair& cp, SlicesArgs& slices_args) {
  // MULTI事务调用
  assert(state_ == State::EXEC);
  cid_ = cp.first;
  full_args_ = cp.second;
  Slices_ = slices_args.slices;
  unique_shard_id_ = slices_args.unique_shard_id;
  unique_shard_cnt_ = slices_args.unique_shard_cnt;
  key_num_ = slices_args.key_num;

  if (run_barrier_.load(std::memory_order_relaxed) != 0) {
    std::cout << "run_barrier_: " << run_barrier_.load(std::memory_order_relaxed) << std::endl;
  }
  assert(run_barrier_.load(std::memory_order_relaxed) == 0);
  assert(!lock_args_.empty());
  assert(is_armed_.load(std::memory_order_relaxed) == false);
  assert(coordinator_state_ == COORD_CANCELLED);
  assert(lock_mode_ == IntentLock::Mode::EXCLUSIVE);
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
    for (uint32_t i = 0; i < Slices_.size(); ++i) {
      auto& slice = Slices_[i];
      if (!(slice.local_mask & ACTIVE)) {
        continue;
      }
      lock_args_[i] = std::move(GetLockArgs(slice.unique_shard_id));
    }
  }else {
    auto& slice = Slices_[unique_shard_id_];
    slice.local_mask |= OUT_OF_ORDER;
    EngineShard* shard = EngineShard::tlocal();
    if (shard->shard_id() == unique_shard_id_) {
      coordinator_state_ |= COORD_INLINE;
    }
    lock_args_[unique_shard_id_] = std::move(GetLockArgs(unique_shard_id_));
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

  if (isInline()) { // 原子事务可能有问题吗
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
    is_armed_.store(true, std::memory_order_release);
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
  auto sid = shard->shard_id();
  auto& sd = Slices_[sid];
/*
- 拿到锁了 → 直接入队，排着等执行
- 没拿到锁，但 txid 比队尾大 → 也可以入队（排到队尾），等前面的事务释放锁后才有机会执行
- 没拿到锁，且 txid 比队尾小 → 不行，会破坏 FIFO 顺序，返回失败让协调器重试
*/

  if ((!(sd.local_mask & KEYLOCK_ACQUIRED)) && LockMultiShardCb(sid)) {
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
    if (state_ != State::UnMULTI) {
      return true;
    }
    GetPqPos(sid) = shard->txq()->SwapBack(this, GetPqPos(sid));
  }
  return true;
}

bool Transaction::RunInShard(EngineShard* shard) {
  auto sid = shard->shard_id();
  auto& sd = Slices_[sid];
  if (!(sd.local_mask & KEYLOCK_ACQUIRED)) {
    if (!LockMultiShardCb(sid)) {
      return false;
    }
    sd.local_mask |= KEYLOCK_ACQUIRED;    
  }
  RunCallback(shard);

  FinishHop(sid);

  return true;
}

void Transaction::RunCallback(EngineShard* shard) {
  assert(cb_);
  cb_(this, shard);
}


void Transaction::FinishHop(ShardId sid) {
  uint32_t prev = run_barrier_.fetch_sub(1, std::memory_order_acq_rel);
  std::cout << "run_barrier_: "  << run_barrier_.load(std::memory_order_relaxed) << std::endl;
  SliceHopCount_[sid]--;
  if (SliceHopCount_[sid] == 0) {
    // 事务不在该分片有跳跃了，可以释放锁了
    auto e = EngineShard::tlocal();
    auto& sd = Slices_[sid];
    UnlockMultiShardCb(sid);
    e->AddCommittedTxid();
    if (GetPqPos(sid) != TxQueue::kEnd) {
      e->txq()->Remove(GetPqPos(sid));
    }      
  }
  if (prev == 1) {
    coordinator_state_ |= COORD_CONCLUDING;
    coordinator_state_ = COORD_CANCELLED;    
    assert(coro_handle_);
    is_armed_.store(false, std::memory_order_release);
    coro_handle_.resume();
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
  return lock_mode_;
}


bool Transaction::LockMultiShardCb(ShardId sid) const {
  return GetDbSlice(sid).Acquire(LockMode(), lock_args_[sid]);
}

void Transaction::UnlockMultiShardCb(ShardId sid) const {
  GetDbSlice(sid).Release(LockMode(), lock_args_[sid]);
}


void Transaction::startMulti() {
  state_ = State::MULTI;  
}

void Transaction::FinishOrDiscardMulti() {
  // TODO
  state_ = State::UnMULTI;
}
void Transaction::CollectCommands(const CommandId* cid, CmdArgList args) {
  std::vector<std::string_view> ViewArgs(args.begin(), args.end());
  multi_.commands.push_back({cid, std::move(ViewArgs)});
}

cppcoro::task<> Transaction::CollectMutex() {
  for (auto& [cid, args] : multi_.commands) {
    cid_ = cid;
    full_args_ = args;
    InitSlice();
    SlicesArgs slices_args;
    slices_args.unique_shard_id = unique_shard_id_;
    slices_args.unique_shard_cnt = unique_shard_cnt_;
    slices_args.key_num = key_num_;
    slices_args.slices = std::move(Slices_);
    multi_.slices_args.push_back(std::move(slices_args));
  }
  std::vector<absl::flat_hash_set<LockFp>> fps(shard_set->size());
  uint32_t active_cnt = 0;
  for (auto& slices_args : multi_.slices_args) {
    for (uint i = 0; i < shard_set->size(); ++i) {
      auto& slice = slices_args.slices[i];
      if (!(slice.local_mask & ACTIVE))
      {
        continue;
      }
      SliceHopCount_[i]++; // 可能和初始化的计数冲突，会多计数1
      for (auto key_id : slice.keyIds)
      {
        fps[i].insert(Fingerprint(full_args_[key_id]));
      }
      slice.local_mask |= KEYLOCK_ACQUIRED; // 这里提前设计了获得锁，要注意一下
    }
  }

  for (int i = 0; i < shard_set->size(); ++i)
  {
    std::vector<LockFp> fps_vec;
    fps_vec.reserve(fps[i].size());
    for (const auto& fp : fps[i]) { 
        fps_vec.emplace_back(fp);
    }
    lock_args_[i].fps = std::move(fps_vec);
  }
  util::BlockingCounter counter(active_cnt);
  auto sid = EngineShard::tlocal()->shard_id();
  struct TryAcquire {
      ShardId sid;
      util::BlockingCounter counter;
      dfly::Transaction* tx;
      
      void operator()() {
          auto* e = EngineShard::tlocal();
          auto& db_slice = tx->namespace_->GetDbSlice(sid);
          if (!tx->LockMultiShardCb(sid)) {
              shard_set->Add(sid, *this);
          } else {
              tx->GetPqPos(sid) = e->txq()->SwapBack(tx, tx->GetPqPos(sid));
              counter->Dec();
          }
      }
  };
  TryAcquire try_acquire = {sid, counter, this};

  for (int i = 0; i < shard_set->size(); ++i)
  {
    if (SliceHopCount_[i] == 0) continue;
    try_acquire();
  }
  co_await counter->Wait();
  co_return;
}

size_t Transaction::StartExec() {
  txid_ = txid_counter_.fetch_add(1, std::memory_order_relaxed);
  state_ = State::EXEC;
  return multi_.commands.size();
}

void Transaction::CollectAndSend(int fd, std::string&& s) {
  if (state_ != State::EXEC) {
    yy::net::sockets::send(fd, s.data(), s.size(), MSG_NOSIGNAL);      
  }else {
    multi_.Res.push_back(std::move(s));
    if (multi_.Res.size() == multi_.commands.size()) {
      auto str = BuildMultiArray(multi_.Res);
      yy::net::sockets::send(fd, str.data(), str.size(), MSG_NOSIGNAL);
    }
  }
}



} 

// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "transaction_layer/transaction.hpp"

#include <bitset>
#include <latch>

#include "network/redis_server.hpp"

namespace dfly {
using namespace std;

thread_local Transaction::TLTmpSpace Transaction::tmp_space;

namespace {

std::atomic_uint64_t op_seq{1};
[[maybe_unused]] std::atomic_uint64_t ids{1};
constexpr size_t kTransSize [[maybe_unused]] = sizeof(Transaction);

struct ScheduleContext {
  util::intrusive_ptr<Transaction> trans;
  bool optimistic_execution = false;

  std::atomic_uint32_t fail_cnt{0};

  ScheduleContext(util::intrusive_ptr<Transaction> t, bool optimistic)
      : trans(std::move(t)), optimistic_execution(optimistic) {}
};

}  // namespace

Transaction::Transaction(const CommandId* cid) : cid_{cid} {
#ifndef NDEBUG
  id = ids.fetch_add(1);
#endif
  InitTxTime();
}
void Transaction::InitTxTime() { time_now_ms_ = util::GetCurrentTimeMs(); }

OpStatus Transaction::InitByArgs(const Namespace* ns, DbIndex index,
                                 CmdArgList args) {
  InitBase(ns, index, args);

  DCHECK_EQ(unique_shard_cnt_, 0u);
  DCHECK(args_slices_.empty());
  DCHECK(kv_fp_.empty());

  OpResult<KeyIndex> key_index = DetermineKeys(cid_, args);
  if (!key_index) return key_index.status();

  // 全局事务（如 SAVE）：激活所有分片，无 key、无 key 锁，
  // 由 ScheduleInShard 在每分片 shard_lock() 上获取分片锁。
  if (cid_->opt_mask() & CO::GLOBAL_TRANS) {
    InitGlobal();
    return OpStatus::OK;
  }

  InitByKeys(*key_index);
  return OpStatus::OK;
}

// /*管道优化专用*/ 复用 PipelineSquasher 预计算的 KeyIndex 与分片，
// 跳过重复的 DetermineKeys()（调用方已保证非全局事务、且 key 有效）。
OpStatus Transaction::InitByArgs(const Namespace* ns, DbIndex index,
                                 CmdArgList args, const KeyIndex& key_index,
                                 ShardId sid) {
  InitBase(ns, index, args);

  DCHECK_EQ(unique_shard_cnt_, 0u);
  DCHECK(args_slices_.empty());
  DCHECK(kv_fp_.empty());

  InitByKeys(key_index, sid);
  return OpStatus::OK;
}

// /*管道优化专用*/ 压缩器复用同一分片事务顺序执行多条命令。每条命令执行前
// 调用本方法，重置上一条命令遗留的运行时状态，使 InitByArgs / SingleHopAsync
// 可被安全重复调用，省去每条命令各分配一个 Transaction 对象的内存开销。
void Transaction::PrepareForReuse(const CommandId* cid) {
  DCHECK_EQ(blocking_count_, 0u);  // 上一条命令已执行完毕
  DCHECK_EQ(run_barrier_->GetCount(), 0u);
  cid_ = cid;
  shard_data_.clear();
  args_slices_.clear();
  key_slices_.clear();
  kv_fp_.clear();
  cb_ptr_.reset();
  txid_ = 0;
  unique_shard_cnt_ = 0;
  unique_shard_id_ = kInvalidSid;
  global_ = false;
  coordinator_state_ = 0;
  owner_shard_id_ = kInvalidSid;
  handle_ = {};
  need_resume.store(false, std::memory_order_relaxed);
  resume_count_.store(false, std::memory_order_relaxed);
  InitTxTime();
}

void Transaction::InitGlobal() {
  global_ = true;
  EnableAllShards();
}

void Transaction::EnableAllShards() {
  unique_shard_cnt_ = shard_set->size();
  unique_shard_id_ = unique_shard_cnt_ == 1 ? 0 : kInvalidSid;
  shard_data_.resize(unique_shard_cnt_);
  for (auto& sd : shard_data_) {
    sd.local_mask |= ACTIVE;
  }
}

void Transaction::InitBase(const Namespace* ns, DbIndex dbid, CmdArgList args) {
  db_index_ = dbid;
  full_args_ = args;
  namespace_ = ns;
}

OpResult<KeyIndex> DetermineKeys(const CommandId* cid, CmdArgList args) {
  if (cid->opt_mask() & (CO::GLOBAL_TRANS | CO::NO_KEY_TRANSACTIONAL))
    return KeyIndex{};

  int num_custom_keys [[maybe_unused]] = -1;

  unsigned start = 0, end = 0, step = 0;

  if (cid->first_key_pos() > 0) {
    start = cid->first_key_pos();
    int8_t last = cid->last_key_pos();
    end = last > 0 ? (unsigned)(last + 1)
                   : (unsigned)(int(args.size()) + last + 1);
    if (cid->interleaved_step()) {
      step = cid->interleaved_step();
    } else {
      step = 1;
    }
    return KeyIndex{start, end, step};
  }
  LOG(FATAL) << "TBD: Not supported " << cid->name();
  return {};
}

void Transaction::InitByKeys(const KeyIndex& key_index) {
  InitByKeys(key_index, kInvalidSid);
}

// /*管道优化专用*/ 单 key 路径复用外部预计算的分片 sid，跳过 Shard()
// 哈希/取模。
void Transaction::InitByKeys(const KeyIndex& key_index, ShardId sid) {
  if ((key_index.end - key_index.start) == 0)  // 不确定
    return;
  DCHECK_LT(key_index.start, full_args_.size());

  if ((key_index.NumArgs() == 1)) {
    StoreKeysInArgs(key_index);
    unique_shard_cnt_ = 1;
    if (sid == kInvalidSid) {
      string_view akey = full_args_[*key_index];
      unique_shard_id_ = Shard(akey, shard_set->size());
    } else {
      unique_shard_id_ = sid;
    }
    shard_data_.resize(1);
    shard_data_[SidToId(unique_shard_id_)].local_mask |= ACTIVE;
    // 填充 key_slices_ 供回调中 GetSlice() 使用
    key_slices_.resize(shard_data_.size());
    auto& sl = key_slices_[0];
    sl.trans_ = this;
    sl.sid_ = unique_shard_id_;
    sl.slices_ =
        std::span<const IndexSlice>(args_slices_.data(), args_slices_.size());
    sl.step_ = key_index.step;
    sl.args_ = full_args_;
    return;
  }

  shard_data_.resize(shard_set->size());

  auto& shard_index = tmp_space.GetShardIndex(shard_data_.size());

  BuildShardIndex(key_index, &shard_index);

  InitShardData(shard_index, key_index.NumArgs());

  if (unique_shard_cnt_ == 1) {
    PerShardData* sd;
    shard_data_.resize(1);
    sd = &shard_data_.front();
    sd->local_mask |= ACTIVE;
    sd->slice_count = -1;
    sd->slice_start = -1;
  }
}

std::vector<Transaction::PerShardCache>& Transaction::TLTmpSpace::GetShardIndex(
    unsigned size) {
  shard_cache.resize(size);
  for (auto& v : shard_cache) v.Clear();
  return shard_cache;
}

void Transaction::BuildShardIndex(const KeyIndex& key_index,
                                  std::vector<PerShardCache>* out) {
  auto& shard_index = *out;
  for (unsigned i : key_index.Range()) {
    string_view key = full_args_[i];

    ShardId sid = Shard(key, shard_data_.size());
    unsigned step = key_index.step;
    shard_index[sid].key_step = step;
    auto& slices = shard_index[sid].slices;
    if (!slices.empty() && slices.back().second == i) {
      slices.back().second = i + step;
    } else {
      slices.emplace_back(i, i + step);
    }
  }
}

void Transaction::InitShardData(std::span<const PerShardCache> shard_index,
                                size_t num_args) {
  args_slices_.reserve(num_args);
  DCHECK(kv_fp_.empty());
  kv_fp_.reserve(num_args);

  for (size_t i = 0; i < shard_data_.size(); ++i) {
    auto& sd = shard_data_[i];
    const auto& src = shard_index[i];

    sd.slice_count = src.slices.size();
    sd.slice_start = args_slices_.size();
    sd.fp_start = kv_fp_.size();
    sd.fp_count = 0;

    if (sd.slice_count == 0) continue;

    sd.local_mask |= ACTIVE;

    unique_shard_cnt_++;
    unique_shard_id_ = i;

    for (const auto& [start, end] : src.slices) {
      args_slices_.emplace_back(start, end);
      for (string_view key :
           KeyIndex(start, end, src.key_step).Range(full_args_)) {
        kv_fp_.push_back(LockTag(key).Fingerprint());
        sd.fp_count++;
      }
    }
  }

  // 为 GetSlice() 构建每个 shard 的 Slice 视图
  key_slices_.resize(shard_data_.size());
  for (size_t i = 0; i < shard_data_.size(); ++i) {
    if (shard_data_[i].slice_count == 0) continue;
    auto& sl = key_slices_[i];
    sl.trans_ = this;
    sl.sid_ = static_cast<ShardId>(i);
    sl.slices_ = std::span<const IndexSlice>(
        args_slices_.data() + shard_data_[i].slice_start,
        shard_data_[i].slice_count);
    sl.step_ = shard_index[i].key_step;
    sl.args_ = full_args_;
  }
}

void Transaction::StoreKeysInArgs(const KeyIndex& key_index) {
  args_slices_.emplace_back(key_index.start, key_index.end);
  for (string_view key : key_index.Range(full_args_))
    kv_fp_.push_back(LockTag(key).Fingerprint());
}

Transaction::~Transaction() { DCHECK_EQ(std::uncaught_exceptions(), 0); }

std::pair<uint16_t, bool> Transaction::DisarmInShardWhen(
    ShardId sid, uint16_t relevant_flags) {
  auto& sd = shard_data_[SidToId(sid)];
  if (sd.is_armed.load(memory_order_acquire)) {
    bool relevant = sd.local_mask & relevant_flags;
    if (relevant) CHECK(sd.is_armed.exchange(false, memory_order_release));
    return {sd.local_mask, relevant};
  }
  return {0, false};
}

uint16_t Transaction::DisarmInShard(ShardId sid) {
  auto& sd = shard_data_[SidToId(sid)];
  return sd.is_armed.exchange(false, memory_order_acquire) ? sd.local_mask : 0;
}

cppcoro::AsyncTask Transaction::SingleHopAsync(RunnableType cb,
                                               std::coroutine_handle<> handle) {
  VLOG(3) << "命令:" << CmdArgListToString(full_args_)
          << " shard_cnt:" << unique_shard_cnt_;
  coordinator_state_ |= COORD_CONCLUDING;
  cb_ptr_ = cb;
  InitBlockingController(handle, unique_shard_cnt_);
  if (unique_shard_cnt_ == 1) {
    VLOG(4) << "SingleHopAsync unique_shard_cnt_ == 1";
    CHECK_EQ(shard_data_.size(), 1u);
    shard_data_.front().is_armed.store(true, memory_order_relaxed);
    run_barrier_->Add(1);
    auto shard_cb = [self = intrusive_ptr_from_this()]() {
      bool success = self->ScheduleInShard(EngineShard::tlocal(), true);
      CHECK(success);
      if (self->shard_data_.front().local_mask & OPTIMISTIC_EXECUTION) {
        VLOG(4) << "SingleHopAsync OPTIMISTIC_EXECUTION";
        self->run_barrier_->Dec();
      } else {
        EngineShard::tlocal()->PollExecution(self);
      }
    };
    if (CanRunInlined()) {
      VLOG(4) << "SingleHopAsync CanRunInlined";
      shard_cb();

    } else {
      VLOG(4) << "SingleHopAsync not CanRunInlined";
      shard_set->Add(unique_shard_id_, shard_cb);
    }
    co_await run_barrier_->Wait();
  } else {
    VLOG(3) << "SingleHopAsync unique_shard_cnt_ > 1";
    co_await ScheduleInternal();
    DispatchHop();
  }
  ResumeIfNeed();

  co_return;
}

cppcoro::task<> Transaction::ScheduleInternal() {
  bool optimistic_exec =
      (coordinator_state_ & COORD_CONCLUDING) &&
      (unique_shard_cnt_ == 1 || (cid_->opt_mask() & CO::IDEMPOTENT));  // 幂等
  auto is_active = [this](uint32_t i) { return IsActive(i); };

  IterateActiveShards([](auto& sd, auto /*i*/) {
    DCHECK_EQ(sd.local_mask & KEYLOCK_ACQUIRED, 0);
  });
#ifdef UNIT_TESTS
  LOG(INFO) << " 事务 " << id << " 开始 ScheduleInternal"
            << " 分片数:" << unique_shard_cnt_ << " 幂等:" << optimistic_exec;
#endif
  while (true) {
    if (unique_shard_cnt_ > 1)
      txid_ = op_seq.fetch_add(1, memory_order_relaxed);
    run_barrier_->Start(unique_shard_cnt_);

    if (CanRunInlined()) {
#ifdef UNIT_TESTS
      LOG(INFO) << " 事务 " << id << " 可以内联执行";
#endif
      [[maybe_unused]] bool success =
          ScheduleInShard(EngineShard::tlocal(), optimistic_exec);
      DCHECK(success);
      run_barrier_->Dec();
      break;
    }
    ScheduleContext schedule_ctx{intrusive_ptr_from_this(), optimistic_exec};
    auto cb = [&schedule_ctx] {
      if (!schedule_ctx.trans->ScheduleInShard(
              EngineShard::tlocal(), schedule_ctx.optimistic_execution)) {
        schedule_ctx.fail_cnt.fetch_add(1, memory_order_relaxed);
      }
      schedule_ctx.trans->FinishHop();
    };
    if (unique_shard_cnt_ == 1) {
#ifdef UNIT_TESTS
      LOG(INFO) << " 事务 " << id << " 唯一 shard 执行";
#endif
      shard_set->Add(unique_shard_id_, cb);
    } else {
      IterateActiveShards(
          [cb](const auto& /*sd*/, ShardId i) { shard_set->Add(i, cb); });
    }
    co_await run_barrier_->Wait();
    if (schedule_ctx.fail_cnt.load(memory_order_relaxed) == 0) {
      break;
    }
#ifdef UNIT_TESTS
    LOG(INFO) << " 事务 " << id << " 重试";
#endif
    atomic_bool should_poll_execution{false};
    run_barrier_->Start(unique_shard_cnt_);

    auto cancel = [&, self = intrusive_ptr_from_this()](EngineShard* shard) {
      bool res = self->CancelShardCb(shard);
      if (res) {
        should_poll_execution.store(true, memory_order_relaxed);
      }
      self->run_barrier_->Dec();
    };
    shard_set->DispatchBriefInParallel(std::move(cancel), is_active);
    co_await run_barrier_->Wait();
    if (should_poll_execution.load(memory_order_relaxed)) {
      IterateActiveShards([](const auto& /*sd*/, auto i) {
        shard_set->Add(i,
                       [] { EngineShard::tlocal()->PollExecution(nullptr); });
      });
    }
    InitTxTime();
  }
  coordinator_state_ |= COORD_SCHED;

  co_return;
}

bool Transaction::ScheduleInShard(EngineShard* shard, bool execute_optimistic) {
  ShardId sid = SidToId(shard->shard_id());
  auto& sd = shard_data_[sid];

  VLOG(3) << "ScheduleInShard 在分片:" << shard->shard_id()
          << " sd掩码:" << sd.local_mask;
  DCHECK_EQ(sd.local_mask & KEYLOCK_ACQUIRED, 0);
  sd.local_mask &= ~(OUT_OF_ORDER | OPTIMISTIC_EXECUTION);

  TxQueue* txq = shard->txq();
  KeyLockArgs lock_args;
  IntentLock::Mode mode = LockMode();
  bool lock_granted = false;
  if (txid_ > 0 && shard->committed_txid() >= txid_) return false;

  auto release_fp_locks = [&]() {
    if (IsGlobal()) {
      shard->shard_lock()->Release(mode);
    } else {
      GetDbSlice(shard->shard_id()).Release(mode, lock_args);
    }
    sd.local_mask &= ~KEYLOCK_ACQUIRED;
    VLOG(4) << "LOCK 释放, 在分片:" << shard->shard_id()
            << " 由回调release_fp_locks清理";
  };
  lock_args = GetLockArgs(
      shard
          ->shard_id());  // 当分片的锁过多时可以选择直接加锁失败，入队，减少加锁循环开销
  const bool keys_unlocked =
      GetDbSlice(shard->shard_id()).Acquire(mode, lock_args);
  // 全局事务无 key，lock_granted 取决于分片锁是否无竞争获取成功。
  bool shard_unlocked = true;
  if (IsGlobal()) {
    shard_unlocked = shard->shard_lock()->Acquire(mode);
  }
  lock_granted = keys_unlocked && shard_unlocked;

  sd.local_mask |= KEYLOCK_ACQUIRED;
  VLOG(4) << "LOCK 获取, 在分片:" << shard->shard_id();
  if (lock_granted) {
    VLOG(4) << "ScheduleInShard lock_granted";
    sd.local_mask |= OUT_OF_ORDER;
  }
  if (lock_granted && execute_optimistic) {
    sd.local_mask |= OPTIMISTIC_EXECUTION;
#ifdef UNIT_TESTS
    LOG(INFO) << " 事务 " << id << " ScheduleInShard execute_optimistic ";
#endif
    RunCallback(shard);
    if (coordinator_state_ & COORD_CONCLUDING) {
      release_fp_locks();
      return true;
    }
  }

  if (txid_ == 0) {
    DCHECK_EQ(unique_shard_cnt_, 1u);
    txid_ = op_seq.fetch_add(1, memory_order_relaxed);
    DCHECK_GT(txid_, shard->committed_txid());
  }

  if (!txq->Empty() && txid_ < txq->Back()->txid() && !lock_granted) {
    if (sd.local_mask & KEYLOCK_ACQUIRED) {
      release_fp_locks();
    }
    return false;
  }
#ifdef UNIT_TESTS
  LOG(INFO) << "事务 " << id << " 在分片:" << shard->shard_id() << " 入队";
#endif
  TxQueue::Iterator it = txq->Push(intrusive_ptr_from_this());
  DCHECK_EQ(TxQueue::kEnd, sd.pq_pos);
  sd.pq_pos = it;
  return true;
}

void Transaction::DispatchHop() {
  std::bitset<1024> poll_flags(0);
  unsigned run_cnt = 0;
  IterateActiveShards([&poll_flags, &run_cnt](auto& sd, auto i) {
    if ((sd.local_mask & OPTIMISTIC_EXECUTION) == 0) {
      run_cnt++;
      poll_flags.set(i, true);
    }
    sd.local_mask &= ~OPTIMISTIC_EXECUTION;
  });

  DCHECK_EQ(run_cnt, poll_flags.count());
  if (run_cnt == 0) return;
  std::atomic_thread_fence(memory_order_release);
  IterateActiveShards([&poll_flags](auto& sd, auto i) {
    if (poll_flags.test(i)) sd.is_armed.store(true, memory_order_relaxed);
  });

  if (CanRunInlined()) {
    EngineShard::tlocal()->PollExecution(intrusive_ptr_from_this());
    return;
  }

  auto poll_cb = [self = intrusive_ptr_from_this()] {
    CHECK(self->namespace_ != nullptr);
    EngineShard::tlocal()->PollExecution(self);
  };
  IterateShards([&poll_cb, &poll_flags](PerShardData& /*sd*/, auto i) {
    if (poll_flags.test(i)) shard_set->Add(i, poll_cb);
  });
}

bool Transaction::CanRunInlined() const {
  auto* es = EngineShard::tlocal();
  if (unique_shard_cnt_ == 1 && unique_shard_id_ == es->shard_id()) {
    return true;
  }
  return false;
}

bool Transaction::RunInShard(EngineShard* shard) {
  DCHECK_GT(txid_, 0u);
  unsigned idx = SidToId(shard->shard_id());
  auto& sd = shard_data_[idx];

  IntentLock::Mode mode = LockMode();
  RunCallback(shard);
  bool is_concluding = coordinator_state_ & COORD_CONCLUDING;
  if (sd.pq_pos != TxQueue::kEnd) {
    shard->txq()->Pop(sd.pq_pos);
    sd.pq_pos = TxQueue::kEnd;
  }
  if (is_concluding) {
    VLOG(4) << "准备清理lock, 分片:" << shard->shard_id();
    DCHECK(sd.local_mask & KEYLOCK_ACQUIRED);
    if (IsGlobal()) {
      shard->shard_lock()->Release(mode);
    } else {
      KeyLockArgs largs;
      largs = GetLockArgs(idx);
      GetDbSlice(shard->shard_id()).Release(mode, largs);
    }
    sd.local_mask &= ~KEYLOCK_ACQUIRED;
    sd.local_mask &= ~OUT_OF_ORDER;
  }
  FinishHop();
#ifdef UNIT_TESTS
  LOG(INFO) << "事务 " << id << " 在分片:" << shard->shard_id()
            << " RunInShard";
#endif
  ResumeIfNeed();
  return is_concluding;
}

/*需要上下文调试时可以加context字段*/
void Transaction::RunCallback(EngineShard* shard) {
  (*cb_ptr_)(this, shard);
  if (unique_shard_cnt_ == 1) {
    cb_ptr_.reset();
  }
  VLOG(4) << "RunCallback!";
  if (blocking_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    need_resume.store(true, std::memory_order_release);
  }
}

bool Transaction::CancelShardCb(EngineShard* shard) {
  ShardId idx = SidToId(shard->shard_id());
  auto& sd = shard_data_[idx];
  TxQueue::Iterator q_pos = exchange(sd.pq_pos, TxQueue::kEnd);
  if (q_pos == TxQueue::kEnd) {
    DCHECK_EQ(sd.local_mask & KEYLOCK_ACQUIRED, 0);
    return false;
  }
  TxQueue* txq = shard->txq();
  bool was_head = txq->Front()->GetTxQueuePos(idx) == q_pos;
  txq->Pop(q_pos);
  auto lock_args = GetLockArgs(shard->shard_id());
  DCHECK(sd.local_mask & KEYLOCK_ACQUIRED);
  if (IsGlobal()) {
    shard->shard_lock()->Release(LockMode());
  } else {
    DCHECK(!lock_args.fps.empty());
    GetDbSlice(shard->shard_id()).Release(LockMode(), lock_args);
  }

  sd.local_mask &= ~KEYLOCK_ACQUIRED;
  return was_head && !txq->Empty();
}

void Transaction::FinishHop() { run_barrier_->Dec(); }

KeyLockArgs Transaction::GetLockArgs(ShardId sid) const {
  KeyLockArgs res;
  res.db_index = db_index_;

  if (unique_shard_cnt_ == 1) {
    res.fps.assign(kv_fp_.begin(), kv_fp_.end());
  } else {
    const auto& sd = shard_data_[sid];
    res.fps.assign(kv_fp_.begin() + sd.fp_start,
                   kv_fp_.begin() + sd.fp_start + sd.fp_count);
  }
  return res;
}

string_view Transaction::Name() const {
  return cid_ ? cid_->name() : "null-command";
}

ShardId Transaction::GetUniqueShard() const { return unique_shard_id_; }

bool Transaction::IsActive(ShardId sid) const {
  if (unique_shard_cnt_ == 0) return false;
  if (unique_shard_cnt_ == 1) {
    return sid == unique_shard_id_;
  }

  return shard_data_[SidToId(sid)].local_mask & ACTIVE;
}

IntentLock::Mode Transaction::LockMode() const {
  return (cid_->opt_mask() & CO::READONLY) ? IntentLock::SHARED
                                           : IntentLock::EXCLUSIVE;
}

DbSlice& Transaction::GetDbSlice(ShardId shard_id) const {
  CHECK(namespace_ != nullptr);
  return namespace_->GetDbSlice(shard_id);
}

}  // namespace dfly

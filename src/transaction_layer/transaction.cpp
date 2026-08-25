#include "transaction_layer/transaction.hpp"

#include <algorithm>
#include <bitset>

#include "detail/common_types.hpp"
#include "sharding/shard_pool.hpp"
#include "util/Time.hpp"

namespace dfly {

namespace {

std::atomic_uint64_t global_seq{1};

struct KeyAccum {
  std::vector<std::string_view> keys;
  std::vector<IndexSlice> slices;
};
thread_local std::vector<KeyAccum> key_accum;

struct AccumGuard {
  ~AccumGuard() {
    for (auto& a : key_accum) {
      a.keys.clear();
      a.slices.clear();
    }
  }
};

}  // namespace

Transaction::Transaction(const CommandId* cid) : cid_(cid) {}

Transaction::~Transaction() {
  if (active_shard_count_ > 1) {
    involved_.many.sds.~vector();
    involved_.many.sids.~vector();
  } else if (active_shard_count_ == 1) {
    involved_.single.sd.~PerShardData();
  }
}

void Transaction::SetStartTime() { start_ms_ = util::GetCurrentTimeMs(); }

void Transaction::ResetForReuse(const CommandId* cid) {
  if (active_shard_count_ > 1) {
    involved_.many.sds.~vector();
    involved_.many.sids.~vector();
  } else if (active_shard_count_ == 1) {
    involved_.single.sd.~PerShardData();
  }
  new (&involved_) InvolvedData{};
  active_shard_count_ = 0;
  cid_ = cid;
  db_ = 0;
  args_ = {};
  start_ms_ = 0;
  txid_ = 0;
  global_ = false;
  coro_ctx_.Clear();
  barrier_->Start(0);
  state_ &= TxState::kPipeline;
}

void Transaction::Init(::dfly::DbIndex db, ::dfly::CmdArgList args) {
  InitBase(db, args);

  if (cid_->opt_mask() & CO::GLOBAL_TRANS) {
    MarkAllShards();
    return;
  }
  BuildKeyMap();
  return;
}

bool Transaction::IsGlobal() const {
  return active_shard_count_ == shard_pool->size();
}

void Transaction::InitBase(::dfly::DbIndex db, ::dfly::CmdArgList args) {
  db_ = db;
  args_ = args;
  SetStartTime();
}

void Transaction::MarkAllShards() {
  AccumGuard guard;
  active_shard_count_ = 0;
  auto& sids = ActivateMany();
  sids.clear();
  for (ShardId sid = 0; sid < static_cast<ShardId>(shard_pool->size()); ++sid)
    sids.push_back(sid);
  involved_.many.sds.resize(sids.size());
  active_shard_count_ = static_cast<uint32_t>(sids.size());
}

void Transaction::BuildKeyMap() {
  AccumGuard guard;
  key_accum.resize(shard_pool->size());

  active_shard_count_ = 0;
  key_num_ = 0;

  ShardId sole = kInvalidSid;
  const unsigned step = cid_->key_step();
  for (const auto& [key, pos] : cid_->Keys(args_)) {
    const ShardId sid = ShardIndex(key, shard_pool->size());
    auto& acc = key_accum[sid];
    if (acc.keys.empty()) {
      if (active_shard_count_ == 0) sole = sid;
      ++active_shard_count_;
    }
    if (!acc.slices.empty() && acc.slices.back().second == pos) {
      acc.slices.back().second = pos + step;
    } else {
      acc.slices.emplace_back(pos, pos + step);
    }
    acc.keys.push_back(key);
  }
  if (active_shard_count_ == 0) return;
  if (active_shard_count_ == 1) {
    involved_.single.sid = sole;
  } else {
    auto& sids = ActivateMany();
    sids.clear();
    for (ShardId sid = 0; sid < shard_pool->size(); ++sid)
      if (!key_accum[sid].keys.empty()) sids.push_back(sid);
    involved_.many.sds.resize(sids.size());
  }

  auto fill = [&](size_t idx, ShardId sid) {
    auto& acc = key_accum[sid];
    PerShardData& sd = ShardDataAt(idx);
    sd.slices = std::move(acc.slices);
    sd.fps.reserve(acc.keys.size());
    key_num_ += acc.keys.size();
    for (const std::string_view k : acc.keys)
      sd.fps.push_back(KeyFingerprint(k));
  };
  if (active_shard_count_ == 1) {
    fill(0, sole);
  } else {
    for (size_t idx = 0; idx < involved_.many.sids.size(); ++idx)
      fill(idx, involved_.many.sids[idx]);
  }
}

size_t Transaction::IndexInvolved(ShardId sid) const {
  if (active_shard_count_ == 1) {
    DCHECK_EQ(sid, involved_.single.sid) << "shard " << sid << " not sole";
    return 0;
  }
  const auto& sids = involved_.many.sids;
  const auto it = std::lower_bound(sids.begin(), sids.end(), sid);
  DCHECK(it != sids.end() && *it == sid) << "shard " << sid << " not involved";
  return static_cast<size_t>(it - sids.begin());
}

bool Transaction::IsInvolved(ShardId sid) const {
  if (active_shard_count_ == 1) return sid == involved_.single.sid;
  const auto& sids = involved_.many.sids;
  return std::binary_search(sids.begin(), sids.end(), sid);
}

ShardId Transaction::InvolvedAt(size_t idx) const {
  return active_shard_count_ == 1 ? involved_.single.sid
                                  : involved_.many.sids[idx];
}

Transaction::PerShardData& Transaction::ShardDataAt(size_t idx) {
  return active_shard_count_ == 1 ? involved_.single.sd
                                  : involved_.many.sds[idx];
}

const Transaction::PerShardData& Transaction::ShardDataAt(size_t idx) const {
  return active_shard_count_ == 1 ? involved_.single.sd
                                  : involved_.many.sds[idx];
}

std::vector<ShardId>& Transaction::ActivateMany() {
  new (&involved_.many.sids) std::vector<ShardId>();
  new (&involved_.many.sds) std::vector<PerShardData>();
  return involved_.many.sids;
}

bool Transaction::CanRunInlined() const {
  auto* es = Shard::tlocal();
  return active_shard_count_ == 1 && involved_.single.sid == es->shard_id();
}

cppcoro::AsyncTask Transaction::Run(Callback cb,
                                    std::coroutine_handle<> resume) {
  DCHECK_GT(active_shard_count_, 0u);
  state_ |= TxState::kScheduling;  // 调度中

  coro_ctx_.Start(std::move(cb), resume, Shard::tlocal()->shard_id(),
                  active_shard_count_);

  if (active_shard_count_ == 1) {
    ShardDataAt(0).is_armed.store(true, std::memory_order_relaxed);
    barrier_->Add(1);
    auto hop = [self = intrusive_ptr_from_this()]() {
      ScheduleResult res = self->ScheduleOnShard(*Shard::tlocal(), true);
      CHECK(res != ScheduleResult::kRejected) << "single-shard tx rejected";
      if (res != ScheduleResult::kGranted) {
        Shard::tlocal()->DriveQueue(self);
      }
      self->barrier_->Dec();
    };
    if (CanRunInlined()) {
      hop();
    } else {
      shard_pool->Post(involved_.single.sid, hop);
    }
    co_await barrier_->Wait();
  } else {
    co_await Schedule();
    state_ |= TxState::kDistributed;
    Distribute();
    state_ &= ~TxState::kDistributed;
  }
  state_ &= ~TxState::kScheduling;
  ResumeIfNeed();
  co_return;
}

cppcoro::task<> Transaction::Schedule() {
  const bool allow_optimistic = (cid_->opt_mask() & CO::IDEMPOTENT) != 0;
  while (true) {
    txid_ = global_seq.fetch_add(1, std::memory_order_relaxed);
    barrier_->Start(active_shard_count_);
    std::atomic<uint32_t> fail_cnt{0};
    auto hop = [this, &fail_cnt, allow_optimistic]() {
      ScheduleResult res = ScheduleOnShard(*Shard::tlocal(), allow_optimistic);
      if (res == ScheduleResult::kRejected)
        fail_cnt.fetch_add(1, std::memory_order_relaxed);
      barrier_->Dec();
    };
    for (size_t i = 0; i < active_shard_count_; ++i)
      shard_pool->Post(InvolvedAt(i), hop);
    co_await barrier_->Wait();

    if (fail_cnt.load(std::memory_order_relaxed) == 0) break;

    std::atomic<bool> need_poll{false};
    barrier_->Start(active_shard_count_);
    for (size_t i = 0; i < active_shard_count_; ++i) {
      const ShardId sid = InvolvedAt(i);
      shard_pool->Post(sid, [this, &need_poll]() {
        if (RollbackOnShard(*Shard::tlocal()))
          need_poll.store(true, std::memory_order_relaxed);
        barrier_->Dec();
      });
    }
    co_await barrier_->Wait();

    // 队首被移走：通知相关分片重新驱动队列
    if (need_poll.load(std::memory_order_relaxed)) {
      for (size_t i = 0; i < active_shard_count_; ++i)
        shard_pool->Post(InvolvedAt(i),
                         [] { Shard::tlocal()->DriveQueue(nullptr); });
    }
    SetStartTime();
  }
  co_return;
}

void Transaction::Distribute() {
  std::bitset<1024> poll;
  uint32_t cnt = 0;
  for (size_t i = 0; i < active_shard_count_; ++i) {
    PerShardData& sd = ShardDataAt(i);
    if (sd.flags & kOptimistic) {
      sd.flags &= ~kOptimistic;
      continue;
    }
    poll.set(i, true);
    ++cnt;
  }
  if (cnt == 0) return;
  for (size_t i = 0; i < active_shard_count_; ++i) {
    if (poll.test(i))
      ShardDataAt(i).is_armed.store(true, std::memory_order_release);
  }

  auto poll_cb = [self = intrusive_ptr_from_this()]() {
    Shard::tlocal()->DriveQueue(self);
  };
  if (CanRunInlined()) {
    Shard::tlocal()->DriveQueue(intrusive_ptr_from_this());
  } else {
    for (size_t i = 0; i < active_shard_count_; ++i) {
      if (poll.test(i)) shard_pool->Post(InvolvedAt(i), poll_cb);
    }
  }
}

Transaction::ScheduleResult Transaction::ScheduleOnShard(
    Shard& shard, bool allow_optimistic) {
  const ShardId sid = shard.shard_id();
  PerShardData& sd = ShardDataAt(IndexInvolved(sid));

  sd.flags &= ~(kOptimistic | kUncontended);
  if (txid_ > 0 && shard.CommittedTxId() >= txid_)
    return ScheduleResult::kRejected;

  const bool granted = AcquireLocks(shard, sd);
  if (granted) {
    sd.flags |= kUncontended;
  }

  if (granted && allow_optimistic) {
    sd.flags |= kOptimistic;
    InvokeCallback(shard);
    ReleaseLocks(shard, sd);
    sd.flags &= ~kUncontended;
    return ScheduleResult::kGranted;
  }

  if (txid_ == 0) {
    txid_ = global_seq.fetch_add(1, std::memory_order_relaxed);
  }

  TxQueue& queue = shard.Queue();

  if (!queue.Empty() && txid_ < queue.Back()->txid() && !granted) {
    ReleaseLocks(shard, sd);
    return ScheduleResult::kRejected;
  }
  // 队列高水位（论文 §2.1）：多分片未拿全锁且队列堆积 → 拒绝入队，
  // 把 CPU 让给队首推进 / SCA 消化队列
  if (active_shard_count_ > 1 && !granted &&
      queue.Size() >= Shard::kQueueHighWater) {
    ReleaseLocks(shard, sd);
    return ScheduleResult::kRejected;
  }
  // 入队：等待队首引理 / SCA 放行
  sd.queue_pos = queue.Push(intrusive_ptr_from_this());
  return ScheduleResult::kQueued;
}

void Transaction::ExecuteOnShard(Shard& shard) {
  const ShardId sid = shard.shard_id();
  PerShardData& sd = ShardDataAt(IndexInvolved(sid));

  DCHECK(sd.queue_pos != TxQueue::kEnd);

  shard.Queue().Pop(sd.queue_pos);
  sd.queue_pos = TxQueue::kEnd;

  InvokeCallback(shard);
  sd.flags &= ~kUncontended;
  ReleaseLocks(shard, sd);
  ResumeIfNeed();
}

bool Transaction::RollbackOnShard(Shard& shard) {
  const ShardId sid = shard.shard_id();
  PerShardData& sd = ShardDataAt(IndexInvolved(sid));
  TxQueue::Iterator pos = sd.queue_pos;
  if (pos == TxQueue::kEnd) return false;

  TxQueue& queue = shard.Queue();
  const bool was_head = (pos == queue.Head());
  queue.Pop(pos);
  sd.queue_pos = TxQueue::kEnd;
  ReleaseLocks(shard, sd);
  return was_head && !queue.Empty();
}

bool Transaction::AllowOn(ShardId sid) {
  PerShardData& sd = ShardDataAt(IndexInvolved(sid));
  return sd.is_armed.exchange(false, std::memory_order_acq_rel);
}

bool Transaction::AllowOnIf(ShardId sid, uint16_t need_flags) {
  PerShardData& sd = ShardDataAt(IndexInvolved(sid));
  if (!(sd.flags & need_flags)) return false;
  return sd.is_armed.exchange(false, std::memory_order_acq_rel);
}

bool Transaction::IsAllowedOn(ShardId sid) const {
  const PerShardData& sd = ShardDataAt(IndexInvolved(sid));
  return sd.is_armed.load(std::memory_order_acquire);
}

DbContext Transaction::GetDbContext() const {
  return DbContext{db_, start_ms_};
}

Transaction::Slice Transaction::GetSlice(ShardId sid) const {
  const PerShardData& sd = ShardDataAt(IndexInvolved(sid));
  return Slice(std::span<const IndexSlice>(sd.slices), cid_->key_step(), args_);
}

KeyLockContext Transaction::LockArgsOn(ShardId sid) const {
  const PerShardData& sd = ShardDataAt(IndexInvolved(sid));
  return KeyLockContext{db_, sd.fps, LockMode()};
}

void Transaction::InvokeCallback(Shard& shard) {
  coro_ctx_.cb(this, &shard);
  coro_ctx_.FinishCallback();
}

void Transaction::CoroutineCtx::FinishCallback() {
  if (pending.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    need_resume.store(true, std::memory_order_release);
  }
}

void Transaction::ResumeIfNeed() {
  if (!coro_ctx_.need_resume.load(std::memory_order_acquire)) return;
  bool expected = false;
  if (coro_ctx_.has_resume.compare_exchange_strong(expected, true,
                                                   std::memory_order_acq_rel)) {
    state_ |= TxState::kFinished;
    auto* es = Shard::tlocal();
    if (es && es->shard_id() == coro_ctx_.owner_) {
      coro_ctx_.resume.resume();
    } else {
      shard_pool->Post(coro_ctx_.owner_,
                       [h = coro_ctx_.resume] { h.resume(); });
    }
  }
}

bool Transaction::AcquireLocks(Shard& shard, PerShardData& sd) {
  const IntentLock::Mode mode = LockMode();
  const KeyLockContext lock_args{db_, sd.fps, mode};
  const bool keys_free = shard.GetShardStorage().Acquire(mode, lock_args);
  const bool shard_free = IsGlobal() ? shard.ShardLock().Acquire(mode) : true;
  return keys_free && shard_free;
}

void Transaction::ReleaseLocks(Shard& shard, PerShardData& sd) {
  const IntentLock::Mode mode = LockMode();
  const KeyLockContext lock_args{db_, sd.fps, mode};
  if (IsGlobal()) {
    shard.ShardLock().Release(mode);
  } else {
    shard.GetShardStorage().Release(mode, lock_args);
  }
}

std::string Transaction::StateName() const {
  std::string name;
  auto append = [&](bool set, const char* s) {
    if (!set) return;
    if (!name.empty()) name += '|';
    name += s;
  };
  append(state_ & TxState::kPipeline, "pipeline");
  append(state_ & TxState::kScheduling, "scheduling");
  append(state_ & TxState::kDistributed, "distributed");
  append(state_ & TxState::kFinished, "finished");
  return name.empty() ? "none" : name;
}

}  // namespace dfly

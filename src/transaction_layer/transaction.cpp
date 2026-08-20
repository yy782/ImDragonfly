// ============================================================================
// transaction.cpp —— Transaction 调度协议实现（独立设计）
//
// 调度协议对应 VVL 论文：
//   - ApplyForLockOn：单分片"锁申请"（计数器递增 + 授予判定），等价论文图 1
//     的请求过程，入队前保证有序性（TxnQueue）与队列高水位约束；
//   - Schedule：多分片调度回合，失败（kRejected）即回滚重试，直到全部
//     分片申请成功；
//   - Distribute：调度成功后放行各分片（armed），投递队列驱动；
//   - DriveQueue（EngineShard 侧）：队首引理执行 + SCA 消化队列。
// ============================================================================
#include "transaction_layer/transaction.hpp"

#include <bitset>

#include "detail/common.hpp"
#include "sharding/db_slice.hpp"
#include "sharding/engine_shard_set.hpp"
#include "util/Time.hpp"

namespace dfly {

namespace {

// 全局事务序（论文 §2.1 TxnQueue 的排序依据）：每分片队列按此单调递增的
// 序列号插入，保证所有分片看到一致的串行化顺序。
std::atomic_uint64_t global_seq{1};

// 多 key 按分片聚合用的线程本地缓冲（BuildKeyMap 复用，避免反复分配）
struct KeyAccum {
  std::vector<std::string_view> keys;
};
thread_local std::vector<KeyAccum> key_accum;

// 回合结束自动回收聚合缓冲
struct AccumGuard {
  ~AccumGuard() {
    for (auto& a : key_accum) a.keys.clear();
  }
};

// 从命令描述提取参数中的 key 范围（Redis 命令的 first/last/step 约定）
OpResult<KeyIndex> DetermineKeys(const CommandId* cid, cmn::CmdArgList args) {
  if (cid->opt_mask() & (CO::GLOBAL_TRANS | CO::NO_KEY_TRANSACTIONAL))
    return KeyIndex{};

  if (cid->first_key_pos() <= 0) {
    LOG(FATAL) << "TBD: Not supported " << cid->name();
  }
  unsigned start = cid->first_key_pos();
  int8_t last = cid->last_key_pos();
  unsigned end = last > 0 ? unsigned(last + 1)
                          : unsigned(int(args.size()) + last + 1);
  unsigned step = cid->interleaved_step() ? cid->interleaved_step() : 1;
  return KeyIndex{start, end, step};
}

}  // namespace

Transaction::Transaction() : cid_(nullptr) {
  start_ms_ = util::GetCurrentTimeMs();
}

Transaction::Transaction(const CommandId* cid) : cid_(cid) {
  start_ms_ = util::GetCurrentTimeMs();
}

Transaction::~Transaction() = default;

OpStatus Transaction::Init(const Namespace* ns, DbIndex db, cmn::CmdArgList args) {
  InitBase(ns, db, args);

  if (cid_->opt_mask() & CO::GLOBAL_TRANS) {
    // 全局事务（如 SAVE）：覆盖所有分片，无 key 锁，锁取分片锁
    MarkAllShards();
    return OpStatus::OK;
  }
  OpResult<KeyIndex> key_index = DetermineKeys(cid_, args);
  if (!key_index) return key_index.status();
  BuildKeyMap(*key_index, kInvalidSid);
  return OpStatus::OK;
}

OpStatus Transaction::Init(const Namespace* ns, DbIndex db, cmn::CmdArgList args,
                           const KeyIndex& key_index, ShardId precomputed_sid) {
  InitBase(ns, db, args);
  BuildKeyMap(key_index, precomputed_sid);
  return OpStatus::OK;
}

void Transaction::InitBase(const Namespace* ns, DbIndex db, cmn::CmdArgList args) {
  DCHECK(cid_ != nullptr) << "command descriptor is not set";
  DCHECK_EQ(active_shard_count_, 0u);
  DCHECK(keys_.empty());
  DCHECK(fps_.empty());
  db_ = db;
  args_ = args;
  ns_ = ns;
}

void Transaction::MarkAllShards() {
  global_ = true;
  active_shard_count_ = shard_set->size();
  sole_shard_ = active_shard_count_ == 1 ? 0 : kInvalidSid;
  shards_.resize(active_shard_count_);
  for (auto& sd : shards_)
    sd.flags.fetch_or(kShardInvolved, std::memory_order_relaxed);
}

void Transaction::BuildKeyMap(const KeyIndex& key_index, ShardId precomputed_sid) {
  const unsigned n = key_index.NumArgs();
  if (n == 0) return;
  DCHECK_LT(key_index.start, args_.size());

  // 单 key：直接映射，无需聚合
  if (n == 1) {
    const std::string_view key = args_[*key_index];
    const ShardId sid = precomputed_sid != kInvalidSid
                            ? precomputed_sid
                            : Shard(key, shard_set->size());
    keys_.push_back(key);
    fps_.push_back(LockTag(key).Fingerprint());
    active_shard_count_ = 1;
    sole_shard_ = sid;
    shards_.resize(1);
    auto& sd = shards_.front();
    sd.flags.fetch_or(kShardInvolved, std::memory_order_relaxed);
    sd.key_begin = 0;
    sd.key_count = 1;
    return;
  }

  // 多 key：按分片聚合
  AccumGuard guard;
  key_accum.resize(shard_set->size());
  for (unsigned i : key_index.Range()) {
    const std::string_view key = args_[i];
    const ShardId sid = Shard(key, shard_set->size());
    key_accum[sid].keys.push_back(key);
  }

  // 统计涉及分片
  uint32_t active = 0;
  ShardId sole = kInvalidSid;
  for (ShardId sid = 0; sid < shard_set->size(); ++sid) {
    if (key_accum[sid].keys.empty()) continue;
    if (active == 0) sole = sid;
    ++active;
  }
  active_shard_count_ = active;

  // 全部 key 落在同一分片：压缩为单分片表示
  if (active == 1) {
    sole_shard_ = sole;
    for (const std::string_view k : key_accum[sole].keys) {
      keys_.push_back(k);
      fps_.push_back(LockTag(k).Fingerprint());
    }
    shards_.resize(1);
    auto& sd = shards_.front();
    sd.flags.fetch_or(kShardInvolved, std::memory_order_relaxed);
    sd.key_begin = 0;
    sd.key_count = keys_.size();
    return;
  }

  // 多分片：keys_ / fps_ 按分片分段，ShardState 记录段界
  sole_shard_ = kInvalidSid;
  shards_.resize(shard_set->size());
  for (ShardId sid = 0; sid < shard_set->size(); ++sid) {
    const auto& acc = key_accum[sid];
    if (acc.keys.empty()) continue;
    auto& sd = shards_[sid];
    sd.flags.fetch_or(kShardInvolved, std::memory_order_relaxed);
    sd.key_begin = keys_.size();
    sd.key_count = acc.keys.size();
    for (const std::string_view k : acc.keys) {
      keys_.push_back(k);
      fps_.push_back(LockTag(k).Fingerprint());
    }
  }
}

size_t Transaction::SidToId(ShardId sid) const {
  return active_shard_count_ == 1 ? 0 : static_cast<size_t>(sid);
}

bool Transaction::CanRunInlined() const {
  auto* es = EngineShard::tlocal();
  return active_shard_count_ == 1 && sole_shard_ == es->shard_id();
}

cppcoro::AsyncTask Transaction::Run(Callback cb, std::coroutine_handle<> resume) {
  DCHECK_GT(active_shard_count_, 0u);
  concluding_ = true;
  cb_ = cb;
  resume_handle_ = resume;
  owner_shard_ = EngineShard::tlocal()->shard_id();
  pending_.store(active_shard_count_, std::memory_order_relaxed);
  SetPhase(TxPhase::kScheduling);

  if (active_shard_count_ == 1) {
    // 单分片：先放行（本分片唯一，申请必然成功），再调度，随后驱动队列
    shards_.front().flags.fetch_or(kAllowed, std::memory_order_relaxed);
    barrier_->Add(1);
    auto hop = [self = intrusive_ptr_from_this()]() {
      LockResult res = self->ApplyForLockOn(*EngineShard::tlocal(), true);
      CHECK(res != LockResult::kRejected) << "single-shard tx rejected";
      if (res != LockResult::kGranted) {
        // 入队后由队首引理执行；若队首被更早的未就绪事务卡住，
        // 则等该事务完成时顺带放行（DriveQueue 的队首链）。
        EngineShard::tlocal()->DriveQueue(self);
      }
      self->barrier_->Dec();
    };
    if (CanRunInlined()) {
      hop();
    } else {
      shard_set->Add(sole_shard_, hop);
    }
    co_await barrier_->Wait();
  } else {
    co_await Schedule();
    SetPhase(TxPhase::kRunning);
    Distribute();
  }
  SetPhase(TxPhase::kFinished);
  // 先标记协调器已退出，再统一恢复调用方协程：分片线程只有在看到
  // complete_ 之后才允许恢复，避免"Run 尚未 co_return 就 resume 上层"。
  complete_.store(true, std::memory_order_release);
  ResumeIfReady();
  co_return;
}

cppcoro::task<> Transaction::Schedule() {
  idempotent_ = (cid_->opt_mask() & CO::IDEMPOTENT) != 0;
  // 多分片仅幂等命令允许乐观内联：乐观路径无法回滚，重复执行必须无害
  const bool allow_optimistic = idempotent_;

  while (true) {
    txid_ = global_seq.fetch_add(1, std::memory_order_relaxed);
    barrier_->Start(active_shard_count_);
    std::atomic<uint32_t> fail_cnt{0};

    auto hop = [this, &fail_cnt, allow_optimistic]() {
      LockResult res = ApplyForLockOn(*EngineShard::tlocal(), allow_optimistic);
      if (res == LockResult::kRejected)
        fail_cnt.fetch_add(1, std::memory_order_relaxed);
      barrier_->Dec();
    };
    for (size_t i = 0; i < shards_.size(); ++i) {
      if (shards_[i].flags.load(std::memory_order_relaxed) & kShardInvolved)
        shard_set->Add(static_cast<ShardId>(i), hop);
    }
    co_await barrier_->Wait();

    if (fail_cnt.load(std::memory_order_relaxed) == 0) break;

    // 回滚本次调度（出队 + 释放锁计数），重新调度
    SetPhase(TxPhase::kScheduling);
    bool need_poll = false;
    barrier_->Start(active_shard_count_);
    shard_set->DispatchBriefInParallel(
        [this, &need_poll](EngineShard* es) {
          if (RollbackOnShard(*es)) need_poll = true;
          barrier_->Dec();
        },
        [this](uint32_t i) {
          return i < shards_.size() &&
                 (shards_[i].flags.load(std::memory_order_relaxed) &
                  kShardInvolved) != 0;
        });
    co_await barrier_->Wait();

    // 队首被移走：通知相关分片重新驱动队列
    if (need_poll) {
      for (size_t i = 0; i < shards_.size(); ++i) {
        if (shards_[i].flags.load(std::memory_order_relaxed) & kShardInvolved)
          shard_set->Add(static_cast<ShardId>(i),
                         [] { EngineShard::tlocal()->DriveQueue(nullptr); });
      }
    }
    start_ms_ = util::GetCurrentTimeMs();
  }
  SetPhase(TxPhase::kWaiting);
  co_return;
}

void Transaction::Distribute() {
  std::bitset<1024> poll;
  uint32_t cnt = 0;
  for (size_t i = 0; i < shards_.size(); ++i) {
    ShardState& sd = shards_[i];
    if ((sd.flags.load(std::memory_order_relaxed) & kShardInvolved) == 0)
      continue;
    if (sd.flags.load(std::memory_order_relaxed) & kRanInline) {
      sd.flags.fetch_and(~kRanInline, std::memory_order_relaxed);
      continue;  // 已乐观内联完成的分片不再分发
    }
    poll.set(i, true);
    ++cnt;
  }
  if (cnt == 0) return;  // 全部内联完成

  // 放行（armed）：分片线程以 acquire 读取
  for (size_t i = 0; i < shards_.size(); ++i) {
    if (poll.test(i))
      shards_[i].flags.fetch_or(kAllowed, std::memory_order_release);
  }

  auto poll_cb = [self = intrusive_ptr_from_this()]() {
    EngineShard::tlocal()->DriveQueue(self);
  };
  if (CanRunInlined()) {  // 多分片下恒为 false，防御保留
    EngineShard::tlocal()->DriveQueue(intrusive_ptr_from_this());
  } else {
    for (size_t i = 0; i < shards_.size(); ++i) {
      if (poll.test(i)) shard_set->Add(static_cast<ShardId>(i), poll_cb);
    }
  }
}

LockResult Transaction::ApplyForLockOn(EngineShard& shard, bool allow_optimistic) {
  const ShardId sid = shard.shard_id();
  ShardState& sd = shards_[SidToId(sid)];

  // 本分片已在本事务乐观内联执行过（上一调度回合完成）：重试回合视为完成
  if (sd.flags.load(std::memory_order_relaxed) & kRanInline)
    return LockResult::kGranted;

  DCHECK_EQ(sd.flags.load(std::memory_order_relaxed) & kKeyHeld, 0u);
  // 注意：不清除 kAllowed —— 它由协调器（单分片 Run / Distribute）设置，
  // 放行位只允许在 AllowOn / AllowOnIf 中清除。
  sd.flags.fetch_and(~(kRanInline | kUncontended), std::memory_order_relaxed);

  // 乱序保护：本事务的序已在本分片执行过（重试边界），拒绝本次申请
  if (txid_ > 0 && shard.CommittedTxId() >= txid_)
    return LockResult::kRejected;

  const IntentLock::Mode mode = LockMode();
  const KeyLockArgs lock_args = LockArgsOn(sid);

  // 计数器锁（论文 §2.1）：Acquire 无条件递增 (CX, CS) 并判定授予；
  // 无论成败，Release 都必须对称递减。
  const bool keys_free = SliceOn(sid).Acquire(mode, lock_args);
  const bool shard_free = global_ ? shard.ShardLock().Acquire(mode) : true;
  const bool granted = keys_free && shard_free;
  if (granted) {
    sd.flags.fetch_or(kKeyHeld | kUncontended, std::memory_order_relaxed);
  }

  // 乐观内联：本分片锁无竞争，直接执行回调并释放
  if (granted && allow_optimistic) {
    sd.flags.fetch_or(kRanInline, std::memory_order_relaxed);
    ExecuteOnShard(shard);
    if (concluding_) return LockResult::kGranted;
  }

  if (txid_ == 0) {  // 单分片：调度时才分配事务序
    txid_ = global_seq.fetch_add(1, std::memory_order_relaxed);
  }

  TxQueue& queue = shard.Queue();
  // 有序性（论文 TxnQueue）：队列已存在更晚序者且本事务未拿锁 → 不可插队
  if (!queue.Empty() && txid_ < queue.Back()->txid() && !granted) {
    ReleaseLocks(shard, sd);
    return LockResult::kRejected;
  }
  // 队列高水位（论文 §2.1）：多分片未拿全锁且队列堆积 → 拒绝入队，
  // 把 CPU 让给队首推进 / SCA 消化队列
  if (active_shard_count_ > 1 && !granted &&
      queue.Size() >= EngineShard::kQueueHighWater) {
    ReleaseLocks(shard, sd);
    return LockResult::kRejected;
  }
  // 入队：等待队首引理 / SCA 放行
  sd.queue_pos = queue.Push(intrusive_ptr_from_this());
  return LockResult::kQueued;
}

bool Transaction::ExecuteOnShard(EngineShard& shard) {
  const ShardId sid = shard.shard_id();
  ShardState& sd = shards_[SidToId(sid)];

  // 出队（乐观内联路径未入队，跳过）
  if (sd.queue_pos != TxQueue::kEnd) {
    shard.Queue().Pop(sd.queue_pos);
    sd.queue_pos = TxQueue::kEnd;
  }

  // 回调：仅执行一次（乐观内联已执行则跳过）
  if ((sd.flags.load(std::memory_order_relaxed) & kRanInline) == 0) {
    InvokeCallback(shard);
  }

  if (concluding_) {
    ReleaseLocks(shard, sd);
    sd.flags.fetch_and(~(kKeyHeld | kUncontended), std::memory_order_relaxed);
  }
  FinishShardExecution();
  return concluding_;
}

bool Transaction::RollbackOnShard(EngineShard& shard) {
  const ShardId sid = shard.shard_id();
  ShardState& sd = shards_[SidToId(sid)];
  TxQueue::Iterator pos = sd.queue_pos;
  if (pos == TxQueue::kEnd) return false;  // 未入队（含乐观内联已完成的分片）

  TxQueue& queue = shard.Queue();
  const bool was_head = (pos == queue.Head());
  queue.Pop(pos);
  sd.queue_pos = TxQueue::kEnd;
  ReleaseLocks(shard, sd);  // 对称抵消 ApplyForLockOn 中的 Acquire
  sd.flags.fetch_and(~(kKeyHeld | kUncontended), std::memory_order_relaxed);
  return was_head && !queue.Empty();
}

bool Transaction::AllowOn(ShardId sid) {
  ShardState& sd = shards_[SidToId(sid)];
  const uint16_t old =
      sd.flags.fetch_and(~kAllowed, std::memory_order_acq_rel);
  return (old & kAllowed) != 0;
}

bool Transaction::AllowOnIf(ShardId sid, uint16_t need_flags, uint16_t* got_flags) {
  ShardState& sd = shards_[SidToId(sid)];
  // 仅当"已放行且满足条件"时清除 kAllowed；否则保持原状（供队首引理后续放行）
  while (true) {
    uint16_t f = sd.flags.load(std::memory_order_acquire);
    if (!(f & kAllowed) || !(f & need_flags)) {
      *got_flags = f;
      return false;
    }
    if (sd.flags.compare_exchange_weak(f, f & ~kAllowed,
                                       std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
      *got_flags = f;
      return true;
    }
  }
}

bool Transaction::IsAllowedOn(ShardId sid) const {
  const ShardState& sd = shards_[SidToId(sid)];
  return (sd.flags.load(std::memory_order_acquire) & kAllowed) != 0;
}

// ---- 执行上下文 ----

DbSlice& Transaction::SliceOn(ShardId sid) const {
  CHECK(ns_ != nullptr);
  return ns_->GetDbSlice(sid);
}

std::span<const std::string_view> Transaction::KeysOn(ShardId sid) const {
  const ShardState& sd = shards_[SidToId(sid)];
  return std::span<const std::string_view>(keys_.data() + sd.key_begin,
                                           sd.key_count);
}

KeyLockArgs Transaction::LockArgsOn(ShardId sid) const {
  KeyLockArgs res;
  res.db_index = db_;
  if (active_shard_count_ == 1) {
    res.fps.assign(fps_.begin(), fps_.end());
  } else {
    const ShardState& sd = shards_[sid];
    res.fps.assign(fps_.begin() + sd.key_begin,
                   fps_.begin() + sd.key_begin + sd.key_count);
  }
  return res;
}

// ---- 内部收尾 ----

bool Transaction::InvokeCallback(EngineShard& shard) {
  if (!cb_) return false;
  cb_.value()(*this, shard);
  if (active_shard_count_ == 1) cb_.reset();  // 单分片：回调仅执行一次
  return true;
}

void Transaction::FinishShardExecution() {
  if (pending_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    // 最后一个分片完成：通知协调器可恢复。
    // 仅在协调器 Run 已退出（complete_）时立即恢复；否则由 Run 末尾
    // 的 ResumeIfReady 统一处理，保证恢复前 Run 协程已 co_return。
    resume_requested_.store(true, std::memory_order_release);
    if (complete_.load(std::memory_order_acquire)) ResumeIfReady();
  }
}

void Transaction::ReleaseLocks(EngineShard& shard, ShardState& sd) {
  const IntentLock::Mode mode = LockMode();
  if (IsGlobal()) {
    shard.ShardLock().Release(mode);
  } else {
    SliceOn(shard.shard_id()).Release(mode, LockArgsOn(shard.shard_id()));
  }
  sd.flags.fetch_and(~kKeyHeld, std::memory_order_relaxed);
}

void Transaction::ResumeIfReady() {
  if (!concluding_) return;
  if (!resume_requested_.exchange(false, std::memory_order_relaxed)) return;
  auto* es = EngineShard::tlocal();
  if (es && es->shard_id() == owner_shard_) {
    resume_handle_.resume();
  } else {
    // 不在协调器分片线程：投递到协调器所在分片再恢复
    shard_set->Add(owner_shard_, [h = resume_handle_] { h.resume(); });
  }
}

}  // namespace dfly

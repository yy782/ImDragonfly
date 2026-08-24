// ============================================================================
// transaction.cpp —— Transaction 调度协议实现（独立设计）
//
// 调度协议对应 VVL 论文：
//   - ScheduleOnShard：单分片调度申请（计数器递增 + 授予判定），等价论文图 1
//     的请求过程，入队前保证有序性（TxnQueue）与队列高水位约束；
//   - Schedule：多分片调度回合，失败（kRejected）即回滚重试，直到全部
//     分片申请成功；
//   - Distribute：调度成功后放行各分片（is_armed），投递队列驱动；
//   - DriveQueue（EngineShard 侧）：队首引理执行 + SCA 消化队列。
// ============================================================================
#include "transaction_layer/transaction.hpp"

#include <algorithm>
#include <bitset>

#include "detail/common.hpp"
#include "sharding/shard_pool.hpp"
#include "util/Time.hpp"

namespace dfly {

namespace {

// 全局事务序（论文 §2.1 TxnQueue 的排序依据）：每分片队列按此单调递增的
// 序列号插入，保证所有分片看到一致的串行化顺序。
std::atomic_uint64_t global_seq{1};

// 多 key 按分片聚合用的线程本地缓冲（BuildKeyMap 复用，避免反复分配）
struct KeyAccum {
  std::vector<std::string_view> keys;   // 该分片的键（顺序与指纹一致）
  std::vector<IndexSlice> slices;       // 该分片的键参数下标段（相邻 key 合并）
};
thread_local std::vector<KeyAccum> key_accum;

// 回合结束自动回收聚合缓冲
struct AccumGuard {
  ~AccumGuard() {
    for (auto& a : key_accum) {
      a.keys.clear();
      a.slices.clear();
    }
  }
};

}  // namespace


Transaction::Transaction(const CommandId* cid) : cid_(cid) {
}

Transaction::~Transaction() {
  if (active_shard_count_ > 1) {
    involved_.many.sds.~vector();
    involved_.many.sids.~vector();
  }
}

void Transaction::SetStartTime() {
  start_ms_ = util::GetCurrentTimeMs();
}

OpStatus Transaction::Init(DbIndex db, cmn::CmdArgList args) {
  InitBase(db, args);

  if (cid_->opt_mask() & CO::GLOBAL_TRANS) {
    // 全局事务（如 SAVE）：覆盖所有分片，无 key 锁，锁取分片锁
    MarkAllShards();
    return OpStatus::OK;
  }
  BuildKeyMap();
  return OpStatus::OK;
}

bool Transaction::IsGlobal() const {
  // 覆盖全部分片即为全局事务（无成员，直接由参与分片数判定）
  return active_shard_count_ == shard_pool->size();
}

void Transaction::InitBase(DbIndex db, cmn::CmdArgList args) {
  db_ = db;
  args_ = args;
  SetStartTime();  // 时间点在初始化时确定（非构造时）
}

void Transaction::MarkAllShards() {
  // TODO
}

void Transaction::BuildKeyMap() {
  // 一次增强 for 遍历：KeyValue 自带原始下标，直接聚合到所属分片，
  // 首次出现的分片即时计入 active_shard_count_（sole 即第一个命中分片）。
  // 键步长是命令级属性，对所有键恒定，循环外取一次。
  AccumGuard guard;
  key_accum.resize(shard_pool->size());
  active_shard_count_ = 0;  // 事务复用前清零
  ShardId sole = kInvalidSid;
  const unsigned step = cid_->key_step();
  for (const auto& [key, pos] : cid_->Keys(args_)) {
    const ShardId sid = Shard(key, shard_pool->size());
    auto& acc = key_accum[sid];
    if (acc.keys.empty()) {  // 该分片首次出现
      if (active_shard_count_ == 0) sole = sid;
      ++active_shard_count_;
    }
    if (!acc.slices.empty() && acc.slices.back().second == pos) {
      acc.slices.back().second = pos + step;  // 相邻键合并为一段
    } else {
      acc.slices.emplace_back(pos, pos + step);
    }
    acc.keys.push_back(key);
  }
  if (active_shard_count_ == 0) return;  // 无键命令（NO_KEY_TRANSACTIONAL 等）

  // 收集参与分片：单分片进标量分支，多分片进向量分支（sids 与 sds 同序）
  if (active_shard_count_ == 1) {
    involved_.single.sid = sole;
  } else {
    auto& sids = ActivateMany();
    sids.clear();
    for (ShardId sid = 0; sid < shard_pool->size(); ++sid)
      if (!key_accum[sid].keys.empty()) sids.push_back(sid);
    involved_.many.sds.resize(sids.size());
  }

  // 每分片自持：键段（slices）与指纹（fps）直接进 PerShardData，无平铺。
  // 约定：many 分支内 sids 与 sds 等长同序，sds[idx] 记录分片 sids[idx] 的状态。
  auto fill = [&](size_t idx, ShardId sid) {
    auto& acc = key_accum[sid];
    PerShardData& sd = ShardDataAt(idx);
    sd.flags |= kShardInvolved;
    sd.slices = std::move(acc.slices);
    sd.fps.reserve(acc.keys.size());
    for (const std::string_view k : acc.keys)
      sd.fps.push_back(KeyFingerprint(k));
  };
  if (active_shard_count_ == 1) {
    fill(0, sole);
  } else {
    // idx 是参与序号，sids[idx] 是真实分片 id（升序，见上方收集段）
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

PerShardData& Transaction::ShardDataAt(size_t idx) {
  return active_shard_count_ == 1 ? involved_.single.sd
                                  : involved_.many.sds[idx];
}

const PerShardData& Transaction::ShardDataAt(size_t idx) const {
  return active_shard_count_ == 1 ? involved_.single.sd
                                  : involved_.many.sds[idx];
}

std::vector<ShardId>& Transaction::ActivateMany() {
  // 前置：Init 已回收上一轮的 many 分支，此刻 union 处于 single 分支
  new (&involved_.many.sids) std::vector<ShardId>();
  new (&involved_.many.sds) std::vector<PerShardData>();
  return involved_.many.sids;
}

bool Transaction::CanRunInlined() const {
  auto* es = EngineShard::tlocal();
  return active_shard_count_ == 1 && involved_.single.sid == es->shard_id();
}

cppcoro::AsyncTask Transaction::Run(Callback cb, std::coroutine_handle<> resume) {
  DCHECK_GT(active_shard_count_, 0u);

  coro_ctx_ = CoroutineCtx(std::move(cb), resume,
                           EngineShard::tlocal()->shard_id(),
                           active_shard_count_);

  if (active_shard_count_ == 1) {
    // 单分片：先放行（本分片唯一，申请必然成功），再调度，随后驱动队列
    ShardDataAt(0).is_armed.store(true, std::memory_order_relaxed);
    barrier_->Add(1);
    auto hop = [self = intrusive_ptr_from_this()]() {
      ScheduleResult res = self->ScheduleOnShard(*EngineShard::tlocal(), true);
      CHECK(res != ScheduleResult::kRejected) << "single-shard tx rejected";
      if (res != ScheduleResult::kGranted) {
        // 入队后由队首引理执行；若队首被更早的未就绪事务卡住，
        // 则等该事务完成时顺带放行（DriveQueue 的队首链）。
        EngineShard::tlocal()->DriveQueue(self);
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
    Distribute();
  }
  // 观察者收尾：调度回合已结束（单分片 barrier 归零 / 多分片 Schedule+
  // Distribute 完成），若命令已全部执行完（need_resume），此时恢复命令
  // 协程是安全的——事务不再被本协程访问，可以被析构。
  coro_ctx_.ResumeIfNeed();
  co_return;
}

cppcoro::task<> Transaction::Schedule() {
  // 多分片仅幂等命令允许乐观内联：乐观路径无法回滚，重复执行必须无害
  const bool allow_optimistic = (cid_->opt_mask() & CO::IDEMPOTENT) != 0;

  while (true) {
    txid_ = global_seq.fetch_add(1, std::memory_order_relaxed);
    barrier_->Start(active_shard_count_);
    std::atomic<uint32_t> fail_cnt{0};

    auto hop = [this, &fail_cnt, allow_optimistic]() {
      ScheduleResult res = ScheduleOnShard(*EngineShard::tlocal(), allow_optimistic);
      if (res == ScheduleResult::kRejected)
        fail_cnt.fetch_add(1, std::memory_order_relaxed);
      barrier_->Dec();
    };
    for (size_t i = 0; i < active_shard_count_; ++i)
      shard_pool->Post(InvolvedAt(i), hop);
    co_await barrier_->Wait();

    if (fail_cnt.load(std::memory_order_relaxed) == 0) break;

    // 回滚本次调度（出队 + 释放锁计数），重新调度。
    // 分片线程并发写，故用原子标志；仅"回滚的恰是队首且队列仍非空"时需要
    // 重新驱动（新队首可能早已放行却被旧队首卡在队首链外）。
    // 集合点用本事务自己的 barrier_，跨分片同步设施不再另设。
    std::atomic<bool> need_poll{false};
    barrier_->Start(active_shard_count_);
    for (size_t i = 0; i < active_shard_count_; ++i) {
      const ShardId sid = InvolvedAt(i);
      shard_pool->Post(sid, [this, &need_poll]() {
        if (RollbackOnShard(*EngineShard::tlocal()))
          need_poll.store(true, std::memory_order_relaxed);
        barrier_->Dec();
      });
    }
    co_await barrier_->Wait();

    // 队首被移走：通知相关分片重新驱动队列
    if (need_poll.load(std::memory_order_relaxed)) {
      for (size_t i = 0; i < active_shard_count_; ++i)
        shard_pool->Post(InvolvedAt(i),
                         [] { EngineShard::tlocal()->DriveQueue(nullptr); });
    }
    SetStartTime();  // 调度冲突重试视为重新开始，刷新时间点
  }
  co_return;
}

void Transaction::Distribute() {
  std::bitset<1024> poll;
  uint32_t cnt = 0;
  for (size_t i = 0; i < active_shard_count_; ++i) {
    PerShardData& sd = ShardDataAt(i);
    if (sd.flags & kRanInline) {
      sd.flags &= ~kRanInline;
      continue;  // 已乐观内联完成的分片不再分发
    }
    poll.set(i, true);
    ++cnt;
  }
  if (cnt == 0) return;  // 全部内联完成

  // 放行（armed）：分片线程以 acquire 读取
  for (size_t i = 0; i < active_shard_count_; ++i) {
    if (poll.test(i))
      ShardDataAt(i).is_armed.store(true, std::memory_order_release);
  }

  auto poll_cb = [self = intrusive_ptr_from_this()]() {
    EngineShard::tlocal()->DriveQueue(self);
  };
  if (CanRunInlined()) {  
    EngineShard::tlocal()->DriveQueue(intrusive_ptr_from_this());
  } else {
    for (size_t i = 0; i < active_shard_count_; ++i) {
      if (poll.test(i)) shard_pool->Post(InvolvedAt(i), poll_cb);
    }
  }
}

ScheduleResult Transaction::ScheduleOnShard(EngineShard& shard, bool allow_optimistic) {
  const ShardId sid = shard.shard_id();
  PerShardData& sd = ShardDataAt(IndexInvolved(sid));

  DCHECK_EQ(sd.flags & kUncontended, 0u);
  // 注意：不清除 is_armed —— 它由协调器（单分片 Run / Distribute）置位，
  // 放行位只允许在 AllowOn / AllowOnIf 中清除。
  sd.flags &= ~kRanInline;

  // 乱序保护：本事务的序已在本分片执行过（重试边界），拒绝本次申请
  if (txid_ > 0 && shard.CommittedTxId() >= txid_)
    return ScheduleResult::kRejected;

  const IntentLock::Mode mode = LockMode();
  const KeyLockArgs lock_args = LockArgsOn(sid);

  // 计数器锁（论文 §2.1）：Acquire 无条件递增 (CX, CS) 并判定授予；
  // 无论成败，Release 都必须对称递减。
  const bool keys_free = shard.GetShardStorage().Acquire(mode, lock_args);
  const bool shard_free = IsGlobal() ? shard.ShardLock().Acquire(mode) : true;
  const bool granted = keys_free && shard_free;
  if (granted) {
    sd.flags |= kUncontended;
  }

  // 乐观内联：本分片锁无竞争，直接执行回调并释放
  if (granted && allow_optimistic) {
    sd.flags |= kRanInline;
    ExecuteOnShard(shard);
    return ScheduleResult::kGranted;  // 回调已执行，本分片回合就此终结（不入队）
  }

  if (txid_ == 0) {  // 单分片：调度时才分配事务序
    txid_ = global_seq.fetch_add(1, std::memory_order_relaxed);
  }

  TxQueue& queue = shard.Queue();
  // 有序性（论文 TxnQueue）：队列已存在更晚序者且本事务未拿锁 → 不可插队
  if (!queue.Empty() && txid_ < queue.Back()->txid() && !granted) {
    ReleaseLocks(shard, sd);
    return ScheduleResult::kRejected;
  }
  // 队列高水位（论文 §2.1）：多分片未拿全锁且队列堆积 → 拒绝入队，
  // 把 CPU 让给队首推进 / SCA 消化队列
  if (active_shard_count_ > 1 && !granted &&
      queue.Size() >= EngineShard::kQueueHighWater) {
    ReleaseLocks(shard, sd);
    return ScheduleResult::kRejected;
  }
  // 入队：等待队首引理 / SCA 放行
  sd.queue_pos = queue.Push(intrusive_ptr_from_this());
  return ScheduleResult::kQueued;
}

void Transaction::ExecuteOnShard(EngineShard& shard) {
  const ShardId sid = shard.shard_id();
  PerShardData& sd = ShardDataAt(IndexInvolved(sid));

  // 出队（乐观内联路径未入队，跳过）
  if (sd.queue_pos != TxQueue::kEnd) {
    shard.Queue().Pop(sd.queue_pos);
    sd.queue_pos = TxQueue::kEnd;
  }

  // 回调：仅执行一次（乐观内联已执行则跳过）
  if ((sd.flags & kRanInline) == 0) {
    InvokeCallback(shard);
  }

  ReleaseLocks(shard, sd);
  sd.flags &= ~kUncontended;
  // 观察者收尾：本分片工作已结束，若调度亦已结束则恢复命令协程。
  coro_ctx_.ResumeIfNeed();
}

bool Transaction::RollbackOnShard(EngineShard& shard) {
  const ShardId sid = shard.shard_id();
  PerShardData& sd = ShardDataAt(IndexInvolved(sid));
  TxQueue::Iterator pos = sd.queue_pos;
  if (pos == TxQueue::kEnd) return false;  // 未入队（含乐观内联已完成的分片）

  TxQueue& queue = shard.Queue();
  const bool was_head = (pos == queue.Head());
  queue.Pop(pos);
  sd.queue_pos = TxQueue::kEnd;
  ReleaseLocks(shard, sd);  // 对称抵消 ScheduleOnShard 中的 Acquire
  sd.flags &= ~kUncontended;
  return was_head && !queue.Empty();
}

bool Transaction::AllowOn(ShardId sid) {
  PerShardData& sd = ShardDataAt(IndexInvolved(sid));
  // 放行闸门：清除并返回是否曾置位（队首引理）
  return sd.is_armed.exchange(false, std::memory_order_acq_rel);
}

bool Transaction::AllowOnIf(ShardId sid, uint16_t need_flags) {
  PerShardData& sd = ShardDataAt(IndexInvolved(sid));
  // 条件不满足则保持放行位原状（供队首引理后续无条件放行）；满足则清除并
  // 返回旧值——未放行时 exchange 自然返回 false，无需预先 load。
  if (!(sd.flags & need_flags))
    return false;
  return sd.is_armed.exchange(false, std::memory_order_acq_rel);
}

bool Transaction::IsAllowedOn(ShardId sid) const {
  const PerShardData& sd = ShardDataAt(IndexInvolved(sid));
  return sd.is_armed.load(std::memory_order_acquire);
}

// ---- 执行上下文 ----

DbContext Transaction::GetDbContext() const {
  return DbContext{db_, start_ms_};
}

Transaction::Slice Transaction::GetSlice(ShardId sid) const {
  const PerShardData& sd = ShardDataAt(IndexInvolved(sid));
  return Slice(std::span<const IndexSlice>(sd.slices), cid_->key_step(), args_);
}

KeyLockArgs Transaction::LockArgsOn(ShardId sid) const {
  KeyLockArgs res;
  res.db_index = db_;
  const PerShardData& sd = ShardDataAt(IndexInvolved(sid));
  res.fps = sd.fps;  // 指纹随分片自持
  return res;
}

// ---- 内部收尾 ----

void Transaction::InvokeCallback(EngineShard& shard) {
  coro_ctx_.cb.value()(*this, shard);  // 执行命令回调
  // 完成回调：pending--，归零即"命令全部执行完"，置可恢复标记
  coro_ctx_.FinishCallback();
}

void Transaction::CoroutineCtx::FinishCallback() {
  // 最后一个分片执行完（fetch_sub 返回 1）即"命令全部完成"，置可恢复标记；
  // 恢复动作由观察者（Run / ExecuteOnShard 末尾）执行，此处不恢复。
  if (pending.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    need_resume.store(true, std::memory_order_release);
  }
}

void Transaction::CoroutineCtx::ResumeIfNeed() {
  if (!need_resume.load(std::memory_order_acquire)) return;  // 命令未全部完成
  bool expected = false;
  // has_resume 防重：仅第一个观察者执行恢复
  if (has_resume.compare_exchange_strong(expected, true,
                                         std::memory_order_acq_rel)) {
    auto* es = EngineShard::tlocal();
    if (es && es->shard_id() == owner_) {
      resume.resume();
    } else {
      shard_pool->Post(owner_, [h = resume] { h.resume(); });
    }
  }
}

void Transaction::ReleaseLocks(EngineShard& shard, PerShardData& sd) {
  const IntentLock::Mode mode = LockMode();
  if (IsGlobal()) {
    shard.ShardLock().Release(mode);
  } else {
    shard.GetShardStorage().Release(mode, LockArgsOn(shard.shard_id()));
  }
  sd.flags &= ~kUncontended;
}

}  // namespace dfly

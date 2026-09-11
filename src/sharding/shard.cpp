#include "sharding/shard.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <memory>

#include "detail/conflig.hpp"
#include "detail/stateless_alloceator.hpp"
#include "raft/raft_node.hpp"
#include "server/redis_server.hpp"
#include "sharding/shard_pool.hpp"
#include "transaction_layer/transaction.hpp"

namespace dfly {

thread_local mi_heap_t* data_heap = nullptr;
thread_local Shard* Shard::shard_ = nullptr;

void Shard::InitThreadLocal(base::UringProactor* pb) {
  DCHECK_EQ(data_heap, nullptr);
  data_heap = mi_heap_new();
  void* ptr = mi_heap_malloc_aligned(data_heap, sizeof(Shard), alignof(Shard));
  shard_ = new (ptr) Shard(pb, data_heap);
  InitTLStatelessAllocMR(shard_->memory_resource());
}

void Shard::DestroyThreadLocal() {
  if (!shard_) return;
  mi_heap_t* tlh = shard_->mi_resource_.heap();
  shard_->~Shard();
  CleanupStatelessAllocMR();
  mi_free(shard_);
  shard_ = nullptr;
  mi_heap_delete(tlh);
  data_heap = nullptr;
}

Shard::Shard(base::UringProactor* pb, mi_heap_t* heap)
    : proactor_(pb),
      shard_id_(pb->GetPoolIndex()),
      mi_resource_(heap),
      storage_(shard_id_, this),
      txq_(&mi_resource_) {}

void Shard::StartRaftLogTimer() {
  if (!use_raft || raft_timer_started_) return;
  raft_timer_started_ = true;
  RaftLogTimerLoop();
}

cppcoro::AsyncTask Shard::RaftLogTimerLoop() {
  while (true) {
    co_await proactor_->ArmPeriodicTimer(kLogFlushIntervalMs);
    FlushLogToMain();
  }
  co_return;
}

void Shard::DriveQueue(Transaction* tx) {
  const ShardId sid = shard_id_;

  const bool tx_ready =
      tx && IsRaftReady(tx) && tx->AllowOnIf(sid, Transaction::kUncontended);

  while (!txq_.Empty()) {
    Transaction* head = txq_.Front().get();
    const bool should_run =
        (head == tx && tx_ready) || (IsRaftReady(head) && head->AllowOn(sid));
    if (!should_run) break;
    if (head == tx) tx = nullptr;
    committed_txid_ = head->txid();
    head->ExecuteOnShard(*this);
  }

  if (tx && tx_ready) {
    // committed_txid_ = tx->txid();
    //  由于tx是乱序执行的，所以txid会比队头大，所以commited_txid_会更大，加上committed_txid_
    //  = tx->txid();的判断的话 committed_txid_要加上std::max比较的逻辑
    tx->ExecuteOnShard(*this);
  }

  MaybeDriveUnblocked();
}

static void PostFollowerReadToMain(TxId txid) {
  auto* main_q = &RedisServer::Instance().MainProactor()->GetTaskQueue();
  const bool ok =
      main_q->TryAdd([txid]() { raft_node->RequestFollowerRead(txid); });
  if (!ok) {
    // 与 FlushLogToMain 同样的不可恢复状态：main 卡死时读也无法被确认，
    // 队列容量 16384 远大于正常在读事务数。
    LOG(FATAL) << "raft: main task queue overflow while posting follower read";
  }
}

// ready 集合查找：区间只增不删，直接遍历（区间数远小于事务数）。
bool Shard::InRanges(const std::vector<std::pair<TxId, TxId>>& ranges,
                     TxId txid) {
  for (const auto& [lo, hi] : ranges) {
    if (txid >= lo && txid <= hi) return true;
  }
  return false;
}

void Shard::AddReadyRanges(bool is_read,
                           std::vector<std::pair<TxId, TxId>> ranges) {
  if (ranges.empty()) return;
  auto& dst = is_read ? read_ready_ranges_ : write_ready_ranges_;
  dst.insert(dst.end(), ranges.begin(), ranges.end());
}

bool Shard::IsRaftReady(Transaction* tx) {
  if (!use_raft) return true;
  if (tx->IsFromLeader()) return true;
  if (tx->IsReadOnly()) {
    if (ReadTxReady(tx->txid())) return true;
    if (raft_node != nullptr && raft_node->LeaseReadAllowed()) return true;
    return false;
  }
  return WriteTxReady(tx->txid());
}

void Shard::MaybeDriveUnblocked() {
  // SCA 会执行队列中**非队首**的事务，是一条绕过 raft 门闩的路径；
  // 且它的合法性前提是"幂等"，与 raft 要求的确定性顺序重放根本冲突。
  // raft 模式下直接关闭，不做兼容。见 raft.md §8。
  if (use_raft) return;
  if (txq_.Size() <= kQueueHighWater) return;

  std::fill(scg_dx_.begin(), scg_dx_.end(), 0);
  std::fill(scg_ds_.begin(), scg_ds_.end(), 0);

  // 检查 tx 与已扫描集合是否冲突，并把 tx 的指纹计入位数组。
  // 不变量：扫描过的事务（无论是否执行）都计入，后续判断只依赖更早者，
  // 与队序（txid）一致；位数组只 0->1，无假阳性，只有指纹碰撞的假冲突。
  // 先完成全部冲突检查再写入，避免同一事务内两个指纹碰撞同一位。
  auto scan_mark = [&](util::intrusive_ptr<Transaction> tx) -> bool {
    const bool is_read = (tx->LockMode() == IntentLock::SHARED);
    const KeyLockContext largs = tx->LockArgsOn(shard_id_);
    bool conflict = false;
    for (LockFp fp : largs.fps) {
      const size_t idx = fp & (kScgBits - 1);
      const uint64_t mask = uint64_t{1} << (idx & 63);
      const size_t word = idx >> 6;
      if (is_read) {
        if (scg_dx_[word] & mask) conflict = true;  // 更早者写我读的
      } else {
        if ((scg_dx_[word] | scg_ds_[word]) & mask)
          conflict = true;  // 更早者读/写我写的
      }
    }
    for (LockFp fp : largs.fps) {
      const size_t idx = fp & (kScgBits - 1);
      const uint64_t mask = uint64_t{1} << (idx & 63);
      const size_t word = idx >> 6;
      if (is_read) {
        scg_ds_[word] |= mask;
      } else {
        scg_dx_[word] |= mask;
      }
    }
    return !conflict;
  };

  size_t scanned = 0;
  TxQueue::Iterator it = txq_.Head();
  while (it != TxQueue::kEnd && scanned < kScgMaxScan) {
    util::intrusive_ptr<Transaction> tx = txq_.At(it);
    it = txq_.Next(it);
    ++scanned;

    if (!tx || tx->IsGlobal()) continue;  // 全局事务走分片锁，不在此判定

    if (!tx->IsAllowedOn(shard_id_)) {
      scan_mark(tx);  // 未就绪：不能执行，但仍计入
      continue;
    }
    if (!scan_mark(tx)) continue;  // 与更早者冲突，保持阻塞

    // 已就绪且无冲突：解除阻塞并提前执行
    [[maybe_unused]] const bool disarmed = tx->AllowOn(shard_id_);
    DCHECK(disarmed) << "IsAllowedOn true but AllowOn returned false";
    committed_txid_ = std::max(committed_txid_, tx->txid());
    tx->ExecuteOnShard(*this);
  }
}

bool Shard::PushLogIfNeed(Transaction* tx) {
  if (!use_raft) return true;
  // 重放窗口内不写日志：重放走的是完整正常命令路径，会经过这里。
  // 不拦住的话，日志里的条目会被当成新命令重新落盘，每次重启翻一倍。
  if (IsRaftReplaying()) return true;

  if (tx->IsReadOnly()) return true;
  if (!tx->IsFromLeader() && !raft_node->is_leader()) return false;

  RaftLogEntry e;
  e.txid = tx->txid();

  e.start_ms = tx->TimeMs();

  e.payload = EncodeRespCommand(tx->Args());

  log_.push_back(std::move(e));

  if (log_.size() >= kLogHighWater) FlushLogToMain();
  return true;
}

void Shard::PushReadIndexIfNeed(Transaction* tx) {
  if (!use_raft) return;
  if (!tx->IsReadOnly()) return;
  if (IsRaftReplaying()) return;
  PostFollowerReadToMain(tx->txid());
}

void Shard::FlushLogToMain() {
  if (log_.empty()) return;
  auto batch = std::move(log_);
  log_.clear();

  auto* main_q = &RedisServer::Instance().MainProactor()->GetTaskQueue();
  const bool ok = main_q->TryAdd([batch = std::move(batch)]() mutable {
    raft_node->SubmitBatch(std::move(batch));
  });
  if (!ok) {
    LOG(FATAL) << "raft: main task queue overflow while flushing shard "
               << shard_id_ << " log";
  }
}

}  // namespace dfly
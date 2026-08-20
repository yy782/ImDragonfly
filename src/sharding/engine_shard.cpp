// ============================================================================
// engine_shard.cpp —— 分片线程调度实现（独立设计）
//
// DriveQueue（队首引理 + 投递事务的乱序执行）与 MaybeDriveUnblocked（轻量
// SCA）共同构成论文的调度推进机制：
//   - 队首引理：每分片只需检查队首——队首已放行（armed）即执行并出队，
//     反复推进直到队首未放行（被其它分片调度时序卡住）；
//   - 乱序执行：投递的本分片事务若锁无竞争（kUncontended），即使不在队首
//     也可直接执行（此时已持有锁，且队列中无更早序者会与其冲突）；
//   - SCA：队首被卡且队列堆积到高水位时，用写集/读集位数组扫描队内事务，
//     找出已全分区就绪且与已扫描者无锁冲突的事务提前执行。
// ============================================================================
#include "sharding/engine_shard.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <memory>

#include "detail/stateless_alloceator.hpp"
#include "sharding/db_slice.hpp"
#include "transaction_layer/transaction.hpp"

namespace dfly {

thread_local mi_heap_t* data_heap = nullptr;
thread_local EngineShard* EngineShard::shard_ = nullptr;

void EngineShard::InitThreadLocal(base::UringProactor* pb) {
  DCHECK_EQ(data_heap, nullptr);
  data_heap = mi_heap_new();
  void* ptr = mi_heap_malloc_aligned(data_heap, sizeof(EngineShard),
                                     alignof(EngineShard));
  shard_ = new (ptr) EngineShard(pb, data_heap);
  InitTLStatelessAllocMR(shard_->memory_resource());
}

void EngineShard::DestroyThreadLocal() {
  if (!shard_) return;
  mi_heap_t* tlh = shard_->mi_resource_.heap();
  shard_->Shutdown();
  shard_->~EngineShard();
  CleanupStatelessAllocMR();
  mi_free(shard_);
  shard_ = nullptr;
  mi_heap_delete(tlh);
  data_heap = nullptr;
}

EngineShard::EngineShard(base::UringProactor* pb, mi_heap_t* heap)
    : proactor_(pb), shard_id_(pb->GetPoolIndex()), mi_resource_(heap),
      txq_(&mi_resource_) {}

void EngineShard::DriveQueue(util::intrusive_ptr<Transaction> tx) {
  const ShardId sid = shard_id_;

  // 1) 投递事务的放行：仅当锁无竞争（kUncontended）时允许在第 3 步跳过
  //    队首提前执行（乱序执行）；若本分片尚未放行（其它分片未调度完），
  //    直接返回，由后续驱动（Distribute 投递 / 队首链）接手。
  uint16_t got = 0;
  const bool tx_allowed = tx && tx->AllowOnIf(sid, kUncontended, &got);
  if (tx && !tx_allowed && (got & kAllowed) == 0) return;

  // 2) 队首引理（论文 §2.1）：队首已放行即执行，直到队首未放行。
  //    注意：无条件放行适用于任何队首（无论竞争与否）——"已入队且已放行
  //    的队首"必然可以安全执行（它是队列中最老者，锁的授予按序进行）。
  while (!txq_.Empty()) {
    util::intrusive_ptr<Transaction> head = txq_.Front();
    if (!head->AllowOn(sid)) break;
    if (head == tx) tx = nullptr;  // 投递事务已在队首链中执行
    committed_txid_ = std::max(committed_txid_, head->txid());
    head->ExecuteOnShard(*this);
  }

  // 3) 投递事务已放行（无竞争）且未在队首循环中执行 → 乱序执行
  if (tx && tx_allowed) {
    committed_txid_ = std::max(committed_txid_, tx->txid());
    tx->ExecuteOnShard(*this);
  }

  // 4) 队列堆积时尝试 SCA
  MaybeDriveUnblocked();
}

void EngineShard::MaybeDriveUnblocked() {
  // 仅队列堆积到高水位以上才付出扫描代价（SCA 的"选择性"）
  if (txq_.Size() <= kQueueHighWater) return;

  std::fill(scg_dx_.begin(), scg_dx_.end(), 0);
  std::fill(scg_ds_.begin(), scg_ds_.end(), 0);

  // 检查 tx 与已扫描集合是否冲突，并把 tx 的指纹计入位数组。
  // 不变量：扫描过的事务（无论是否执行）都计入，后续判断只依赖更早者，
  // 与队序（txid）一致；位数组只 0->1，无假阳性，只有指纹碰撞的假冲突。
  // 先完成全部冲突检查再写入，避免同一事务内两个指纹碰撞同一位。
  auto scan_mark = [&](util::intrusive_ptr<Transaction> tx) -> bool {
    const bool is_read = (tx->LockMode() == IntentLock::SHARED);
    const KeyLockArgs largs = tx->LockArgsOn(shard_id_);
    bool conflict = false;
    for (LockFp fp : largs.fps) {
      const size_t idx = fp & (kScgBits - 1);
      const uint64_t mask = uint64_t{1} << (idx & 63);
      const size_t word = idx >> 6;
      if (is_read) {
        if (scg_dx_[word] & mask) conflict = true;  // 更早者写我读的
      } else {
        if ((scg_dx_[word] | scg_ds_[word]) & mask) conflict = true;  // 更早者读/写我写的
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
      scan_mark(tx);  // 未就绪：不能执行，但仍计入（保守，防止越过它）
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

}  // namespace dfly

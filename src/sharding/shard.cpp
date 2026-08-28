#include "sharding/shard.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <memory>

#include "detail/stateless_alloceator.hpp"
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

void Shard::DriveQueue(Transaction* tx) {
  const ShardId sid = shard_id_;

  const bool tx_ready = tx && tx->AllowOnIf(sid, Transaction::kUncontended);

  while (!txq_.Empty()) {
    Transaction* head = txq_.Front().get();
    const bool should_run = (head == tx && tx_ready) || head->AllowOn(sid);
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

void Shard::MaybeDriveUnblocked() {
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

}  // namespace dfly

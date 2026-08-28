#pragma once

#include <mimalloc.h>

#include <array>
#include <cstdint>

#include "detail/intent_lock.hpp"
#include "detail/task_queue.hpp"
#include "detail/tx_queue.hpp"
#include "io/uring_proactor.hpp"
#include "sharding/shard_storage.hpp"
#include "util/intrusive_ptr.hpp"
#include "util/mi_memory_resource.hpp"

namespace dfly {

class Transaction;

class Shard {
 public:
  friend class ShardPool;

  static void InitThreadLocal(base::UringProactor* pb);
  static void DestroyThreadLocal();
  static Shard* tlocal() { return shard_; }
  bool IsMyThread() const { return this == shard_; }

  ShardId shard_id() const { return shard_id_; }
  base::UringProactor* proactor() const { return proactor_; }
  std::pmr::memory_resource* memory_resource() { return &mi_resource_; }
  dfly::TaskQueue* GetQueue() { return &proactor_->GetTaskQueue(); }
  void DriveQueue(Transaction* tx);

  TxQueue& Queue() { return txq_; }
  const TxQueue& Queue() const { return txq_; }
  IntentLock& ShardLock() { return shard_lock_; }
  TxId CommittedTxId() const { return committed_txid_; }

#ifdef UNIT_TESTS
  void set_committed_txid(TxId v) { committed_txid_ = v; }
#endif

  ShardStorage& GetShardStorage() { return storage_; }
  const ShardStorage& GetShardStorage() const { return storage_; }

  // 队列高水位阈值（VVL 论文 §2.1）：多分片事务未拿全锁且队列达到此值时
  // 放弃入队，把 CPU 让给队首推进 / SCA 消化队列。
  static constexpr size_t kQueueHighWater = 8;

 private:
  Shard(base::UringProactor* pb, mi_heap_t* heap);

  // 轻量 SCA（选择性冲突分析，论文 §2.6）：仅队列堆积时激活，用写集/读集
  // 位数组扫描出已就绪且无冲突的事务提前执行。
  void MaybeDriveUnblocked();

  base::UringProactor* proactor_;
  ShardId shard_id_;
  MiMemoryResource mi_resource_;
  static thread_local Shard* shard_;

  ShardStorage storage_;
  // dragonflydb官方选择namespaces管理分片存储，为的是多租户，我们不需要多租户，shard管理即可
  TxQueue txq_;
  IntentLock shard_lock_;
  TxId committed_txid_ = 0;

  // SCA 位数组：2^16 位 = 8KB，L1 缓存友好
  static constexpr size_t kScgBits = 1 << 16;
  static constexpr size_t kScgWords = kScgBits / 64;
  static constexpr size_t kScgMaxScan = 32;  // 单次 SCA 最多扫描的事务数
  std::array<uint64_t, kScgWords> scg_dx_{};  // 已扫描事务的写集
  std::array<uint64_t, kScgWords> scg_ds_{};  // 已扫描事务的读集
};

}  // namespace dfly

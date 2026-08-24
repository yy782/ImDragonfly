// ============================================================================
// shard.hpp —— 分片线程（独立设计）
//
// 每个分片线程持有一个 Shard，承载：
//   - 分片本地资源（内存资源、proactor、任务队列）——平台层；
//   - 本分片的事务队列 txq_ 与全局事务锁 shard_lock_（VVL 论文 §2.1）：
//     队列按 txid 有序插入，是"谁在锁释放后获得锁"的仲裁依据；
//   - 调度推进：DriveQueue 实现队首引理（队首已放行即执行），并在队列
//     堆积到高水位时启动轻量 SCA（MaybeDriveUnblocked）提前执行无冲突者；
//   - 已执行水位 committed_txid_：单调不减，供事务调度侧做乱序保护。
//
// 本文件为独立设计，不兼容原版调用点；平台层接口（tlocal / GetQueue /
// memory_resource / proactor 等）为分片线程基础设施，保持不变。
// ============================================================================
#pragma once

#include <mimalloc.h>

#include <array>
#include <cstdint>

#include "detail/intent_lock.hpp"
#include "detail/mi_memory_resource.hpp"
#include "detail/tx_queue.hpp"
#include "sharding/shard_storage.hpp"
#include "net/uring_proactor.hpp"
#include "util/intrusive_ptr.hpp"
#include "util/task_queue.hpp"

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
  PMR_NS::memory_resource* memory_resource() { return &mi_resource_; }
  util::TaskQueue* GetQueue() { return &proactor_->GetTaskQueue(); }

  // ---- 调度接口 ----

  // 驱动本分片队列：队首引理执行（队首已放行即执行，直到队首未放行），
  // 随后按需启动 SCA。tx 为协调器投递的本分片事务（可为空，仅触发推进）。
  void DriveQueue(util::intrusive_ptr<Transaction> tx);

  TxQueue& Queue() { return txq_; }
  const TxQueue& Queue() const { return txq_; }
  IntentLock& ShardLock() { return shard_lock_; }
  // 已执行水位（单调不减）
  TxId CommittedTxId() const { return committed_txid_; }

  // 本分片键空间存储（锁表 + 键值表）
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

  ShardStorage storage_;  // 本分片键空间存储（须在 mi_resource_ 之后构造）
  TxQueue txq_;
  IntentLock shard_lock_;
  TxId committed_txid_ = 0;  // 已执行水位（只增不减）

  // SCA 位数组：2^16 位 = 8KB，L1 缓存友好
  static constexpr size_t kScgBits = 1 << 16;
  static constexpr size_t kScgWords = kScgBits / 64;
  static constexpr size_t kScgMaxScan = 32;  // 单次 SCA 最多扫描的事务数
  std::array<uint64_t, kScgWords> scg_dx_{};  // 已扫描事务的写集
  std::array<uint64_t, kScgWords> scg_ds_{};  // 已扫描事务的读集
};

}  // namespace dfly

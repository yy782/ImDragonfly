// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include <mimalloc.h>

#include <cstdint>

#include "cppcoro/async_task.hpp"
#include "detail/intent_lock.hpp"
#include "detail/mi_memory_resource.hpp"
#include "detail/tx_queue.hpp"
#include "net/uring_proactor.hpp"
#include "util/task_queue.hpp"

namespace dfly {

using ShardId = uint16_t;
using TxId = uint64_t;

class EngineShard;
class DbSlice;
class Transaction;

class EngineShard {
 public:
  friend class EngineShardSet;

  static void InitThreadLocal(base::UringProactor* pb);
  static void DestroyThreadLocal();
  static EngineShard* tlocal() { return shard_; }
  bool IsMyThread() const { return this == shard_; }
  ShardId shard_id() const { return shard_id_; }
  PMR_NS::memory_resource* memory_resource() { return &mi_resource_; }
  util::TaskQueue* GetQueue() { return &proactor_->GetTaskQueue(); }

  void PollExecution(std::shared_ptr<Transaction> trans);

  TxQueue* txq() { return &txq_; }
  const TxQueue* txq() const { return &txq_; }

  // 分片级锁：全局事务（如 SAVE）在调度入队时获取，保证与其它全局事务
  // 互斥；concluding hop 结束时释放。
  IntentLock* shard_lock() { return &shard_lock_; }

  size_t committed_txid() const { return committed_txid_; }
#ifdef UNIT_TESTS
  size_t& committed_txid() { return committed_txid_; }
#endif
 private:
  EngineShard(base::UringProactor* pb, mi_heap_t* heap);
  void Shutdown();
  base::UringProactor* proactor_;
  ShardId shard_id_;
  MiMemoryResource mi_resource_;
  static thread_local EngineShard* shard_;
  TxQueue txq_;
  IntentLock shard_lock_;

  size_t committed_txid_ = 0;
};

}  // namespace dfly
#include "sharding/shard.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <memory>

#include "detail/conflig.hpp"
#include "detail/stateless_alloceator.hpp"
#include "raft/raft_node.hpp"
#include "redis/facade/ParseRESP.hpp"
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


}

static void PostFollowerReadToMain(TxId txid) {
  auto* main_q = &RedisServer::Instance().MainProactor()->GetTaskQueue();
  const bool ok =
      main_q->TryAdd([txid]() { raft_node->RequestFollowerRead(txid); });
  if (!ok) {
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
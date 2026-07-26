#include "engine_shard.hpp"

#include <glog/logging.h>

#include "db_slice.hpp"
#include "detail/stateless_alloceator.hpp"
#include "transaction_layer/transaction.hpp"

namespace dfly {
thread_local mi_heap_t* data_heap = nullptr;
thread_local EngineShard* EngineShard::shard_ = nullptr;

void EngineShard::InitThreadLocal(yy::net::EventLoop* pb) {
  LOG(INFO) << "Initializing EngineShard thread local for proactor "
            << pb->id();
  data_heap = mi_heap_new();
  void* ptr = mi_heap_malloc_aligned(data_heap, sizeof(EngineShard),
                                     alignof(EngineShard));
  shard_ = new (ptr) EngineShard(pb, data_heap);
  InitTLStatelessAllocMR(shard_->memory_resource());
  LOG(INFO) << "EngineShard thread local initialized, shard_id="
            << shard_->shard_id();
}

EngineShard::EngineShard(yy::net::EventLoop* pb, mi_heap_t* heap)
    : proactor_(pb), shard_id_(pb->id()), mi_resource_(heap), txq_() {}

void EngineShard::DestroyThreadLocal() {
  if (!shard_) return;
  mi_heap_t* tlh = shard_->mi_resource_.heap();
  shard_->Shutdown();
  shard_->~EngineShard();
  CleanupStatelessAllocMR();
  mi_free(shard_);
  shard_ = nullptr;
  mi_heap_delete(tlh);
}

void EngineShard::Shutdown() {}


void EngineShard::PollExecution(Transaction* trans) {
  ShardId sid = shard_id();
  uint16_t flags = Transaction::OUT_OF_ORDER;
  auto [trans_mask, disarmed] =
      trans ? trans->DisarmInShardWhen(sid, flags) : std::make_pair(uint16_t(0), false);
  if (trans && trans_mask == 0)  
    return;

  auto run = [this](Transaction* tx) -> bool  {
    return tx->RunInShard(this);
  };

  Transaction* head = nullptr;

  while (!txq_.Empty()) {
    head = get<Transaction*>(txq_.Front());
    bool should_run = (head == trans && disarmed) || head->DisarmInShard(sid);
    if (!should_run)
      break;
    if (head == trans)
      trans = nullptr;

    TxId txid = head->txid();
 
    committed_txid_ = txid;
    run(head);
  }
  if (trans && disarmed) {
    DCHECK(trans_mask & Transaction::OUT_OF_ORDER);
    bool concludes = run(trans);
  }
}




}  // namespace dfly

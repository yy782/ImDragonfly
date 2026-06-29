#pragma once
#include <cstdint>
#include <mimalloc.h> 
#include "detail/mi_memory_resource.hpp"
#include "net/base/uring_proactor_pool.hpp"
#include "util/task_queue.hpp"
#include "cppcoro/async_task.hpp"
#include "detail/intent_lock.hpp"
#include "detail/tx_queue.hpp"

namespace dfly {

using ShardId = uint16_t;
using TxId = uint64_t;

class EngineShard;
class DbSlice;
class Transaction;

class EngineShard 
{
public:
    friend class EngineShardSet;

    struct Stats {
        uint64_t tx_optimistic_total = 0;
        uint64_t tx_ooo_total = 0;
    };

    static void InitThreadLocal(base::UringProactorPtr pb);
    static void DestroyThreadLocal();
    static EngineShard* tlocal() { return shard_; }
    bool IsMyThread() const { return this == shard_;}
    ShardId shard_id() const { return shard_id_; } 
    PMR_NS::memory_resource* memory_resource() { return &mi_resource_; }
    util::TaskQueue* GetQueue() { return &proactor_->GetTaskQueue(); }    

    cppcoro::task<void> PollExecution(Transaction* trans);

    TxQueue* txq() { return &txq_; }
    const TxQueue* txq() const { return &txq_; }

    TxId committed_txid() const { return committed_txid_; }

    IntentLock* shard_lock() { return &shard_lock_; }

    DbSlice* GetDbSlice(ShardId sid);

    const Stats& stats() const { return stats_; }
    Stats& stats() { return stats_; }

private:
    EngineShard(base::UringProactorPtr pb, mi_heap_t* heap);
    void Shutdown(); 
    base::UringProactorPtr proactor_;
    ShardId shard_id_;
    MiMemoryResource mi_resource_;
    static thread_local EngineShard* shard_;      

    TxQueue txq_;
    TxId committed_txid_ = 0;
    IntentLock shard_lock_;
    Stats stats_;
};

}  // namespace dfly
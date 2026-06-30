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

    static void InitThreadLocal(base::UringProactorPtr pb);
    static void DestroyThreadLocal();
    static EngineShard* tlocal() { return shard_; }
    bool IsMyThread() const { return this == shard_;}
    ShardId shard_id() const { return shard_id_; } 
    PMR_NS::memory_resource* memory_resource() { return &mi_resource_; }
    util::TaskQueue* GetQueue() { return &proactor_->GetTaskQueue(); }    

    void PollExecution(Transaction* trans);

    TxQueue* txq() { return &txq_; }
    const TxQueue* txq() const { return &txq_; }

    DbSlice* GetDbSlice(ShardId sid);

private:
    EngineShard(base::UringProactorPtr pb, mi_heap_t* heap);
    void Shutdown(); 
    base::UringProactorPtr proactor_;
    ShardId shard_id_;
    MiMemoryResource mi_resource_;
    static thread_local EngineShard* shard_;      
    TxQueue txq_;
};


}  // namespace dfly
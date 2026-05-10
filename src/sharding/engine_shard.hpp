#pragma once
#include <cstdint>
#include <mimalloc.h> 
#include "detail/task_queue.hpp"
#include "detail/mi_memory_resource.hpp"
#include "net/base/uring_proactor_pool.hpp"
namespace dfly {

using ShardId = uint16_t;

class EngineShard 
{
public:
    friend class EngineShardSet;

    static void InitThreadLocal(base::UringProactorPtr pb); // 在当前线程中创建并初始化 EngineShard 实例，并绑定到线程本地存储
    static void DestroyThreadLocal(); // 销毁当前线程的 EngineShard 实例，释放资源。    
    static EngineShard* tlocal() { return shard_; } // 获取当前线程绑定的 EngineShard 实例。
    bool IsMyThread() const { return this == shard_;}
    ShardId shard_id() const { return shard_id_; } 
    PMR_NS::memory_resource* memory_resource() { return &mi_resource_; }
    TaskQueue* GetQueue() { return &queue_; }    

private:
    EngineShard(base::UringProactorPtr pb, mi_heap_t* heap);
    void Shutdown(); 
    TaskQueue queue_;
    ShardId shard_id_;
    MiMemoryResource mi_resource_;
    static thread_local EngineShard* shard_;      
};

}  // namespace dfly
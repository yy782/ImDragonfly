#pragma once
#include <cstdint>
#include <mimalloc.h> 
#include "detail/task_queue.hpp"
#include "detail/intent_lock.hpp"
#include "util/sliding_counter.h"
#include "memory/mi_memory_resource.hpp"

namespace dfly {

using ShardId = uint16_t;

class EngineShard 
{
public:
    friend class EngineShardSet;

    static void InitThreadLocal(util::ProactorBase* pb); // 在当前线程中创建并初始化 EngineShard 实例，并绑定到线程本地存储
    static void DestroyThreadLocal(); // 销毁当前线程的 EngineShard 实例，释放资源。    
    static EngineShard* tlocal() { return shard_; } // 获取当前线程绑定的 EngineShard 实例。
    bool IsMyThread() const { return this == shard_;}
    ShardId shard_id() const { return shard_id_; } 
    PMR_NS::memory_resource* memory_resource() { return &mi_resource_; }
    TaskQueue* GetFiberQueue() { return &queue_; }
    TaskQueue* GetSecondaryQueue() { return &queue2_; }  
    IntentLock* shard_lock() { return &shard_lock_; }    
    bool journal() const { return journal_; }
    void set_journal(bool enable) { journal_ = enable; }
    void SetReplica(bool replica) { is_replica_ = replica; }
    bool IsReplica() const { return is_replica_; }   
    void StopPeriodicFiber(); // 停止后台任务     
    
    
    

private:
    EngineShard(util::ProactorBase* pb, mi_heap_t* heap);
    void Shutdown(); 
    void StartPeriodicHeartbeatFiber(util::ProactorBase* pb);  // 启动一个周期性执行的心跳纤程，定期更新 Shard 的状态
    void StartPeriodicShardHandlerFiber(util::ProactorBase* pb, std::function<void()> shard_handler); // 启动一个周期性执行的分片处理纤程，执行用户传入的回调函数。   
    TaskQueue queue_,queue2_;
    ShardId shard_id_;

    bool is_replica_ = false;  // 是否为副本节点（不主动淘汰）
    bool journal_ = false;      // 是否启用 Journal（复制日志）  
    MiMemoryResource mi_resource_;

    IntentLock shard_lock_;  
    static __thread EngineShard* shard_;      
};

}  // namespace dfly
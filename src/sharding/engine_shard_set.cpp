#include "engine_shard_set.hpp"
#include "common.hpp"
#include "namespaces.hpp"

#include <functional>

namespace dfly{

EngineShardSet* shard_set = nullptr;

void EngineShardSet::Init(uint32_t sz, std::function<void()> shard_handler) {
    shards_.reset(new EngineShard*[sz]);
    size_ = sz;
    //size_t max_shard_file_size = GetTieredFileLimit(sz);
    pp_->AwaitFiberOnAll([this](uint32_t index, ProactorBase* pb) {
        if (index < size_) {
            InitThreadLocal(pb);
        }
    });

    namespaces = new Namespaces();

    // pp_->AwaitFiberOnAll([&](uint32_t index, ProactorBase* pb) {
    //     if (index < size_) {
    //     // auto* shard = EngineShard::tlocal();
    //     // shard->InitTieredStorage(pb, max_shard_file_size);
    //     // shard->StartPeriodicHeartbeatFiber(pb);
    //     // shard->StartPeriodicShardHandlerFiber(pb, shard_handler);
    //     }
    // });
}


void EngineShardSet::PreShutdown() {
    RunBlockingInParallel([](EngineShard* shard) {
        // shard->StopPeriodicFiber();
        // if (shard->tiered_storage()) {
        //     shard->tiered_storage()->Close();
        // }
    });
}

void EngineShardSet::Shutdown() {
    namespaces->Clear();
    RunBlockingInParallel([](EngineShard*) { 
        EngineShard::DestroyThreadLocal(); 
    });
    delete namespaces;
    namespaces = nullptr;    
}


void EngineShardSet::InitThreadLocal(ProactorBase* pb) {
    EngineShard::InitThreadLocal(pb);
    EngineShard* es = EngineShard::tlocal();
    shards_[es->shard_id()] = es;
}












ShardId Shard(std::string_view key)
{
    auto size = shard_set.size();
    size_t hash = std::hash<std::string_view>{}(key);

    if(isPowerOfTwo(size))
    {
        return hash & (size-1);
    }
    else 
        return hash % size;
}


}  // namespace dfly
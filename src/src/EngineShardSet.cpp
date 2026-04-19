#include "EngineShardSet.hpp"

namespace dfly{

void EngineShardSet::Init(uint32_t sz, std::function<void()> shard_handler) {
    shards_.reset(new EngineShard*[sz]);
    size_ = sz;
    //size_t max_shard_file_size = GetTieredFileLimit(sz);
    pp_->AwaitFiberOnAll([this](uint32_t index, ProactorBase* pb) {
        if (index < size_) {
            InitThreadLocal(pb);
        }
    });
    pp_->AwaitFiberOnAll([&](uint32_t index, ProactorBase* pb) {
        if (index < size_) {
        // auto* shard = EngineShard::tlocal();
        // shard->InitTieredStorage(pb, max_shard_file_size);
        // shard->StartPeriodicHeartbeatFiber(pb);
        // shard->StartPeriodicShardHandlerFiber(pb, shard_handler);
        }
    });
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
    RunBlockingInParallel([](EngineShard*) { EngineShard::DestroyThreadLocal(); });
}
}  // namespace dfly
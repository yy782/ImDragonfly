#include "EngineShard.hpp"
#include "../include/memory/stateless_alloceator.hpp"

namespace dfly{
constexpr size_t kQueueLen = 64;
thread_local mi_heap_t* data_heap = nullptr; // 线程本地堆指针
__thread EngineShard* EngineShard::shard_ = nullptr;
void EngineShard::InitThreadLocal(ProactorBase* pb) {
    data_heap = mi_heap_new();
    void* ptr = mi_heap_malloc_aligned(data_heap, sizeof(EngineShard), alignof(EngineShard));
    shard_ = new (ptr) EngineShard(pb, data_heap);
    InitTLStatelessAllocMR(shard_->memory_resource()); // 初始化无状态内存分配器
    //shard_->shard_search_indices_ = std::make_unique<ShardDocIndices>();
}
EngineShard::EngineShard(util::ProactorBase* pb, mi_heap_t* heap) : 
queue_(kQueueLen, 1, 1),
queue2_(kQueueLen / 2, 2, 2),
shard_id_(pb->GetPoolIndex()),
mi_resource_(heap) {
    queue_.Start(absl::StrCat("shard_queue_", shard_id()));
    queue2_.Start(absl::StrCat("l2_queue_", shard_id()));
}
void EngineShard::DestroyThreadLocal() {
    if (!shard_)
        return;

    uint32_t shard_id = shard_->shard_id();
    mi_heap_t* tlh = shard_->mi_resource_.heap();
    shard_->Shutdown();
    shard_->~EngineShard();
    CleanupStatelessAllocMR();
    mi_free(shard_);
    shard_ = nullptr;
    mi_heap_delete(tlh);
}

void EngineShard::Shutdown() {
    queue_.Shutdown();
    queue2_.Shutdown();
}
}  // namespace dfly
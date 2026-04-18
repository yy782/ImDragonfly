#include "EngineShard.hpp"

thread_local mi_heap_t* data_heap = nullptr; // 线程本地堆指针
__thread EngineShard* EngineShard::shard_ = nullptr;
void EngineShard::InitThreadLocal(ProactorBase* pb) {
    data_heap = mi_heap_new();
    void* ptr = mi_heap_malloc_aligned(data_heap, sizeof(EngineShard), alignof(EngineShard));
    shard_ = new (ptr) EngineShard(pb, data_heap);
    InitTLStatelessAllocMR(shard_->memory_resource()); // 初始化无状态内存分配器
    //shard_->shard_search_indices_ = std::make_unique<ShardDocIndices>();
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
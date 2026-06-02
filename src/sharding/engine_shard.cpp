#include "engine_shard.hpp"
#include "detail/stateless_alloceator.hpp"
#include <glog/logging.h>


namespace dfly{
thread_local mi_heap_t* data_heap = nullptr;
thread_local EngineShard* EngineShard::shard_ = nullptr;

void EngineShard::InitThreadLocal(base::UringProactorPtr pb) {
    LOG(INFO) << "Initializing EngineShard thread local for proactor " << pb->GetPoolIndex();
    data_heap = mi_heap_new();
    void* ptr = mi_heap_malloc_aligned(data_heap, sizeof(EngineShard), alignof(EngineShard));
    shard_ = new (ptr) EngineShard(pb, data_heap);
    InitTLStatelessAllocMR(shard_->memory_resource());
    LOG(INFO) << "EngineShard thread local initialized, shard_id=" << shard_->shard_id();
}
EngineShard::EngineShard(base::UringProactorPtr pb, mi_heap_t* heap) : 
proactor_(pb),
shard_id_(pb->GetPoolIndex()),
mi_resource_(heap) {
}
void EngineShard::DestroyThreadLocal() {
    if (!shard_)
        return;
    mi_heap_t* tlh = shard_->mi_resource_.heap();
    shard_->Shutdown();
    shard_->~EngineShard();
    CleanupStatelessAllocMR();
    mi_free(shard_);
    shard_ = nullptr;
    mi_heap_delete(tlh);
}

void EngineShard::Shutdown() {
}

}  // namespace dfly
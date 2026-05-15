#include "engine_shard.hpp"
#include "detail/stateless_alloceator.hpp"
#include <glog/logging.h>


namespace dfly{
constexpr size_t kQueueLen = 64;
thread_local mi_heap_t* data_heap = nullptr; // 线程本地堆指针
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
queue_(kQueueLen),
shard_id_(pb->GetPoolIndex()),
mi_resource_(heap) {
    queue_.Start("shard_queue_"+std::to_string(shard_id()));
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
    queue_.Shutdown();

}
}  // namespace dfly
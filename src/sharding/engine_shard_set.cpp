#include "engine_shard_set.hpp"
#include "detail/common.hpp"
#include "namespaces.hpp"

#include <functional>
#include "util/maths.hpp"

namespace dfly{

EngineShardSet* shard_set = nullptr;

void EngineShardSet::Init(uint32_t sz) {
    shards_.reset(new EngineShard*[sz]);
    size_ = sz;
    //size_t max_shard_file_size = GetTieredFileLimit(sz);
    pp_->AwaitOnAll([this](base::UringProactorPtr pb) {
        InitThreadLocal(pb);
    });

    namespaces = new Namespaces();

}


void EngineShardSet::PreShutdown() {
}

void EngineShardSet::Shutdown() {
    namespaces->Clear();
    RunBlockingInParallel([](EngineShard*) { 
        EngineShard::DestroyThreadLocal(); 
    });
    delete namespaces;
    namespaces = nullptr;    
}


void EngineShardSet::InitThreadLocal(base::UringProactorPtr pb) {
    EngineShard::InitThreadLocal(pb);
    EngineShard* es = EngineShard::tlocal();
    shards_[es->shard_id()] = es;
}

ShardId Shard(std::string_view key)
{
    auto size = shard_set->size();
    size_t hash = std::hash<std::string_view>{}(key);

    if(util::isPowerOfTwo(size))
    {
        return hash & (size-1);
    }
    else 
        return hash % size;


    return hash;    
}


}  // namespace dfly
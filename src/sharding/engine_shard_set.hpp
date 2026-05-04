#pragma once
#include "engine_shard.hpp"
#include "base/uring_proactor_pool.hpp"

#include "util/Time.hpp"
#include <latch>
namespace dfly{
class TieredStorage;
class ShardDocIndices;
class BlockingController;
class EngineShardSet;

class EngineShardSet {
public:

    explicit EngineShardSet(base::UringProactorPool* pp) : 
    pp_(pp) {}
    uint32_t size() const { return size_; }
    base::UringProactorPool* pool() { return pp_; }
    void Init(uint32_t size);
    void PreShutdown();
    void Shutdown();


    // template <typename F> 
    // auto Await(ShardId sid, F&& f) { // 同步等待
    //     return shards_[sid]->GetQueue()->Await(std::forward<F>(f));
    // }
    template <typename F> 
    auto Add(ShardId sid, F&& f) { // 异步执行
        assert(sid < size_);
        return shards_[sid]->GetQueue()->Add(std::forward<F>(f));
    }
    template <typename U> 
    void RunBlockingInParallel(U&& func) {
        RunBlockingInParallel(std::forward<U>(func), [](uint32_t) {
                return true; 
            });
    }
    template <typename U, typename P> 
    void RunBlockingInParallel(U&& func, P&& pred) {
        uint32_t Count = 0;
        for (uint32_t i = 0; i < size(); ++i) {
            if (!pred(i))
                continue;
            Count++;
        }
        std::latch latch(Count);
        for (uint32_t i = 0; i < size(); ++i) {
            if (!pred(i))
                continue;
            auto dest = pp_->at(i);
            dest->DispatchBrief([&func, &latch]() mutable {
                func(EngineShard::tlocal());
                latch.count_down();
            });            
        }
        latch.wait();
    }

private:
    void InitThreadLocal(base::UringProactorPtr pb);
    base::UringProactorPool* pp_;
    std::unique_ptr<EngineShard*[]> shards_;
    uint32_t size_ = 0;
};





ShardId Shard(std::string_view key);



extern EngineShardSet* shard_set;
}  // namespace dfly



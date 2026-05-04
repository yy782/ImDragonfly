#pragma once
#include "engine_shard.hpp"
#include "base/proactor_pool.hpp"

#include "util/Time.hpp"

namespace dfly{
class TieredStorage;
class ShardDocIndices;
class BlockingController;
class EngineShardSet;

class EngineShardSet {
public:

    explicit EngineShardSet(util::UringProactorPool* pp) : 
    pp_(pp) {}

    uint32_t size() const { return size_; }

    util::UringProactorPool* pool() { return pp_; }

    void Init(uint32_t size);

    // Shutdown sequence:
    // - EngineShardSet.PreShutDown()
    // - Namespaces.Clear()
    // - EngineShardSet.Shutdown()
    void PreShutdown();
    void Shutdown();


    template <typename F> 
    auto Await(ShardId sid, F&& f) { // 同步等待
        return shards_[sid]->GetFiberQueue()->Await(std::forward<F>(f));
    }

    // Uses a shard queue to dispatch. Callback runs in a dedicated fiber.
    template <typename F> 
    auto Add(ShardId sid, F&& f) { // 异步执行
        assert(sid < size_);
        return shards_[sid]->GetFiberQueue()->Add(std::forward<F>(f));
    }

    template <typename F> auto 
    AddL2(ShardId sid, F&& f) { // 异步执行
        return shards_[sid]->GetSecondaryQueue()->Add(std::forward<F>(f));
    }

    template <typename U> 
    void RunBriefInParallel(U&& func) const { // 在所有分片上并行执行
        RunBriefInParallel(std::forward<U>(func), [](auto i) { return true; });
    }
    template <typename U, typename P> 
    void RunBriefInParallel(U&& func, P&& pred) const; // 在满足条件的分片上并行执行
    template <typename U> 
    void RunBlockingInParallel(U&& func) {
        RunBlockingInParallel(std::forward<U>(func), [](auto i) { return true; });
    }
    template <typename U, typename P> 
    void RunBlockingInParallel(U&& func, P&& pred);
    template <typename U> 
    void AwaitRunningOnShardQueue(U&& func) {
        util::BlockingCounter bc(size_);
        for (size_t i = 0; i < size_; ++i) {
        Add(i, [&func, bc]() mutable {
            func(EngineShard::tlocal());
            bc->Dec();
        });
        }

        bc->Wait();
    }
private:
    void InitThreadLocal(util::UringProactor* pb);
    base::UringProactorPool* pp_;
    std::unique_ptr<EngineShard*[]> shards_;
    uint32_t size_ = 0;
};

template <typename U, typename P>
void EngineShardSet::RunBriefInParallel(U&& func, P&& pred) const {
    util::BlockingCounter bc{0};

    for (uint32_t i = 0; i < size(); ++i) {
        if (!pred(i))
            continue;

        bc->Add(1);
        auto dest = pp_->at(i);
        dest->DispatchBrief([&func, bc]() mutable {
            func(EngineShard::tlocal());
            bc->Dec();
        });
    }
    bc->Wait();
}

template <typename U, typename P> 
void EngineShardSet::RunBlockingInParallel(U&& func, P&& pred) {
    util::BlockingCounter bc{0};


    for (uint32_t i = 0; i < size(); ++i) {
        if (!pred(i))
            continue;
        bc->Add(1);
        auto dest = pp_->at(i);
        dest->DispatchBrief([&func, bc]() mutable {
            func(EngineShard::tlocal());
            bc->Dec();
        });
    }
    bc->Wait();
}

ShardId Shard(std::string_view key);



extern EngineShardSet* shard_set;
}  // namespace dfly



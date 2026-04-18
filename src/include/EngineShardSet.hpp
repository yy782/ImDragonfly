
#include "EngineShard.hpp"
#include "util/proactor_pool.h"


class TieredStorage;
class ShardDocIndices;
class BlockingController;
class EngineShardSet;

class EngineShardSet {
public:
    struct CachedStats {
        std::atomic_uint64_t used_memory;
        CachedStats() : used_memory(0) {}

        CachedStats(const CachedStats& o) : 
        used_memory(o.used_memory.load()) {}
    };

    explicit EngineShardSet(util::ProactorPool* pp) : 
    pp_(pp) {}

    uint32_t size() const { return size_; }

    util::ProactorPool* pool() { return pp_; }

    void Init(uint32_t size, std::function<void()> shard_handler);

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
        util::fb2::BlockingCounter bc(size_);
        for (size_t i = 0; i < size_; ++i) {
        Add(i, [&func, bc]() mutable {
            func(EngineShard::tlocal());
            bc->Dec();
        });
        }

        bc->Wait();
    }
private:
    void InitThreadLocal(util::ProactorBase* pb);
    util::ProactorPool* pp_;
    std::unique_ptr<EngineShard*[]> shards_;
    uint32_t size_ = 0;
};

template <typename U, typename P>
void EngineShardSet::RunBriefInParallel(U&& func, P&& pred) const {
    util::fb2::BlockingCounter bc{0};

    for (uint32_t i = 0; i < size(); ++i) {
        if (!pred(i))
        continue;

        bc->Add(1);
        util::ProactorBase* dest = pp_->at(i);
        dest->DispatchBrief([&func, bc]() mutable {
        func(EngineShard::tlocal());
        bc->Dec();
        });
    }
    bc->Wait();
}

template <typename U, typename P> void EngineShardSet::RunBlockingInParallel(U&& func, P&& pred) {
    util::fb2::BlockingCounter bc{0};
    static_assert(std::is_invocable_v<U, EngineShard*>,
                    "Argument must be invocable EngineShard* as argument.");
    static_assert(std::is_void_v<std::invoke_result_t<U, EngineShard*>>,
                    "Callable must not have a return value!");

    for (uint32_t i = 0; i < size(); ++i) {
        if (!pred(i))
        continue;

        bc->Add(1);
        util::ProactorBase* dest = pp_->at(i);

        // the "Dispatch" call spawns a fiber underneath.
        dest->Dispatch([&func, bc]() mutable {
        func(EngineShard::tlocal());
        bc->Dec();
        });
    }
    bc->Wait();
}

ShardId Shard(std::string_view v, ShardId shard_num);

// absl::GetCurrentTimeNanos is twice faster than clock_gettime(CLOCK_REALTIME) on my laptop
// and 4 times faster than on a VM. it takes 5-10ns to do a call.

extern uint64_t TEST_current_time_ms;

inline uint64_t GetCurrentTimeMs() {
    return TEST_current_time_ms ? TEST_current_time_ms : absl::GetCurrentTimeNanos() / 1000000;
}

inline uint64_t GetCurrentTimeNs() {
    return TEST_current_time_ms ? TEST_current_time_ms * 1000000 : absl::GetCurrentTimeNanos();
}

extern EngineShardSet* shard_set;



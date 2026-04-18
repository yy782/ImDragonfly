#include <string_view>
#include <vector>
#include <string>

#include "helio/util/fibers/fiberqueue_threadpool.h"
#include "helio/util/fibers/fibers.h"

class TaskQueue {
public:
    TaskQueue(unsigned queue_size, unsigned start_size, unsigned pool_max_size):
    queue_(queue_size), consumer_fibers_(start_size){}

    template <typename F> 
    bool TryAdd(F&& f) {
        return queue_.TryAdd(std::forward<F>(f));
    }
    template <typename F> 
    bool Add(F&& f) {
        if (queue_.TryAdd(std::forward<F>(f)))
        return false;

        ++blocked_submitters_;
        auto res = queue_.Add(std::forward<F>(f));
        --blocked_submitters_;
        return res;
    }

    template <typename F> 
    auto Await(F&& f) -> decltype(f()) {
        util::fb2::Done done;
        using ResultType = decltype(f());
        util::detail::ResultMover<ResultType> mover;

        ++blocked_submitters_;
        Add([&mover, f = std::forward<F>(f), done]() mutable {
        mover.Apply(f);
        done.Notify();
        });
        --blocked_submitters_;
        done.Wait();
        return std::move(mover).get();
    }

    void Start(std::string_view base_name){
  for (size_t i = 0; i < consumer_fibers_.size(); ++i) {
        auto& fb = consumer_fibers_[i];

        std::string name = std::string(base_name)+std::to_string(i);
        fb =
            Fiber(Fiber::Opts{.priority = FiberPriority::HIGH, .name = name}, [this] { queue_.Run(); });
    }
    }

    void Shutdown(){
        queue_.Shutdown();
        for (auto& fb : consumer_fibers_)
            fb.JoinIfNeeded();
    }

    static unsigned blocked_submitters() {
        return blocked_submitters_;
    }

private:
    util::fb2::FiberQueue queue_;
    std::vector<util::fb2::Fiber> consumer_fibers_;

    static __thread unsigned blocked_submitters_;
};
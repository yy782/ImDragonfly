
#pragma once
#include <string_view>
#include <vector>
#include <string>
#include "util/task_queue.hpp"
#include "util/thread.hpp"


namespace dfly {



 


class TaskQueue {
public:
    TaskQueue(unsigned queue_size):
    queue_(queue_size){}

    template <typename F> 
    bool TryAdd(F&& f) {
        return queue_.TryAdd(std::forward<F>(f));
    }


    template <typename F> 
    cppcoro::task<bool> Add(F&& f) {
        if (queue_.TryAdd(std::forward<F>(f)))
            return false;
        auto res = co_await queue_.Add(std::forward<F>(f));
        return res;
    }

    template <typename F> 
    auto Await(F&& f) -> cppcoro::task<decltype(f())> {
        util::Done done;
        using ResultType = decltype(f());
        util::detail::ResultMover<ResultType> mover;
        Add([&mover, f = std::forward<F>(f), done]() mutable {
            mover.Apply(f);
            done.Notify();
        });
        done.Wait();
        return std::move(mover).get();
    }

    void Start(std::string_view base_name){
        worker_ = [this]()mutable{
                    while (queue_.isRuning()) {
                        queue_.Run();
                    }
                };
    }

    auto Shutdown(){
        queue_.Shutdown();
    }

private:
    util::TaskQueue queue_;
    util::Thread worker_;
};

}  // namespace dfly
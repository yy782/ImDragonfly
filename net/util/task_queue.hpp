
#pragma once
#include "util/synchronization.hpp"
#include "cppcoro/async_task.hpp"

#include "util/result_mover.hpp"
#include "util/lock_free_queue.hpp"
#include <functional>
#include <atomic>
namespace util{


class TaskQueue {
public:
    explicit TaskQueue(unsigned queue_size = 128): queue_(queue_size) {
    }


    template <typename F> 
    bool TryAdd(F&& f) {
        bool enqueued = queue_.try_enqueue(std::forward<F>(f));
        if (enqueued) {
            pull_ec_.notify();
            return true;
        }
        return false;
    }

    template <typename F> 
    cppcoro::AsyncTask<cppcoro::AsyncPromise> Add(F&& f) {
        while (true) {
            auto key = push_ec_.prepareWait();
            if (TryAdd(std::forward<F>(f))) {
                break;
            }
            co_await push_ec_.wait(key.epoch()); // 这里挂起协程后， 负责执行下文的线程是处理任务队列的线程
        }
        co_return;
    }

    void Shutdown(){
        is_closed_.store(true, std::memory_order_seq_cst);
        pull_ec_.notifyAll();


    }

    void Run(){
        CbFunc func;
        while (true) {
            pull_ec_.wait();
            if (is_closed_.load(std::memory_order_acquire))
                break;
            if (!queue_.try_dequeue(func)) {
                continue; 
            }
            push_ec_.notify();
            try {
                func();
            } catch (std::exception& e) {
            }
        }
    }

    bool isRuning() const { return !is_closed_.load(std::memory_order_relaxed); }
 private:
    // task index since the last preemption.
    using CbFunc = std::function<void()>;
    using FuncQ = util::mpmc_bounded_queue<CbFunc>;

    FuncQ queue_;

    EventCount push_ec_;
    ThreadEvent pull_ec_;
        

    std::atomic<bool> is_closed_{false};
};

}

#pragma once
#include "synchronization.hpp"

namespace util{


class TaskQueue {
public:
    explicit TaskQueue(unsigned queue_size = 128): queue_(queue_size) {
    }


    template <typename F> bool 
    TryAdd(F&& f) {
        // check if f can accept task index argument
        bool enqueued = queue_.try_enqueue(std::forward<F>(f));
        if (enqueued) {
            pull_ec_.notify();
            return true;
        }
        return false;
    }

    template <typename F> 
    cppcoro::task<bool> Add(F&& f) {
        if (TryAdd(std::forward<F>(f))) {
            return false;
        }

        bool result = false;
        while (true) {
            auto key = push_ec_.prepareWait();
            if (TryAdd(std::forward<F>(f))) {
                break;
            }
            result = true;
            co_await push_ec_.wait(key.epoch()); // 这里挂起协程后， 负责执行下文的线程是处理任务队列的线程
        }
        co_return result;
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

        co_await done.Wait();
        co_return std::move(mover).get();
    }

    void Shutdown(){
        is_closed_.store(true, memory_order_seq_cst);
        pull_ec_.notifyAll();
    }

    cppcoro::task<void> Run(){
        bool is_closed = false;
        CbFunc func;

        auto cb = [&] {
            if (queue_.try_dequeue(func)) {
                push_ec_.notify(); 
                return true;
            }

            if (is_closed_.load(std::memory_order_acquire)) {
                is_closed = true;
                return true;
            }

            return false;
        };

        while (true) {
            co_await pull_ec_.await(cb);
            if (is_closed)
                break;
            try {
                func();
            } catch (std::exception& e) {
                LOG(FATAL) << "Exception " << e.what();
            }
        }
    }

    bool isRuning() const { return !is_closed_.load(std::memory_order_relaxed); }
 private:
  // task index since the last preemption.
  using CbFunc = std::function<void()>;
  using FuncQ = base::mpmc_bounded_queue<CbFunc>;

  FuncQ queue_;

  EventCount push_ec_, pull_ec_;
  std::atomic<bool> is_closed_{false};
};

}
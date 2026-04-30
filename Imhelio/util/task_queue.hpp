

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
    bool Add(F&& f) {
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
            push_ec_.wait(key.epoch());
        }
        return result;
    }

  template <typename F> 
  auto Await(F&& f) -> decltype(f()) {
    Done done;
    using ResultType = decltype(f());
    util::detail::ResultMover<ResultType> mover;

    Add([&mover, f = std::forward<F>(f), done]() mutable {
        mover.Apply(f);
        done.Notify();
    });

    done.Wait();
    return std::move(mover).get();
  }

    void Shutdown(){
        is_closed_.store(true, memory_order_seq_cst);
        pull_ec_.notifyAll();
    }

  void Run(){
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
            pull_ec_.await(cb);
            if (is_closed)
                break;
            try {
                func();
            } catch (std::exception& e) {
                LOG(FATAL) << "Exception " << e.what();
            }
        }
    }

 private:
  // task index since the last preemption.
  using CbFunc = std::function<void()>;
  using FuncQ = base::mpmc_bounded_queue<CbFunc>;

  FuncQ queue_;

  EventCount push_ec_, pull_ec_;
  std::atomic_bool is_closed_{false};
};

}
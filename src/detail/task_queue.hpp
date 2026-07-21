
#pragma once
#include "task_queue.hpp"
#include "thread.hpp"



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
    auto Add(F&& f) { // 异步，创建协程等待任务被执行，保证任务的生命周期
        return queue_.Add(std::forward<F>(f));
    }

    void Start(std::string_view base_name){
        worker_ = base::Thread(base_name.data(), [this]()mutable{
                    queue_.Run();
                });
    }

    auto Shutdown(){
        queue_.Shutdown();

        worker_.join();
    }

private:
    base::TaskQueue queue_;
    base::Thread worker_;
};

}  // namespace dfly
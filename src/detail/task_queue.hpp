#include <string_view>
#include <vector>
#include <string>



namespace dfly {




struct Worker{
    using Task = cppcoro::task<void, task_promise<void, false>>;
    template<class Func>
    Task run(Func&& func){
        func();
        co_return;
    }
    std::string name;
    Task task;
};    


class TaskQueue {
public:
    TaskQueue(unsigned queue_size, unsigned start_size, unsigned pool_max_size):
    queue_(queue_size), consumers_(start_size){}

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
        util::Done done;
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
        for (size_t i = 0; i < consumers_.size(); ++i) {
                auto& worker = consumers_[i];

                std::string name = std::string(base_name)+std::to_string(i);
                worker.name = std::move(name);
                worker.task = worker.run([this]()mutable{
                    queue_.Run();
                });
        }
    }

    auto Shutdown(){
        queue_.Shutdown();
        for(const auto worker : consumers_){
            co_await worker.task;
        }        
    }
    static unsigned blocked_submitters() {
        return blocked_submitters_;
    }

private:
    util::TaskQueue queue_;
    std::vector<Worker> consumers_;

    static __thread unsigned blocked_submitters_;
};

}  // namespace dfly
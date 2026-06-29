#pragma once // uring_proactor.hpp  

#include "util/lock_free_queue.hpp"
#include "util/thread.hpp"
#include "util/task_queue.hpp"
#include "socket.hpp"
#include <liburing.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <functional>
#include <memory>
#include <atomic>
#include <thread>
#include <mutex>
#include <system_error>
#include <coroutine>
#include <unordered_map>
#include <chrono>
#include <poll.h>
namespace base {

class UringProactor {
public:
    using CbFunc = util::TaskQueue::CbFunc;
    explicit UringProactor(uint32_t index, size_t queue_size = 1024, size_t ring_size = 256);
    ~UringProactor();
    
    UringProactor(const UringProactor&) = delete;
    UringProactor& operator=(const UringProactor&) = delete;
    

    template<typename Func>
    bool DispatchBrief(Func&& f) {
        if (!task_queue_.TryAdd(std::forward<Func>(f))) {
            return false;
        }

        Wakeup();
        return true;
    }
    
    template<typename Func>
    cppcoro::AsyncTask AsyncAdd(Func&& f) {
        co_await task_queue_.AsyncAdd(std::forward<Func>(f));
        co_return;
    }
    

    void SubmitAccept(int fd, std::coroutine_handle<> handle, void* awaitable);
    void SubmitRead(int fd, void* buf, size_t len, off_t offset, 
                    std::coroutine_handle<> handle, void* awaitable);
    void SubmitWrite(int fd, const void* buf, size_t len, off_t offset,
                     std::coroutine_handle<> handle, void* awaitable);
    void SubmitClose(int fd, std::coroutine_handle<> handle, void* awaitable);
    

    void loop();
    void stop();
    
    uint32_t GetPoolIndex() const { return thread_index_; }
    pthread_t GetLoopThreadId() const { return loop_thread_id_; }
    util::TaskQueue& GetTaskQueue() { return task_queue_; }
private:
    
    struct PendingOp {
        int fd;
        int op_type;  // 0:accept, 1:read, 2:write, 3:close
        void* buf;
        size_t len;
        off_t offset;
        std::coroutine_handle<> handle;
        void* awaitable; 
    };
    
    void Wakeup();
    void DrainTasks();
    void SubmitPendingOps();
    void HandleCqe(struct io_uring_cqe* cqe);
    
    uint32_t thread_index_; 

    struct io_uring_ring;
    std::unique_ptr<io_uring_ring> ring_;
    int ring_fd_;
    int event_fd_;
    
    util::TaskQueue task_queue_;
    std::unique_ptr<base::mpmc_bounded_queue<PendingOp>> pending_ops_;
    
    std::atomic<bool> running_;
    std::atomic<bool> stop_;
    pthread_t loop_thread_id_;
    std::mutex submit_mutex_;
    
    static constexpr uint64_t WAKEUP_COOKIE = 0xFFFFFFFFFFFFFFFFULL;
    

};

} // namespace base

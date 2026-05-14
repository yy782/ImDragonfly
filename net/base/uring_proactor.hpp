#pragma once // uring_proactor.hpp  

#include "util/lock_free_queue.hpp"
#include "util/thread.hpp"
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
    explicit UringProactor(uint32_t index, size_t queue_size = 1024, size_t ring_size = 256);
    ~UringProactor();
    
    UringProactor(const UringProactor&) = delete;
    UringProactor& operator=(const UringProactor&) = delete;
    
    // 核心：入队任务（可从任意线程调用）
    template<typename Func>
    bool DispatchBrief(Func&& f) {
        // 将任务包装成 std::function 并入队
        auto task = std::make_unique<Task>(std::forward<Func>(f));
        if (!task_queue_->try_enqueue(std::move(task))) {
            return false;
        }
        // 唤醒事件循环
        Wakeup();
        return true;
    }
    
    // 协程相关接口
    void SubmitAccept(int fd, std::coroutine_handle<> handle, void* awaitable);
    void SubmitRead(int fd, void* buf, size_t len, off_t offset, 
                    std::coroutine_handle<> handle, void* awaitable);
    void SubmitWrite(int fd, const void* buf, size_t len, off_t offset,
                     std::coroutine_handle<> handle, void* awaitable);
    void SubmitClose(int fd, std::coroutine_handle<> handle, void* awaitable);
    
    // 启动事件循环
    void loop();
    void stop();
    
    uint32_t GetPoolIndex() const { return thread_index_; }
private:
    using Task = std::function<void()>;
    using TaskPtr = std::unique_ptr<Task>;
    
    struct PendingOp {
        int fd;
        int op_type;  // 0:accept, 1:read, 2:write, 3:close
        void* buf;
        size_t len;
        off_t offset;
        std::coroutine_handle<> handle;
        void* awaitable;  // 指向 Awaitable 对象，用于设置结果
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
    
    std::unique_ptr<base::mpmc_bounded_queue<TaskPtr>> task_queue_;
    std::unique_ptr<base::mpmc_bounded_queue<PendingOp>> pending_ops_;
    
    std::atomic<bool> running_;
    std::atomic<bool> stop_;
    pthread_t loop_thread_id_;
    std::mutex submit_mutex_;
    
    static constexpr uint64_t WAKEUP_COOKIE = 0xFFFFFFFFFFFFFFFFULL;
};

} // namespace base

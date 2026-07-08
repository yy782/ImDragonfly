#pragma once

#include "util/lock_free_queue.hpp"
#include "util/thread.hpp"
#include "util/task_queue.hpp"
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
#include <chrono>
#include <poll.h>
#include <vector>

namespace base {

struct UringConfig {
    int queue_depth = 256;             // SQ/CQ 环形队列深度，影响并发操作数
    bool use_defer_taskrun = true;     // 启用 DEFER_TASKRUN：必须为 true，确保协程正确性
    bool use_single_issuer = true;     // 单发布者优化，每个 Proactor 单线程运行时使用
    bool use_sqpoll = false;           // 内核侧轮询（SQPOLL），高吞吐但增加延迟
    uint32_t sqpoll_idle_ms = 10;    // SQPOLL 空闲超时（毫秒），0=永不睡眠
    bool use_registered_bufs = true;   // 使用注册缓冲区进行零拷贝接收
    int registered_buf_count = 256;    // 注册缓冲区数量，影响并发接收操作数
    int registered_buf_size = 65536;   // 每个注册缓冲区大小（字节），默认 64KB
    int cqe_batch_size = 32;           // 每次 PollOnce 处理的最大 CQE 数量
    uint32_t kSqeBatchSize = 32;
};

class UringProactor;
struct IoCompletionSlot {
    std::coroutine_handle<> coro;  // 协程句柄，IO完成时恢复执行
                                    // 在 AllocSlot() 中初始化为 nullptr，在 await_suspend() 中设置
    int32_t result = 0;             // IO操作结果（字节数或错误码）
    uint32_t flags = 0;             // 标志位，用于存储额外信息（如缓冲区索引）
};

class IoAwaitable {
public:
    IoAwaitable(UringProactor* p, uint32_t idx) noexcept
        : proactor_(p), slot_idx_(idx) {}

    bool await_ready() const noexcept { return false; }  // 总是挂起，等待IO完成
    void await_suspend(std::coroutine_handle<> h) noexcept;  // 存储协程句柄并提交SQE
    int await_resume() noexcept;  // 返回IO操作结果（字节数或错误码）

protected:
    UringProactor* proactor() const noexcept { return proactor_; }
    uint32_t slot_idx() const noexcept { return slot_idx_; }

private:
    friend class UringProactor;
    UringProactor* proactor_;  // 所属的 Proactor
    uint32_t slot_idx_;        // 在槽位表中的索引
};

struct RecvResult {
    int bytes;
    const char* data;  
    int buf_index = -1;     
};

class RecvAwaitable : public IoAwaitable {
public:
    using IoAwaitable::IoAwaitable;


    RecvResult await_resume() noexcept;
};




class UringProactor {
public:
    using CbFunc = util::TaskQueue::CbFunc;
    using IoResult = int;

    explicit UringProactor(uint32_t index, size_t queue_size = 1024, size_t ring_size = 256);
    ~UringProactor();
    
    UringProactor(const UringProactor&) = delete;
    UringProactor& operator=(const UringProactor&) = delete;

    template<typename Func>
    bool DispatchBrief(Func&& f) {
        if (!task_queue_.TryAdd(std::forward<Func>(f))) {
            return false;
        }
        WakeupIfNeeded();
        return true;
    }
    
    template<typename Func>
    cppcoro::AsyncTask AsyncAdd(Func&& f) {
        co_await task_queue_.AsyncAdd(std::forward<Func>(f));
        co_return;
    }

    using CbType = std::function<void(IoResult, uint32_t)>;

    struct SubmitEntry {
        explicit SubmitEntry(io_uring_sqe* sqe) : sqe_(sqe) {}
        
        io_uring_sqe* sqe() { return sqe_; }
        const io_uring_sqe* sqe() const { return sqe_; }
        
        void PrepAccept(int fd) {
            io_uring_prep_accept(sqe_, fd, nullptr, nullptr, 0);
        }
        
        void PrepRead(int fd, void* buf, size_t len, off_t offset = 0) {
            io_uring_prep_read(sqe_, fd, buf, len, offset);
        }
        
        void PrepWrite(int fd, const void* buf, size_t len, off_t offset = 0) {
            io_uring_prep_write(sqe_, fd, buf, len, offset);
        }
        
        void PrepClose(int fd) {
            io_uring_prep_close(sqe_, fd);
        }
        
        void PrepPollAdd(int fd, uint32_t events) {
            io_uring_prep_poll_add(sqe_, fd, events);
        }
        
    private:
        io_uring_sqe* sqe_;
    };

    SubmitEntry GetSubmitEntry(CbType cb, uint32_t submit_tag = 0);

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
    struct CompletionEntry {
        CbType cb;
        int64_t index = -1;
    };

    void WakeupIfNeeded();
    void WakeRing();
    void ArmWakeupEvent();
    bool DrainTasks();
    void ProcessCqeBatch(unsigned count, io_uring_cqe** cqes);
    void ReapCompletions(unsigned count, io_uring_cqe** cqes);
    void RegrowCentries();

    uint32_t thread_index_;

    struct io_uring_ring {
        struct io_uring ring;
        io_uring_ring() { memset(&ring, 0, sizeof(ring)); }
        ~io_uring_ring() { io_uring_queue_exit(&ring); }
    };
    std::unique_ptr<io_uring_ring> ring_;

    uint8_t msgring_supported_f_ : 1;
    uint8_t poll_first_ : 1;
    uint8_t taskrun_flag_f_ : 1;
    uint8_t : 5;

    int ring_fd_;
    int event_fd_;
    
    util::TaskQueue task_queue_;
    
    std::atomic<bool> running_;
    std::atomic<bool> stop_;
    std::atomic<uint32_t> tq_seq_{0};
    std::atomic<uint32_t> tq_wakeup_ev_{0};
    std::atomic<uint32_t> tq_wakeup_skipped_ev_{0};
    pthread_t loop_thread_id_;

    static constexpr uint64_t kIgnoreIndex = 0;
    static constexpr uint64_t kWakeIndex = 1;
    static constexpr uint64_t kUserDataCbIndex = 1024;
    static constexpr uint32_t WAIT_SECTION_STATE = 1UL << 31;

    uint32_t kSqeBatchSize;

    
    uint32_t reg_buf_count_ = 0;
};

using UringProactorPtr = std::shared_ptr<UringProactor>;

} // namespace base
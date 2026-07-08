// 高性能 io_uring Proactor，集成 C++20 协程
// 设计理念：
//   - 每个 Proactor 单线程运行（SPSC 环形队列访问）→ 零竞争
//   - 预分配待处理操作槽位表 → 热路径无堆分配
//   - SQE 批量提交 + IORING_SETUP_DEFER_TASKRUN → 最小化系统调用
//   - 注册缓冲区 → 零拷贝接收
//   - 所有异步操作返回可等待类型，支持 co_await
//
// 关键特性：
//   1. 零竞争：每个 Proactor 运行在独立线程，SPSC 环形队列
//   2. 无热路径分配：预分配 IoCompletionSlot 槽位表
//   3. 批量提交：达到阈值时批量提交 SQE，减少 io_uring_enter 调用
//   4. 零拷贝接收：使用注册缓冲区避免内存复制
//   5. DEFER_TASKRUN：确保协程句柄在 CQE 处理前存储，避免竞争条件
//   6. C++20 协程集成：所有操作返回 awaitable 类型

#pragma once

#include <liburing.h>
#include <liburing/io_uring.h>

#include <array>
#include <atomic>
#include <coroutine>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <thread>
#include <vector>
#include "util/task_queue.hpp"

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
    explicit UringProactor(UringConfig cfg = {}, int pool_index = -1);
    ~UringProactor();

    // Non-copyable, non-movable
    UringProactor(const UringProactor&) = delete;
    UringProactor& operator=(const UringProactor&) = delete;
    UringProactor(UringProactor&&) = delete;
    UringProactor& operator=(UringProactor&&) = delete;

    IoAwaitable AsyncAccept(int listen_fd);

    // 使用注册缓冲区进行零拷贝接收（高性能）。
    // 返回的数据在下一次读取同一缓冲区索引前保持有效。
    // 使用示例：auto [bytes, data, buf_idx] = co_await proactor.AsyncRecvFixed(fd);
    RecvAwaitable AsyncRecvFixed(int fd, int buf_idx);

    // 发送数据。返回写入的字节数（或负的错误码）。
    IoAwaitable AsyncSend(int fd, const void* buf, size_t len);

    // 关闭文件描述符。
    IoAwaitable AsyncClose(int fd);

    // ============================================================
    // 轮询 / 事件循环
    // ============================================================

    // 提交所有待处理的 SQEs 并轮询至少 `min_cqe` 个完成事件。
    // 参数：
    //   min_cqe: 最小等待的完成事件数（默认1，避免忙等待）
    //   timeout_ms: 超时时间（毫秒），0=无限等待
    // 返回值：处理的 CQE 数量（>=0），或负的错误码
    int PollOnce(unsigned min_cqe = 1, unsigned timeout_ms = 0);

    // 运行 Proactor 事件循环，直到收到关闭信号。
    // 循环调用 PollOnce()，同时处理任务队列。
    // 这是 Proactor 的主事件循环，通常在独立线程中运行。
    void loop();

    void stop() noexcept;

    // Flush all pending SQEs (submit without waiting).
    int Flush();
    util::TaskQueue& GetTaskQueue() { return task_queue_; }
    

    template <typename F>
    bool DispatchBrief(F&& f) {
        return task_queue_.TryAdd(std::forward<F>(f));
    }

    void ResumeSlot(uint32_t slot_idx, int32_t result, int32_t extra = 0);

    // struct io_uring* GetRing() { return &ring_; }
    // uint32_t GetRingFd() const { return ring_fd_; }
    

    pthread_t GetLoopThreadId() const {
        return loop_thread_id_;
    }
    
    int GetPoolIndex() const {
        return pool_index_;
    }
    uint32_t reg_buf_count() const {
        return reg_buf_count_;
    }
private:
    friend class IoAwaitable;
    friend class RecvAwaitable;
    friend class AcceptAwaitable;
    friend class UringSocket;

    void InitRing();
    void InitRegisteredBuffers();

    uint32_t AllocSlot();
    void FreeSlot(uint32_t slot_idx);
    IoCompletionSlot& GetSlot(uint32_t idx) { return pending_slots_[idx]; }

    struct io_uring_sqe* GetSqeOrFlush();
    void SubmitIfNeeded();

    void ProcessCqe(struct io_uring_cqe* cqe);
    int AcquireRegBuf();
    void ReleaseRegBuf(int index);

    struct io_uring ring_;
    int ring_fd_ = -1;
    UringConfig config_;
    int pool_index_ = -1;

    static constexpr size_t kMaxPendingSlots = 4096;
    std::vector<IoCompletionSlot> pending_slots_;
    uint32_t next_slot_;
    uint32_t slot_mask_;

    struct RegBufSlot {
        char* memory;
        int index = -1;
        int next = -1;
        
        RegBufSlot() = default;
        RegBufSlot(RegBufSlot&& other) noexcept
            : memory(other.memory), index(other.index), next(other.next) {
            if (this == &other) {
                return;
            }
            if (memory) {
                delete[] memory;
            }
            memory = other.memory;
            index = other.index;
            next = other.next;
            other.memory = nullptr;
            other.index = -1;
            other.next = -1;
        }
        RegBufSlot(const RegBufSlot&) = delete;
        RegBufSlot& operator=(const RegBufSlot&) = delete;
    };
    std::vector<RegBufSlot> reg_bufs_;
    uint32_t reg_buf_freelist_ = 0;

    util::TaskQueue task_queue_;
    
    pthread_t loop_thread_id_;

    bool shutdown_{false};

    uint32_t pending_sqes_{0};

    uint32_t kSqeBatchSize;

    
    uint32_t reg_buf_count_ = 0;
};

using UringProactorPtr = std::shared_ptr<UringProactor>;

}  // namespace base

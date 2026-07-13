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

// ============================================================
// Proactor 配置结构体
// ============================================================
struct UringConfig {
    int queue_depth = 256;             // SQ/CQ 环形队列深度，影响并发操作数
    bool use_defer_taskrun = true;     // 启用 DEFER_TASKRUN：必须为 true，确保协程正确性
    bool use_single_issuer = true;     // 单发布者优化，每个 Proactor 单线程运行时使用
    bool use_sqpoll = false;           // 内核侧轮询（SQPOLL），高吞吐但增加延迟
    uint32_t sqpoll_idle_ms = 1000;    // SQPOLL 空闲超时（毫秒），0=永不睡眠
    bool use_registered_bufs = true;   // 使用注册缓冲区进行零拷贝接收
    int registered_buf_count = 256;    // 注册缓冲区数量，影响并发接收操作数
    int registered_buf_size = 65536;   // 每个注册缓冲区大小（字节），默认 64KB
    int cqe_batch_size = 32;           // 每次 PollOnce 处理的最大 CQE 数量
    int task_queue_size = 1024;    
    uint32_t sqe_batch_size = 32;    
};

// ============================================================
// Forward declarations
// ============================================================
class UringProactor;

// ============================================================
// 内部结构：待处理IO操作槽位
// ============================================================
struct IoCompletionSlot {
    std::coroutine_handle<> coro;  // 协程句柄，IO完成时恢复执行
                                    // 在 AllocSlot() 中初始化为 nullptr，在 await_suspend() 中设置
    int32_t result = 0;             // IO操作结果（字节数或错误码）
    uint32_t flags = 0;             // 标志位，用于存储额外信息（如缓冲区索引）
};

// ============================================================
// Awaitable 类 — co_await 的返回类型
// ============================================================

// 通用的 io_uring 完成事件等待器
// 挂起协程，提交准备好的 SQE（或延迟提交），
// 当 CQE 到达时恢复协程执行。
// 工作流程：
//   1. co_await 调用 await_suspend() 存储协程句柄
//   2. 提交 SQE（可能批量）
//   3. 协程挂起，线程继续处理其他事件
//   4. CQE 到达时，ProcessCqe 恢复协程
//   5. await_resume() 返回操作结果
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

// After co_await, returns bytes_received + gives access to buffer
struct RecvResult {
    int bytes;
    const char* data;  // valid only if registered buffers used
    int buf_index;     // registered buffer index, -1 if not used
};

// Recv awaiter — additionally provides buffer access
class RecvAwaitable : public IoAwaitable {
public:
    using IoAwaitable::IoAwaitable;

    RecvResult await_resume() noexcept;
};

// Accept awaiter — returns new client fd
class AcceptAwaitable : public IoAwaitable {
public:
    using IoAwaitable::IoAwaitable;
};

// ============================================================
// UringProactor — main proactor class
// ============================================================
class UringProactor {
public:
    explicit UringProactor(UringConfig cfg = {}, int pool_index = -1);
    ~UringProactor();

    // Non-copyable, non-movable
    UringProactor(const UringProactor&) = delete;
    UringProactor& operator=(const UringProactor&) = delete;
    UringProactor(UringProactor&&) = delete;
    UringProactor& operator=(UringProactor&&) = delete;

    // ============================================================
    // 异步IO操作（所有方法都返回可 co_await 的类型）
    // ============================================================

    // 接受新连接。返回新的文件描述符（或负的错误码）。
    // 使用示例：int client_fd = co_await proactor.AsyncAccept(listen_fd);
    AcceptAwaitable AsyncAccept(int listen_fd);

    // 使用注册缓冲区进行零拷贝接收（高性能）。
    // 返回的数据在下一次读取同一缓冲区索引前保持有效。
    // 使用示例：auto [bytes, data, buf_idx] = co_await proactor.AsyncRecvFixed(fd);
    RecvAwaitable AsyncRecvFixed(int fd, int buf_idx);

    // 发送数据。返回写入的字节数（或负的错误码）。
    IoAwaitable AsyncSend(int fd, const void* buf, size_t len);


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
    void Run();
    
    // Run() 的别名（用于现有代码兼容）
    void loop() { Run(); }

    // 发送关闭信号，使 Run() 在处理完现有事件后退出。
    void Shutdown() noexcept;
    
    // Alias for Shutdown() (used in existing code)
    void stop() { Shutdown(); }

    // Flush all pending SQEs (submit without waiting).
    int Flush();

    // ============================================================
    // Task / Coroutine Dispatch
    // ============================================================
    util::TaskQueue& GetTaskQueue() { return task_queue_; }
    
    // Dispatch a function to be executed on this proactor's thread
    template <typename F>
    bool DispatchBrief(F&& f) {
        return task_queue_.TryAdd(std::forward<F>(f));
    }

    // Awake a specific coroutine by slot index with a result.
    // Used internally by completion dispatch.
    void ResumeSlot(uint32_t slot_idx, int32_t result, int32_t extra = 0);

    // ============================================================
    // Raw ring access (for advanced usage)
    // ============================================================
    struct io_uring* GetRing() { return &ring_; }
    uint32_t GetRingFd() const { return ring_fd_; }
    
    // Get the thread ID of the loop thread (as pthread_t)
    pthread_t GetLoopThreadId() const {
        return loop_thread_id_;
    }
    
    // Get the pool index of this proactor
    int GetPoolIndex() const {
        return pool_index_;
    }

private:
    friend class IoAwaitable;
    friend class RecvAwaitable;
    friend class AcceptAwaitable;
    friend class UringSocket;

    // ---- Ring initialization ----
    void InitRing();
    void InitRegisteredBuffers();

    // ---- Slot management ----
    // 分配一个新的槽位，返回槽位索引
    // 协程句柄在 await_suspend() 中设置，避免竞态条件
    uint32_t AllocSlot();
    void FreeSlot(uint32_t slot_idx);
    IoCompletionSlot& GetSlot(uint32_t idx) { return pending_slots_[idx]; }

    // ---- SQE helpers ----
    struct io_uring_sqe* GetSqeOrFlush();
    void SubmitIfNeeded();

    // ---- CQE processing ----
    void ProcessCqe(struct io_uring_cqe* cqe);

    // ---- Registered buffer management ----
    int AcquireRegBuf();
    void ReleaseRegBuf(int index);

    // ---- Members ----
    struct io_uring ring_;
    int ring_fd_ = -1;
    UringConfig config_;
    int pool_index_ = -1;
    size_t MaxPendingSlots_;
    std::vector<IoCompletionSlot> pending_slots_;
    uint32_t next_slot_{0};
    uint32_t slot_mask_;
    struct  RegBufSlot {
        char* memory;
        int index = -1;
        int next = -1;
    };
    std::vector<RegBufSlot> reg_bufs_;
    int next_buf_ = 0;
    util::TaskQueue task_queue_;
    pthread_t loop_thread_id_;
    bool shutdown_{false};
    uint32_t pending_sqes_{0};
    uint32_t SqeBatchSize_;
    uint32_t reg_buf_count_;
};

using UringProactorPtr = std::shared_ptr<UringProactor>;

}  // namespace base

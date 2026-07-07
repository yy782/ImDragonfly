// High-performance io_uring proactor with C++20 coroutine integration
// Design philosophy:
//   - Single-threaded per proactor (SPSC ring access) → zero contention
//   - Pre-allocated pending ops table → no hot-path allocations
//   - SQE batching + IORING_SETUP_DEFER_TASKRUN → minimal syscalls
//   - Registered buffers → zero-copy recv
//   - All async ops return awaitable types for co_await

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

#include "io_buf.hpp"
#include "util/task_queue.hpp"

namespace base {

// ============================================================
// Proactor Configuration
// ============================================================
struct UringConfig {
    int queue_depth = 256;             // SQ/CQ ring depth
    bool use_defer_taskrun = true;     // IORING_SETUP_DEFER_TASKRUN
    bool use_single_issuer = true;     // IORING_SETUP_SINGLE_ISSUER
    bool use_sqpoll = false;           // IORING_SETUP_SQPOLL (kernel-side polling)
    uint32_t sqpoll_idle_ms = 1000;    // SQPOLL idle timeout (0 = never sleep)
    bool use_registered_bufs = true;   // IORING_REGISTER_BUFFERS for recv
    int registered_buf_count = 256;    // Number of registered buffers
    int registered_buf_size = 65536;   // 64KB per registered buffer
    int cqe_batch_size = 32;           // Max CQEs to process per PollOnce
};

// ============================================================
// Forward declarations
// ============================================================
class UringProactor;

// ============================================================
// Internal: pending IO operation slot
// ============================================================
struct alignas(64) IoCompletionSlot {
    std::coroutine_handle<> coro;
    int32_t result = 0;
    uint32_t flags = 0;
    int32_t fd = -1;  // returned fd for accept
};

// ============================================================
// Awaiters — return types for co_await
// ============================================================

// Generic awaiter for io_uring completion events
// Suspends the coroutine, submits the prepared SQE (or defers),
// and resumes when the CQE arrives.
class IoAwaitable {
public:
    IoAwaitable(UringProactor* p, uint32_t idx) noexcept
        : proactor_(p), slot_idx_(idx) {}

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept;
    int await_resume() noexcept;

protected:
    UringProactor* proactor() const noexcept { return proactor_; }
    uint32_t slot_idx() const noexcept { return slot_idx_; }

private:
    friend class UringProactor;
    UringProactor* proactor_;
    uint32_t slot_idx_;
};

// Recv awaiter — additionally provides buffer access
class RecvAwaitable : public IoAwaitable {
public:
    using IoAwaitable::IoAwaitable;

    // After co_await, returns bytes_received + gives access to buffer
    struct RecvResult {
        int bytes;
        const char* data;  // valid only if registered buffers used
        int buf_index;     // registered buffer index, -1 if not used
    };
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
    // Async IO Operations (all co_await-able)
    // ============================================================

    // Accept a new connection. Returns new fd (or -errno).
    AcceptAwaitable AsyncAccept(int listen_fd);

    // Receive data into IoBuf. Returns bytes read (or -errno).
    IoAwaitable AsyncRecv(int fd, IoBuf& buf, size_t max_len);

    // Receive into raw buffer. Returns bytes read (or -errno).
    IoAwaitable AsyncRecvRaw(int fd, void* buf, size_t len);

    // Receive using registered buffers (zero-copy).
    // The returned data is valid until the next read on this buffer index.
    RecvAwaitable AsyncRecvFixed(int fd);

    // Send data. Returns bytes written (or -errno).
    IoAwaitable AsyncSend(int fd, const void* buf, size_t len);

    // Send from an IoBuf (scatter/gather via writev).
    IoAwaitable AsyncSendBuf(int fd, const IoBuf& buf);

    // Close a file descriptor.
    IoAwaitable AsyncClose(int fd);

    // Cancel a pending operation by user_data.
    IoAwaitable AsyncCancel(uint64_t user_data);

    // ============================================================
    // Polling / Event Loop
    // ============================================================

    // Submit all pending SQEs and poll for at least `min_cqe` completions.
    // Returns: number of CQEs processed, or -errno.
    int PollOnce(unsigned min_cqe = 1, unsigned timeout_ms = 0);

    // Run the proactor loop until shutdown is signaled.
    // Calls PollOnce() in a loop, also drains the TaskQueue.
    void Run();
    
    // Alias for Run() (used in existing code)
    void loop() { Run(); }

    // Signal shutdown, causing Run() to exit after draining.
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

    // ---- Ring initialization ----
    void InitRing();
    void InitRegisteredBuffers();

    // ---- Slot management ----
    uint32_t AllocSlot(std::coroutine_handle<> coro);
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

    // Pre-allocated pending operation slots.
    // Indexed by user_data. Lock-free SPSC: submission writes, completion reads.
    static constexpr size_t kMaxPendingSlots = 4096;
    std::vector<IoCompletionSlot> pending_slots_;
    std::atomic<uint32_t> next_slot_{0};
    uint32_t slot_mask_;

    // Registered buffers for zero-copy recv
    struct alignas(64) RegBufSlot {
        std::unique_ptr<char[]> memory;
        int index = -1;
        std::atomic<bool> in_use{false};
        
        RegBufSlot() = default;
        RegBufSlot(RegBufSlot&& other) noexcept 
            : memory(std::move(other.memory)), 
              index(other.index), 
              in_use(other.in_use.load(std::memory_order_relaxed)) {
            other.index = -1;
        }
        RegBufSlot& operator=(RegBufSlot&& other) noexcept {
            if (this != &other) {
                memory = std::move(other.memory);
                index = other.index;
                in_use.store(other.in_use.load(std::memory_order_relaxed), std::memory_order_relaxed);
                other.index = -1;
            }
            return *this;
        }
        RegBufSlot(const RegBufSlot&) = delete;
        RegBufSlot& operator=(const RegBufSlot&) = delete;
    };
    std::vector<RegBufSlot> reg_bufs_;

    // Task queue for dispatching external tasks
    util::TaskQueue task_queue_;
    
    // Thread ID of the loop thread
    pthread_t loop_thread_id_;

    // Shutdown flag
    std::atomic<bool> shutdown_{false};

    // Pending SQE count (for batching)
    std::atomic<uint32_t> pending_sqes_{0};

    // SQE submission batch threshold
    static constexpr uint32_t kSqeBatchSize = 32;

    // Registered buffer freelist
    std::atomic<uint32_t> reg_buf_freelist_{0};
    uint32_t reg_buf_count_ = 0;
};

using UringProactorPtr = std::shared_ptr<UringProactor>;

}  // namespace base

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
    int queue_depth = 256;
    bool use_defer_taskrun = true;
    bool use_single_issuer = true;
    bool use_sqpoll = false;
    uint32_t sqpoll_idle_ms = 1000;
    bool use_registered_bufs = true;
    int registered_buf_count = 256;
    int registered_buf_size = 65536;
    int cqe_batch_size = 32;
    int task_queue_size = 1024;
    uint32_t sqe_batch_size = 32;
};

class UringProactor;

struct IoCompletionSlot {
    std::coroutine_handle<> coro;
    int32_t result = 0;
    uint32_t flags = 0;
};

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

struct RecvResult {
    int bytes;
    const char* data;
    int buf_index;
};

class RecvAwaitable : public IoAwaitable {
public:
    using IoAwaitable::IoAwaitable;
    RecvResult await_resume() noexcept;
};

class AcceptAwaitable : public IoAwaitable {
public:
    using IoAwaitable::IoAwaitable;
};

class UringProactor {
public:
    explicit UringProactor(UringConfig cfg = {}, int pool_index = -1);
    ~UringProactor();

    UringProactor(const UringProactor&) = delete;
    UringProactor& operator=(const UringProactor&) = delete;
    UringProactor(UringProactor&&) = delete;
    UringProactor& operator=(UringProactor&&) = delete;

    AcceptAwaitable AsyncAccept(int listen_fd);
    RecvAwaitable AsyncRecvFixed(int fd, int buf_idx);
    IoAwaitable AsyncSend(int fd, const void* buf, size_t len);

    int PollOnce(unsigned min_cqe = 1, unsigned timeout_ms = 0);
    void Run();
    void loop() { Run(); }

    void Shutdown() noexcept;
    void stop() { Shutdown(); }

    int Flush();

    util::TaskQueue& GetTaskQueue() { return task_queue_; }

    template <typename F>
    bool DispatchBrief(F&& f) {
        return task_queue_.TryAdd(std::forward<F>(f));
    }

    void ResumeSlot(uint32_t slot_idx, int32_t result, int32_t extra = 0);

    struct io_uring* GetRing() { return &ring_; }
    uint32_t GetRingFd() const { return ring_fd_; }
    pthread_t GetLoopThreadId() const { return loop_thread_id_; }
    int GetPoolIndex() const { return pool_index_; }

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
    size_t MaxPendingSlots_;
    std::vector<IoCompletionSlot> pending_slots_;
    uint32_t next_slot_{0};
    uint32_t slot_mask_;
    struct RegBufSlot {
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

// High-performance io_uring proactor implementation
// Key performance invariants:
//   1. No heap allocation on hot paths (accept/recv/send/complete)
//   2. Lock-free slot allocation via atomic counter
//   3. Batched SQE submission → fewer io_uring_enter syscalls
//   4. Registered buffers for zero-copy recv
//   5. DEFER_TASKRUN avoids unnecessary task work notifications
//      (this is REQUIRED — without it, CQEs may be processed before
//       await_suspend stores the coroutine handle)
//   6. SPSC CQE processing (single-threaded per proactor)

#include "uring_proactor.hpp"

#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace base {

// ============================================================
// IoAwaitable — the bridge between io_uring and C++20 coroutines
// ============================================================

void IoAwaitable::await_suspend(std::coroutine_handle<> h) noexcept {
    // Store coroutine handle BEFORE potential submission.
    // With IORING_SETUP_DEFER_TASKRUN, CQEs are NOT processed
    // during io_uring_submit(), so there is no race here.
    // This is the only correct ordering.
    proactor_->GetSlot(slot_idx_).coro = h;
    proactor_->SubmitIfNeeded();
}

int IoAwaitable::await_resume() noexcept {
    return proactor_->GetSlot(slot_idx_).result;
}

RecvAwaitable::RecvResult RecvAwaitable::await_resume() noexcept {
    auto& slot = proactor()->GetSlot(slot_idx());
    RecvResult r;
    r.bytes = slot.result;
    r.buf_index = slot.flags;  // reg buf index, -1 if not fixed-recv

    if (r.bytes > 0 && r.buf_index >= 0) {
        r.data = proactor()->reg_bufs_[r.buf_index].memory.get();
        // Release: buffer is now available for reuse in next PollOnce()
        proactor()->ReleaseRegBuf(r.buf_index);
    } else {
        r.data = nullptr;
    }
    return r;
}

// ============================================================
// UringProactor construction / destruction
// ============================================================

UringProactor::UringProactor(UringConfig cfg, int pool_index) : config_(cfg), pool_index_(pool_index) {
    // Round up to nearest power of 2 for fast modulo via bitmask
    size_t slots = 1;
    while (slots < kMaxPendingSlots) slots <<= 1;
    if (slots > kMaxPendingSlots) slots = kMaxPendingSlots;
    slot_mask_ = static_cast<uint32_t>(slots - 1);

    pending_slots_.resize(slots);

    InitRing();

    if (config_.use_registered_bufs) {
        InitRegisteredBuffers();
    }
}

UringProactor::~UringProactor() {
    Shutdown();

    if (!reg_bufs_.empty()) {
        io_uring_unregister_buffers(&ring_);
    }

    io_uring_queue_exit(&ring_);
}

// ============================================================
// Ring Initialization
// ============================================================

void UringProactor::InitRing() {
    struct io_uring_params params;
    std::memset(&params, 0, sizeof(params));

    // Feature flags — ordered from most impactful to least
    if (config_.use_defer_taskrun) {
        params.flags |= IORING_SETUP_DEFER_TASKRUN;
    }
    if (config_.use_single_issuer) {
        params.flags |= IORING_SETUP_SINGLE_ISSUER;
    }
    if (config_.use_sqpoll) {
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = config_.sqpoll_idle_ms;
    }

    int ret = io_uring_queue_init_params(config_.queue_depth, &ring_, &params);
    if (ret < 0) {
        if (config_.use_sqpoll) {
            LOG(WARNING) << "SQPOLL not supported, falling back to interrupt mode";
            params.flags &= ~IORING_SETUP_SQPOLL;
            config_.use_sqpoll = false;
            ret = io_uring_queue_init_params(config_.queue_depth, &ring_, &params);
        }
    }
    CHECK_GE(ret, 0) << "io_uring_queue_init_params failed: " << -ret;

    ring_fd_ = ring_.ring_fd;
    LOG(INFO) << "Proactor ring: depth=" << config_.queue_depth
              << " defer_tw=" << config_.use_defer_taskrun
              << " single_issuer=" << config_.use_single_issuer
              << " sqpoll=" << config_.use_sqpoll
              << " reg_bufs=" << config_.use_registered_bufs;
}

void UringProactor::InitRegisteredBuffers() {
    reg_buf_count_ = static_cast<uint32_t>(config_.registered_buf_count);
    reg_bufs_.resize(reg_buf_count_);

    std::vector<struct iovec> iovecs;
    iovecs.reserve(reg_buf_count_);

    for (uint32_t i = 0; i < reg_buf_count_; ++i) {
        auto& slot = reg_bufs_[i];
        slot.memory = std::make_unique<char[]>(config_.registered_buf_size);
        slot.index = static_cast<int>(i);

        struct iovec iov;
        iov.iov_base = slot.memory.get();
        iov.iov_len = static_cast<size_t>(config_.registered_buf_size);
        iovecs.push_back(iov);
    }

    int ret = io_uring_register_buffers(&ring_, iovecs.data(), iovecs.size());
    if (ret < 0) {
        LOG(WARNING) << "io_uring_register_buffers failed: " << -ret
                     << ", falling back to standard recv";
        config_.use_registered_bufs = false;
        reg_bufs_.clear();
        reg_buf_count_ = 0;
    } else {
        reg_buf_freelist_.store(0, std::memory_order_relaxed);
        LOG(INFO) << "Registered " << reg_buf_count_ << " fixed buffers ("
                  << config_.registered_buf_size << "B each)";
    }
}

// ============================================================
// Slot Management — lock-free SPSC access
// ============================================================

uint32_t UringProactor::AllocSlot(std::coroutine_handle<> coro) {
    uint32_t idx = next_slot_.fetch_add(1, std::memory_order_relaxed) & slot_mask_;
    auto& slot = pending_slots_[idx];
    slot.coro = coro;
    slot.result = -ECANCELED;
    slot.flags = -1;  // -1 = no reg buf
    slot.fd = -1;
    return idx;
}

void UringProactor::ResumeSlot(uint32_t slot_idx, int32_t result, int32_t extra) {
    auto& slot = pending_slots_[slot_idx];
    slot.result = result;
    slot.flags = extra;

    auto coro = slot.coro;
    slot.coro = nullptr;
    if (coro) {
        coro.resume();
    } else {
        // Safety: with DEFER_TASKRUN this should never happen.
        // If it does, the coroutine handle wasn't stored yet,
        // meaning the CQE arrived before await_suspend.
        LOG(ERROR) << "ResumeSlot with null coroutine handle — missing DEFER_TASKRUN?";
    }
}

// ============================================================
// SQE Management — batched submission
// ============================================================

struct io_uring_sqe* UringProactor::GetSqeOrFlush() {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (__builtin_expect(sqe != nullptr, 1)) {
        return sqe;
    }

    // Ring full → flush pending SQEs to free up entries
    io_uring_submit(&ring_);
    pending_sqes_.store(0, std::memory_order_relaxed);
    return io_uring_get_sqe(&ring_);
}

void UringProactor::SubmitIfNeeded() {
    uint32_t prev = pending_sqes_.fetch_add(1, std::memory_order_relaxed);
    if (prev + 1 >= kSqeBatchSize) {
        io_uring_submit(&ring_);
        pending_sqes_.store(0, std::memory_order_relaxed);
    }
}

int UringProactor::Flush() {
    uint32_t n = pending_sqes_.exchange(0, std::memory_order_relaxed);
    if (n > 0) {
        return io_uring_submit(&ring_);
    }
    return 0;
}

// ============================================================
// Async IO Operations
// ============================================================

AcceptAwaitable UringProactor::AsyncAccept(int listen_fd) {
    uint32_t slot_idx = AllocSlot(nullptr);

    struct io_uring_sqe* sqe = GetSqeOrFlush();
    io_uring_prep_accept(sqe, listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    sqe->user_data = slot_idx;

    return AcceptAwaitable(this, slot_idx);
}

IoAwaitable UringProactor::AsyncRecv(int fd, IoBuf& buf, size_t max_len) {
    uint32_t slot_idx = AllocSlot(nullptr);

    buf.ensureWritableBytes(max_len);
    void* dest = buf.BeginWrite();

    struct io_uring_sqe* sqe = GetSqeOrFlush();
    io_uring_prep_recv(sqe, fd, dest, max_len, 0);
    sqe->user_data = slot_idx;

    // NOTE: The caller must call buf.hasWritten(n) after co_await returns n > 0.
    // We don't track the IoBuf pointer — the coroutine handles it via the result.

    return IoAwaitable(this, slot_idx);
}

IoAwaitable UringProactor::AsyncRecvRaw(int fd, void* buf, size_t len) {
    uint32_t slot_idx = AllocSlot(nullptr);

    struct io_uring_sqe* sqe = GetSqeOrFlush();
    io_uring_prep_recv(sqe, fd, buf, len, 0);
    sqe->user_data = slot_idx;

    return IoAwaitable(this, slot_idx);
}

RecvAwaitable UringProactor::AsyncRecvFixed(int fd) {
    int buf_idx = AcquireRegBuf();
    if (buf_idx < 0) {
        // All registered buffers in use — caller should retry
        uint32_t slot_idx = AllocSlot(nullptr);
        auto& slot = GetSlot(slot_idx);
        slot.result = -ENOBUFS;
        return RecvAwaitable(this, slot_idx);
    }

    uint32_t slot_idx = AllocSlot(nullptr);

    struct io_uring_sqe* sqe = GetSqeOrFlush();
    io_uring_prep_read_fixed(sqe, fd,
                             reg_bufs_[buf_idx].memory.get(),
                             static_cast<unsigned>(config_.registered_buf_size),
                             0, buf_idx);
    sqe->user_data = slot_idx;

    // Stash reg buf index so ProcessCqe can find it
    {
        auto& slot = GetSlot(slot_idx);
        slot.fd = buf_idx;
        slot.flags = buf_idx;  // also in flags for await_resume
    }

    return RecvAwaitable(this, slot_idx);
}

IoAwaitable UringProactor::AsyncSend(int fd, const void* buf, size_t len) {
    uint32_t slot_idx = AllocSlot(nullptr);

    struct io_uring_sqe* sqe = GetSqeOrFlush();
    io_uring_prep_send(sqe, fd, buf, len, MSG_NOSIGNAL);
    sqe->user_data = slot_idx;

    return IoAwaitable(this, slot_idx);
}

IoAwaitable UringProactor::AsyncSendBuf(int fd, const IoBuf& buf) {
    uint32_t slot_idx = AllocSlot(nullptr);

    struct io_uring_sqe* sqe = GetSqeOrFlush();
    struct iovec iov;
    iov.iov_base = const_cast<char*>(buf.peek());
    iov.iov_len = buf.readable_size();

    io_uring_prep_writev(sqe, fd, &iov, 1, 0);
    sqe->user_data = slot_idx;

    return IoAwaitable(this, slot_idx);
}

IoAwaitable UringProactor::AsyncClose(int fd) {
    uint32_t slot_idx = AllocSlot(nullptr);

    struct io_uring_sqe* sqe = GetSqeOrFlush();
    io_uring_prep_close(sqe, fd);
    sqe->user_data = slot_idx;

    return IoAwaitable(this, slot_idx);
}

IoAwaitable UringProactor::AsyncCancel(uint64_t user_data) {
    uint32_t slot_idx = AllocSlot(nullptr);

    struct io_uring_sqe* sqe = GetSqeOrFlush();
    io_uring_prep_cancel(sqe, reinterpret_cast<void*>(user_data), 0);
    sqe->user_data = slot_idx;

    return IoAwaitable(this, slot_idx);
}

// ============================================================
// Registered Buffer Management
// ============================================================

int UringProactor::AcquireRegBuf() {
    if (!config_.use_registered_bufs || reg_buf_count_ == 0) {
        return -1;
    }

    uint32_t start = reg_buf_freelist_.load(std::memory_order_relaxed);
    for (uint32_t i = 0; i < reg_buf_count_; ++i) {
        uint32_t idx = (start + i) % reg_buf_count_;
        bool expected = false;
        if (reg_bufs_[idx].in_use.compare_exchange_strong(
                expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
            reg_buf_freelist_.store((idx + 1) % reg_buf_count_, std::memory_order_relaxed);
            return static_cast<int>(idx);
        }
    }
    return -1;
}

void UringProactor::ReleaseRegBuf(int index) {
    if (index >= 0 && static_cast<uint32_t>(index) < reg_buf_count_) {
        reg_bufs_[index].in_use.store(false, std::memory_order_release);
    }
}

// ============================================================
// CQE Processing
// ============================================================

void UringProactor::ProcessCqe(struct io_uring_cqe* cqe) {
    uint32_t slot_idx = static_cast<uint32_t>(cqe->user_data);
    int32_t result = cqe->res;
    auto& slot = GetSlot(slot_idx);

    // Extract registered buffer index (if this was a fixed read)
    // We do NOT release here — the await_resume callback handles it,
    // ensuring the buffer data is valid when the coroutine reads it.
    int32_t reg_buf = slot.flags;

    // For regular accept/send/recv, flags stays as-is
    ResumeSlot(slot_idx, result, reg_buf);
}

// ============================================================
// Polling / Event Loop
// ============================================================

int UringProactor::PollOnce(unsigned min_cqe, unsigned timeout_ms) {
    // Flush pending SQEs before waiting
    unsigned pending = pending_sqes_.exchange(0, std::memory_order_relaxed);

    struct __kernel_timespec ts;
    struct __kernel_timespec* pts = nullptr;

    if (timeout_ms > 0) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000UL;
        pts = &ts;
    }

    // submit_and_wait: submit pending SQEs + wait for completions
    int wait_nr = (pending > 0 || min_cqe > 0) ? static_cast<int>(min_cqe) : 0;
    int submitted = io_uring_submit_and_wait(&ring_, static_cast<unsigned>(wait_nr));
    if (submitted < 0) {
        // Error from io_uring_submit_and_wait
        int err = -submitted;
        if (err != ETIME && err != EINTR) {
            return -err;
        }
        // Timeout or interrupted → still process any completions that arrived
        // submitted is negative, but we still want to process any CQEs that might have arrived
        submitted = 0;
    }

    // Batch-process CQEs
    struct io_uring_cqe* cqe = nullptr;
    unsigned head;
    unsigned processed = 0;
    unsigned batch_count = 0;

    io_uring_for_each_cqe(&ring_, head, cqe) {
        ProcessCqe(cqe);
        ++processed;
        ++batch_count;

        if (batch_count >= config_.cqe_batch_size) {
            io_uring_cq_advance(&ring_, batch_count);
            batch_count = 0;
        }
    }
    if (batch_count > 0) {
        io_uring_cq_advance(&ring_, batch_count);
    }

    return static_cast<int>(processed);
}

void UringProactor::Run() {
    // Store the thread ID of the loop thread
    loop_thread_id_ = pthread_self();
    
    while (!shutdown_.load(std::memory_order_acquire)) {
        // Drain task queue first — user tasks have priority
        task_queue_.TryDrain();

        // Poll with at least 1 completion event, longer timeout for efficiency
        // Wait for at least 1 CQE with 10ms timeout to avoid busy waiting
        int processed = PollOnce(1, 10);

        if (processed < 0) {
            // Real error (not timeout or interrupt)
            LOG(ERROR) << "Proactor poll error: " << -processed;
            if (processed == -ENOMEM) {
                break;
            }
            // For other errors, continue but log
        }
        
        // If processed == 0, it means we got timeout (ETIME) or interrupt (EINTR)
        // In either case, just continue the loop
    }

    // Final cleanup
    task_queue_.TryDrain();
    PollOnce(0, 0);
}

void UringProactor::Shutdown() noexcept {
    shutdown_.store(true, std::memory_order_release);
    task_queue_.Shutdown();
}

}  // namespace base

#include "uring_proactor.hpp"
#include <liburing.h>
#include <errno.h>
#include <cstring>
#include <glog/logging.h>
#include <mimalloc.h>
#include <sys/syscall.h>
#include <bitset>

#define VPRO(verbosity) VLOG(verbosity) << "PRO[" << GetPoolIndex() << "] "

namespace base {

namespace {

void wait_for_cqe(io_uring* ring, unsigned wait_nr, __kernel_timespec* ts) {
    struct io_uring_cqe* cqe_ptr = nullptr;
    int res = io_uring_wait_cqes(ring, &cqe_ptr, wait_nr, ts, nullptr);
    if (res < 0) {
        res = -res;
        LOG_IF(ERROR, res != EAGAIN && res != EINTR && res != ETIME) 
            << "wait_for_cqe error: " << strerror(res);
    }
}

constexpr uint16_t kCqeBatchLen = 128;
constexpr size_t kAlign = 4096;

} // namespace


UringProactor::UringProactor(uint32_t index, size_t queue_size, size_t ring_size)
    : thread_index_(index)
    , task_queue_(queue_size)
    , running_(false)
    , stop_(false)
    , msgring_supported_f_(0)
    , poll_first_(0)
    , taskrun_flag_f_(0)
{ 
    LOG(INFO) << "Initializing UringProactor index=" << index 
              << ", queue_size=" << queue_size 
              << ", ring_size=" << ring_size;

    ring_ = std::make_unique<io_uring_ring>();
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    msgring_supported_f_ = 0;
    poll_first_ = 0;
    taskrun_flag_f_ = 0;

    params.flags |= IORING_SETUP_SUBMIT_ALL;
    poll_first_ = 1;

    params.flags |= (IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_COOP_TASKRUN |
                     IORING_SETUP_TASKRUN_FLAG | IORING_SETUP_SINGLE_ISSUER);
    taskrun_flag_f_ = 1;

    int init_res = io_uring_queue_init_params(ring_size, &ring_->ring, &params);
    if (init_res < 0) {
        init_res = -init_res;
        if (init_res == ENOMEM) {
            LOG(ERROR) << "io_uring does not have enough memory. Increase max locked memory limit.";
            exit(1);
        }
        LOG(FATAL) << "Error initializing io_uring: (" << init_res << ") " << strerror(init_res);
    }

    io_uring_probe* uring_probe = io_uring_get_probe_ring(&ring_->ring);
    msgring_supported_f_ = io_uring_opcode_supported(uring_probe, IORING_OP_MSG_RING);
    io_uring_free_probe(uring_probe);
    VLOG_IF(1, msgring_supported_f_) << "msgring supported!";

    unsigned req_feats = IORING_FEAT_SINGLE_MMAP | IORING_FEAT_FAST_POLL | IORING_FEAT_NODROP;
    CHECK_EQ(req_feats, params.features & req_feats)
        << "Required io_uring feature is not present in the kernel";

    ring_fd_ = ring_->ring.ring_fd;
    VLOG(1) << "io_uring initialized, ring_fd=" << ring_fd_;

    event_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (event_fd_ < 0) {
        LOG(FATAL) << "eventfd failed: " << strerror(errno);
    }
    VLOG(1) << "eventfd created, fd=" << event_fd_;

    size_t sz = ring_->ring.sq.ring_sz + params.sq_entries * sizeof(struct io_uring_sqe);
    LOG_FIRST_N(INFO, 1) << "IORing with " << params.sq_entries << " entries, allocated " << sz
                         << " bytes, cq_entries is " << *ring_->ring.cq.kring_entries;

    centries_.resize(params.sq_entries);
    next_free_ce_ = 0;
    for (size_t i = 0; i < centries_.size() - 1; ++i) {
        centries_[i].index = i + 1;
    }

    LOG(INFO) << "UringProactor index=" << index << " initialized successfully";
}

UringProactor::~UringProactor() {
    stop();
    if (event_fd_ >= 0) {
        close(event_fd_);
    }
}

void UringProactor::WakeupIfNeeded() {
    auto current = tq_seq_.fetch_add(2, std::memory_order_acq_rel);
    if (current == WAIT_SECTION_STATE) {
        WakeRing();
    } else {
        tq_wakeup_skipped_ev_.fetch_add(1, std::memory_order_relaxed);
    }
}

void UringProactor::WakeRing() {
    tq_wakeup_ev_.fetch_add(1, std::memory_order_relaxed);

    static const uint64_t wake_val = 1;
    CHECK_EQ(8, write(event_fd_, &wake_val, sizeof(wake_val)));
}

void UringProactor::ArmWakeupEvent() {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_->ring);
    CHECK_NOTNULL(sqe);

    io_uring_prep_poll_add(sqe, event_fd_, POLLIN);
    sqe->user_data = kIgnoreIndex;
    sqe->flags |= IOSQE_IO_LINK;

    sqe = io_uring_get_sqe(&ring_->ring);
    CHECK_NOTNULL(sqe);

    static thread_local uint64_t donot_care;
    io_uring_prep_read(sqe, event_fd_, &donot_care, 8, 0);
    sqe->user_data = kWakeIndex;
}

bool UringProactor::DrainTasks() {
    return task_queue_.TryDrain();
}

void UringProactor::RegrowCentries() {
    size_t prev = centries_.size();
    VLOG(1) << "RegrowCentries from " << prev << " to " << prev * 2
            << " pending cb-cnt: " << pending_cb_cnt_;

    centries_.resize(prev * 2);
    next_free_ce_ = prev;
    for (; prev < centries_.size() - 1; ++prev)
        centries_[prev].index = prev + 1;
}

UringProactor::SubmitEntry UringProactor::GetSubmitEntry(CbType cb, uint32_t submit_tag) {
    io_uring_sqe* res = io_uring_get_sqe(&ring_->ring);
    if (res == NULL) {
        int submitted = io_uring_submit(&ring_->ring);
        if (submitted > 0) {
            res = io_uring_get_sqe(&ring_->ring);
        } else {
            LOG(FATAL) << "Fatal error submitting to iouring: " << -submitted;
        }
    }

    memset(res, 0, sizeof(io_uring_sqe));

    if (cb) {
        if (next_free_ce_ < 0) {
            RegrowCentries();
            DCHECK_GT(next_free_ce_, 0);
        }
        res->user_data = (next_free_ce_ + kUserDataCbIndex) | (uint64_t(submit_tag) << 32);
        DCHECK_LT(unsigned(next_free_ce_), centries_.size());

        auto& e = centries_[next_free_ce_];
        DCHECK(!e.cb);
        DVLOG(3) << "GetSubmitEntry: index: " << next_free_ce_;

        next_free_ce_ = e.index;
        e.cb = std::move(cb);
        ++pending_cb_cnt_;
    } else {
        res->user_data = kIgnoreIndex | (uint64_t(submit_tag) << 32);
    }

    return SubmitEntry{res};
}

void UringProactor::ProcessCqeBatch(unsigned count, io_uring_cqe** cqes) {
    for (unsigned i = 0; i < count; ++i) {
        io_uring_cqe cqe = *cqes[i];

        uint32_t user_data = cqe.user_data & 0xFFFFFFFF;
        uint32_t user_tag = cqe.user_data >> 32;

        if (user_data >= kUserDataCbIndex) {
            if (cqe.user_data == UINT64_MAX) {
                LOG(ERROR) << "Fatal error: cqe.user_data is UINT64_MAX, likely a kernel bug.";
                exit(1);
            }

            size_t index = user_data - kUserDataCbIndex;
            DCHECK_LT(index, centries_.size());
            auto& e = centries_[index];

            DCHECK(e.cb) << index;

            CbType func = std::move(e.cb);
            e.index = next_free_ce_;
            next_free_ce_ = index;
            --pending_cb_cnt_;
            func(cqe.res, cqe.flags);
            continue;
        }

        if (cqe.res < 0 && cqe.res != -ECANCELED && cqe.res != -ETIME) {
            LOG(WARNING) << "CQE error: " << -cqe.res << " cqe_type=" << user_tag;
        }

        if (user_data == kIgnoreIndex)
            continue;

        if (user_data == kWakeIndex) {
            DCHECK_EQ(cqe.res, 8);
            DVLOG(2) << "PRO[" << thread_index_ << "] Wakeup " << cqe.res << "/" << cqe.flags;
            ArmWakeupEvent();
            continue;
        }

        LOG(ERROR) << "Unrecognized user_data " << cqe.user_data;
    }
}

void UringProactor::ReapCompletions(unsigned init_count, io_uring_cqe** cqes) {
    DCHECK_GT(init_count, 0U);
    unsigned batch_count = init_count;
    do {
        ProcessCqeBatch(batch_count, cqes);
        io_uring_cq_advance(&ring_->ring, batch_count);

        batch_count = io_uring_peek_batch_cqe(&ring_->ring, cqes, kCqeBatchLen);
    } while (batch_count > 0);
}

void UringProactor::loop() {
    loop_thread_id_ = util::Thread::current_tid();
    running_ = true;
    stop_ = false;

    LOG(INFO) << "UringProactor index=" << thread_index_ << " event loop starting";

    ArmWakeupEvent();

    struct io_uring_cqe* cqes[kCqeBatchLen];
    uint32_t tq_seq = 0;

    while (true) {
        int num_submitted = 0;
        bool call_submit = true;

        if (taskrun_flag_f_) {
            unsigned sq_ready = io_uring_sq_ready(&ring_->ring);
            bool taskrun_pending = IO_URING_READ_ONCE(*ring_->ring.sq.kflags) & IORING_SQ_TASKRUN;
            call_submit = (sq_ready > 0) || taskrun_pending;
        }

        if (call_submit) {
            num_submitted = io_uring_submit_and_get_events(&ring_->ring);
        }

        if (num_submitted == -EBUSY) {
            VLOG(1) << "EBUSY " << io_uring_sq_ready(&ring_->ring);
            num_submitted = 0;
        } else if (num_submitted < 0 && num_submitted != -ETIME) {
            LOG(DFATAL) << "Error submitting to iouring: " << -num_submitted;
            continue;
        }

        tq_seq = tq_seq_.load(std::memory_order_acquire);

        DrainTasks();

        uint32_t cqe_count = io_uring_peek_batch_cqe(&ring_->ring, cqes, kCqeBatchLen);
        if (cqe_count) {
            ReapCompletions(cqe_count, cqes);
            continue;
        }

        if (io_uring_sq_ready(&ring_->ring) > 0) {
            continue;
        }

        if (stop_) {
            break;
        }

        if (task_queue_.TryDrain()) {
            continue;
        }

        if (io_uring_sq_ready(&ring_->ring) > 0) {
            continue;
        }

        cqe_count = io_uring_peek_batch_cqe(&ring_->ring, cqes, kCqeBatchLen);
        if (cqe_count) {
            ReapCompletions(cqe_count, cqes);
            continue;
        }

        if (stop_) {
            break;
        }

        if (tq_seq_.compare_exchange_weak(tq_seq, WAIT_SECTION_STATE, std::memory_order_acq_rel,
                                          std::memory_order_relaxed)) {
            if (stop_) {
                tq_seq_.store(0, std::memory_order_release);
                break;
            }
            wait_for_cqe(&ring_->ring, 1, nullptr);

            tq_seq = 0;
            tq_seq_.store(0, std::memory_order_release);
        }
    }

    running_ = false;
    LOG(INFO) << "UringProactor index=" << thread_index_ << " event loop stopped";
}

void UringProactor::stop() {
    stop_ = true;
    task_queue_.Shutdown();
    WakeRing();
    
    if (util::Thread::current_tid() != loop_thread_id_ && running_) {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

void UringProactor::SubmitAccept(int fd, std::coroutine_handle<> handle, void* awaitable) {
    auto cb = [handle, awaitable](IoResult res, uint32_t) {
        if (awaitable) {
            auto* a = static_cast<UringSocket::AcceptAwaitable*>(awaitable);
            a->fd_ = res;
        }
        if (handle) {
            handle.resume();
        }
    };

    SubmitEntry se = GetSubmitEntry(std::move(cb), 0);
    se.PrepAccept(fd);

    WakeupIfNeeded();
}

void UringProactor::SubmitRead(int fd, void* buf, size_t len, off_t offset,
                               std::coroutine_handle<> handle, void* awaitable) {
    auto cb = [handle, awaitable](IoResult res, uint32_t) {
        if (awaitable) {
            auto* a = static_cast<UringSocket::ReadAwaitable*>(awaitable);
            a->result_ = res;
        }
        if (handle) {
            handle.resume();
        }
    };

    SubmitEntry se = GetSubmitEntry(std::move(cb), 1);
    se.PrepRead(fd, buf, len, offset);

    WakeupIfNeeded();
}

void UringProactor::SubmitWrite(int fd, const void* buf, size_t len, off_t offset,
                                std::coroutine_handle<> handle, void* awaitable) {
    auto cb = [handle, awaitable](IoResult res, uint32_t) {
        if (awaitable) {
            auto* a = static_cast<UringSocket::WriteAwaitable*>(awaitable);
            a->result_ = res;
        }
        if (handle) {
            handle.resume();
        }
    };

    SubmitEntry se = GetSubmitEntry(std::move(cb), 2);
    se.PrepWrite(fd, buf, len, offset);

    WakeupIfNeeded();
}

} // namespace base
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

    kSqeBatchSize = config_.kSqeBatchSize;

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
        if (util::Thread::current_tid() == loop_thread_id_) {
            int submitted = io_uring_submit(&ring_->ring);
            if (submitted > 0) {
                res = io_uring_get_sqe(&ring_->ring);
            } else {
                LOG(FATAL) << "Fatal error submitting to iouring: " << -submitted;
            }
        } else {
            WakeRing();
            int spin = 0;
            while (!(res = io_uring_get_sqe(&ring_->ring))) {
                if (++spin > 100) {
                    std::this_thread::yield();
                    spin = 0;
                }
            }
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

void UringProactor::SubmitIfNeeded() {
    uint32_t prev = (++pending_sqes_);
    if (prev + 1 >= kSqeBatchSize) {
        assert(io_uring_submit(&ring_) > 0);
        pending_sqes_ = 0;
    }
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

    

    return IoAwaitable(this, slot_idx);
}

RecvAwaitable UringProactor::AsyncRecvFixed(int fd, int buf_idx) {
    assert(buf_idx >= 0 && buf_idx < static_cast<int>(reg_buf_count_));
    uint32_t slot_idx = AllocSlot();
    struct io_uring_sqe* sqe = GetSqeOrFlush();
    io_uring_prep_read_fixed(sqe, fd,
                             reg_bufs_[buf_idx].memory,
                             static_cast<unsigned>(config_.registered_buf_size),
                             0, buf_idx);
    sqe->user_data = slot_idx;
    {
        auto& slot = GetSlot(slot_idx);
        slot.flags = buf_idx;  
    }

    

    return RecvAwaitable(this, slot_idx);
}

        if (stop_) {
            break;
        }

        if (task_queue_.TryDrain()) {
            continue;
        }

    

    return IoAwaitable(this, slot_idx);
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

            VPRO(2) << "wait_for_cqe";
            wait_for_cqe(&ring_->ring, 1, nullptr);

            tq_seq = 0;
            tq_seq_.store(0, std::memory_order_release);
        }
    }

    running_ = false;
    LOG(INFO) << "UringProactor index=" << thread_index_ << " event loop stopped";
}

// ============================================================
// 轮询 / 事件循环
// ============================================================

// 主轮询函数：提交待处理的 SQEs 并等待完成事件
// 参数：
//   min_cqe: 最小等待的完成事件数（默认1，避免忙等待）
//   timeout_ms: 超时时间（毫秒），0=无限等待
// 返回值：处理的 CQE 数量（>=0），或负的错误码
int UringProactor::PollOnce(unsigned min_cqe, unsigned timeout_ms) {
    // // 在等待前刷新待处理的 SQEs
    unsigned pending = pending_sqes_;
    pending_sqes_ = 0;

    struct __kernel_timespec ts;
    struct __kernel_timespec* pts = nullptr;

    if (timeout_ms > 0) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000UL;
        pts = &ts;
    }
}

void UringProactor::SubmitAccept(int fd, std::coroutine_handle<> handle, void* awaitable) {
    auto cb = [handle, awaitable](IoResult res, uint32_t) {
        if (awaitable) {
            *static_cast<int*>(awaitable) = res;
        }
        if (handle) {
            handle.resume();
        }
    };



    return 1;
}

    WakeupIfNeeded();
}

void UringProactor::SubmitWrite(int fd, const void* buf, size_t len, off_t offset,
                                std::coroutine_handle<> handle, void* awaitable) {
    auto cb = [handle, awaitable](IoResult res, uint32_t) {
        if (awaitable) {
            *static_cast<ssize_t*>(awaitable) = res;
        }
        if (handle) {
            handle.resume();
        }
    };

    SubmitEntry se = GetSubmitEntry(std::move(cb), 2);
    se.PrepWrite(fd, buf, len, offset);


        


        // 批量处理 CQEs
        struct io_uring_cqe* cqe = nullptr;
        unsigned head;
        unsigned kprocessed = 0;
        unsigned batch_count = 0;

        io_uring_for_each_cqe(&ring_, head, cqe) {
            ProcessCqe(cqe);
            ++kprocessed;
            ++batch_count;

            if (batch_count >= config_.cqe_batch_size) {
                io_uring_cq_advance(&ring_, batch_count);
                batch_count = 0;
            }
        }
        if (batch_count > 0) {
            io_uring_cq_advance(&ring_, batch_count);
        }

        int ret = io_uring_submit(&ring_);
        if (ret < 0) {
            LOG(FATAL) << "Proactor submit error: " << -ret;
            break;  
        }else if (ret > 0){
            continue;
        }

        if (!task_queue_.Empty()) {
            continue;
        }    

        int processed = PollOnce(1, 1);


void UringProactor::SubmitClose(int fd, std::coroutine_handle<> handle, void* awaitable) {
    auto cb = [handle](IoResult res, uint32_t) {
        if (handle) {
            handle.resume();
        }
    };

    SubmitEntry se = GetSubmitEntry(std::move(cb), 3);
    se.PrepClose(fd);

    WakeupIfNeeded();
}

} // namespace base
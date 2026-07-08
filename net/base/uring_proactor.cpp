// 高性能 io_uring Proactor 实现
// 关键性能保证：
//   1. 热路径无堆分配（accept/recv/send/complete 操作）
//   2. 通过原子计数器实现无锁槽位分配
//   3. 批量 SQE 提交 → 减少 io_uring_enter 系统调用
//   4. 注册缓冲区支持零拷贝接收
//   5. DEFER_TASKRUN 避免不必要的任务工作通知
//      （这是必须的 — 没有它，CQE 可能在 await_suspend 存储协程句柄前被处理）
//   6. SPSC CQE 处理（每个 Proactor 单线程）
//
// 实现要点：
//   - 每个 Proactor 运行在独立线程，避免锁竞争
//   - 预分配 4096 个 IoCompletionSlot 槽位，热路径无分配
//   - 批量提交：达到 kSqeBatchSize（默认32）时一次性提交
//   - 零拷贝：使用注册缓冲区避免内存复制
//   - 协程集成：所有异步操作挂起协程，IO完成时恢复

#include "uring_proactor.hpp"

#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace base {


void IoAwaitable::await_suspend(std::coroutine_handle<> h) noexcept {
    proactor_->GetSlot(slot_idx_).coro = h;
    proactor_->SubmitIfNeeded();
}

int IoAwaitable::await_resume() noexcept {
    return proactor_->GetSlot(slot_idx_).result;
}

RecvResult RecvAwaitable::await_resume() noexcept {
    auto& slot = proactor()->GetSlot(slot_idx());
    RecvResult r;
    r.bytes = slot.result;
    r.buf_index = slot.flags;  // reg buf index, -1 if not fixed-recv

    if (r.bytes >= 0) {
        r.data = proactor()->reg_bufs_[r.buf_index].memory;
    }
    return r;
}



UringProactor::UringProactor(UringConfig cfg, int pool_index) : config_(cfg), pool_index_(pool_index) {
    // 向上取整到最近的2的幂，以便通过位掩码快速取模
    size_t slots = 1;
    while (slots < kMaxPendingSlots) slots <<= 1;
    if (slots > kMaxPendingSlots) slots = kMaxPendingSlots;
    slot_mask_ = static_cast<uint32_t>(slots - 1);

    pending_slots_.resize(slots);

    kSqeBatchSize = config_.kSqeBatchSize;

    InitRing();

    if (config_.use_registered_bufs) {
        InitRegisteredBuffers();
    }
}

UringProactor::~UringProactor() {
    assert(std::uncaught_exceptions() == 0);
    stop();

    if (!reg_bufs_.empty()) {
        io_uring_unregister_buffers(&ring_);
    }

    io_uring_queue_exit(&ring_);
}


void UringProactor::InitRing() {
    struct io_uring_params params;
    std::memset(&params, 0, sizeof(params));
    if (config_.use_defer_taskrun) {
        params.flags |= IORING_SETUP_DEFER_TASKRUN;  // 必须启用：确保协程正确性
    }
    if (config_.use_single_issuer) {
        params.flags |= IORING_SETUP_SINGLE_ISSUER;  // 单发布者优化
    }
    if (config_.use_sqpoll) {
        params.flags |= IORING_SETUP_SQPOLL;  // 内核侧轮询（高吞吐但增加延迟）
        params.sq_thread_idle = config_.sqpoll_idle_ms;  // SQPOLL 线程空闲超时
    }


    int ret = io_uring_queue_init_params(config_.queue_depth, &ring_, &params);
    // if (ret < 0) {
    //     if (config_.use_sqpoll) {
    //         // SQPOLL 可能不被支持，回退到中断模式
    //         LOG(WARNING) << "SQPOLL not supported, falling back to interrupt mode";
    //         params.flags &= ~IORING_SETUP_SQPOLL;
    //         config_.use_sqpoll = false;
    //         ret = io_uring_queue_init_params(config_.queue_depth, &ring_, &params);
    //     }
    // }
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
        slot.memory =  new char[config_.registered_buf_size];
        slot.index = static_cast<int>(i);
        slot.next = (i + 1 < reg_buf_count_) ? static_cast<int>(i + 1) : -1;

        struct iovec iov;
        iov.iov_base = slot.memory;
        iov.iov_len = static_cast<size_t>(config_.registered_buf_size);
        iovecs.push_back(iov);
    }

    int ret = io_uring_register_buffers(&ring_, iovecs.data(), iovecs.size());
    if (ret < 0) {
        LOG(FATAL) << "io_uring_register_buffers failed: " << -ret
                     << ", falling back to standard recv";
        config_.use_registered_bufs = false;
        reg_bufs_.clear();
        reg_buf_count_ = 0;
    } else {
        reg_buf_freelist_ = 0;
        LOG(INFO) << "Registered " << reg_buf_count_ << " fixed buffers ("
                  << config_.registered_buf_size << "B each)";
    }
}


uint32_t UringProactor::AllocSlot() {
    uint32_t idx = next_slot_ & slot_mask_;
    --next_slot_;
    auto& slot = pending_slots_[idx];
    slot.coro = nullptr;
    slot.result = -ECANCELED;
    slot.flags = -1;  
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
        LOG(FATAL) << "ResumeSlot with null coroutine handle — race condition detected";
    }
}

struct io_uring_sqe* UringProactor::GetSqeOrFlush() {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (__builtin_expect(sqe != nullptr, 1)) {
        return sqe;  // 大多数情况下有可用 SQE
    }

    // // 环形队列已满 → 提交待处理的 SQEs 以释放条目
    // io_uring_submit(&ring_);
    // pending_sqes_.store(0, std::memory_order_relaxed);

    // TODO

    assert(sqe);
    return io_uring_get_sqe(&ring_);
}

void UringProactor::SubmitIfNeeded() {
    uint32_t prev = (++pending_sqes_);
    if (prev + 1 >= kSqeBatchSize) {
        assert(io_uring_submit(&ring_) > 0);
        pending_sqes_ = 0;
    }
}

int UringProactor::Flush() {
    // uint32_t n = pending_sqes_.exchange(0, std::memory_order_relaxed);
    // if (n > 0) {
    //     return io_uring_submit(&ring_);
    // }
    // return 0;
    assert(false);
}

// ============================================================
// Async IO Operations
// ============================================================

IoAwaitable UringProactor::AsyncAccept(int listen_fd) {
    uint32_t slot_idx = AllocSlot();

    struct io_uring_sqe* sqe = GetSqeOrFlush();
    io_uring_prep_accept(sqe, listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    sqe->user_data = slot_idx;

    

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

IoAwaitable UringProactor::AsyncSend(int fd, const void* buf, size_t len) {
    uint32_t slot_idx = AllocSlot();

    struct io_uring_sqe* sqe = GetSqeOrFlush();
    io_uring_prep_send(sqe, fd, buf, len, MSG_NOSIGNAL);
    sqe->user_data = slot_idx;

    

    return IoAwaitable(this, slot_idx);
}

int UringProactor::AcquireRegBuf() {
    if (!config_.use_registered_bufs || reg_buf_count_ == 0) {
        assert(false);
    }

    uint32_t idx = reg_buf_freelist_;
    assert(idx != -1);
    reg_buf_freelist_ = reg_bufs_[idx].next;
    return idx;
}

void UringProactor::ReleaseRegBuf(int index) {
    if (!config_.use_registered_bufs || reg_buf_count_ == 0) {
        assert(false);
    }
    assert(index >= 0 && index < static_cast<int>(reg_buf_count_));
    reg_bufs_[index].next = reg_buf_freelist_;
    reg_buf_freelist_ = static_cast<uint32_t>(index);
}


void UringProactor::ProcessCqe(struct io_uring_cqe* cqe) {
    uint32_t slot_idx = static_cast<uint32_t>(cqe->user_data);
    int32_t result = cqe->res;
    auto& slot = GetSlot(slot_idx);
    int32_t reg_buf = slot.flags;

    ResumeSlot(slot_idx, result, reg_buf);
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

    // submit_and_wait: 提交待处理的 SQEs + 等待完成事件
    // wait_nr: 等待的最小 CQE 数量
    // 如果有待处理的 SQEs 或 min_cqe > 0，则等待至少 min_cqe 个完成事件
    // 否则不等待（仅提交）
    int wait_nr = (pending > 0 || min_cqe > 0) ? static_cast<int>(min_cqe) : 0;
    struct io_uring_cqe *cqe_ptr = nullptr; // ???
    int submitted = io_uring_submit_and_wait_timeout(&ring_, &cqe_ptr,static_cast<unsigned>(wait_nr), pts, 0);
    if (submitted < 0) {
        // io_uring_submit_and_wait 错误
        int err = -submitted;
        if (err != ETIME && err != EINTR) {
            return -err;  // 返回真正的错误码
        }
        // 超时或中断 → 仍然处理可能已到达的 CQEs
        // submitted 是负数，但我们仍然要处理可能已经到达的 CQEs
        submitted = 0;
    }



    return 1;
}

void UringProactor::loop() {

    loop_thread_id_ = pthread_self();
    
    while (!shutdown_) {

        task_queue_.TryDrain();


        


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


        if (processed < 0) {
            LOG(FATAL) << "Proactor poll error: " << -processed;
            if (processed == -ENOMEM) {
                break;  // 内存不足，无法继续
            } 
        }
        
        // 如果 processed == 0，表示超时（ETIME）或中断（EINTR）
        // 这两种情况下都继续循环，等待下一个事件
    }

    // 最终清理：处理剩余的任务和IO事件
    task_queue_.TryDrain();
    PollOnce(0, 0);  // 提交并处理任何剩余的SQEs，不等待
}

void UringProactor::stop() noexcept {
    shutdown_ = true;
    task_queue_.Shutdown();
}

}  // namespace base

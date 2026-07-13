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
    r.buf_index = slot.flags;
    r.data = proactor()->reg_bufs_[r.buf_index].memory;
    return r;
}


UringProactor::UringProactor(UringConfig cfg, int pool_index) : 
    config_(cfg), 
    pool_index_(pool_index),
    task_queue_(config_.task_queue_size) 
    {
    // 向上取整到最近的2的幂，以便通过位掩码快速取模
    MaxPendingSlots_ = cfg.queue_depth;
    SqeBatchSize_ = cfg.sqe_batch_size;
    size_t slots = 1;
    while (slots < MaxPendingSlots_) slots <<= 1;
    if (slots > MaxPendingSlots_) slots = MaxPendingSlots_;
    slot_mask_ = static_cast<uint32_t>(slots - 1);

    // 预分配 IoCompletionSlot 槽位表
    // 使用 vector 一次性分配，避免热路径上的堆分配
    pending_slots_.resize(slots);

    // 初始化 io_uring 实例
    InitRing();

    // 如果启用注册缓冲区，初始化缓冲区池
    if (config_.use_registered_bufs) {
        InitRegisteredBuffers();
    }
}

UringProactor::~UringProactor() {
    Shutdown();

    if (!reg_bufs_.empty()) {
        io_uring_unregister_buffers(&ring_);
        for (auto& slot : reg_bufs_) {
            delete[] slot.memory;
        }
    }

    io_uring_queue_exit(&ring_);
}

// ============================================================
// Ring Initialization
// ============================================================

void UringProactor::InitRing() {
    struct io_uring_params params;
    std::memset(&params, 0, sizeof(params));

    // 特性标志 — 按影响程度排序
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

    // 初始化 io_uring 实例
    int ret = io_uring_queue_init_params(config_.queue_depth, &ring_, &params);
    if (ret < 0) {
        if (config_.use_sqpoll) {
            // SQPOLL 可能不被支持，回退到中断模式
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
        slot.memory = new char[config_.registered_buf_size];
        slot.index = static_cast<int>(i);
        slot.next = i + 1;
        struct iovec iov;
        iov.iov_base = slot.memory;
        iov.iov_len = static_cast<size_t>(config_.registered_buf_size);
        iovecs.push_back(iov);
    }
    reg_bufs_[reg_buf_count_ - 1].next = -1;

    int ret = io_uring_register_buffers(&ring_, iovecs.data(), iovecs.size());
    if (ret < 0) {
        LOG(WARNING) << "io_uring_register_buffers failed: " << -ret
                     << ", falling back to standard recv";
        config_.use_registered_bufs = false;
        reg_bufs_.clear();
        reg_buf_count_ = 0;
    } else {
        LOG(INFO) << "Registered " << reg_buf_count_ << " fixed buffers ("
                  << config_.registered_buf_size << "B each)";
    }
}

// ============================================================
// Slot Management — lock-free SPSC access
// ============================================================

uint32_t UringProactor::AllocSlot() {
    uint32_t idx = (next_slot_++) & slot_mask_;
    auto& slot = pending_slots_[idx];
    slot.coro = nullptr;
    slot.result = -ECANCELED;
    slot.flags = -1;  // -1 = 未使用注册缓冲区
    
    return idx;
}

void UringProactor::ResumeSlot(uint32_t slot_idx, int32_t result, int32_t extra) {
    auto& slot = pending_slots_[slot_idx];
    slot.result = result;
    slot.flags = extra;
    assert(slot.coro);
    if (slot.coro) {
        slot.coro.resume();
    }
    slot.coro = nullptr;
}

// ============================================================
// SQE 管理 — 批量提交
// ============================================================

// 获取一个 SQE 条目，如果环形队列已满则刷新提交
// 性能关键路径：使用 __builtin_expect 提示编译器优化
struct io_uring_sqe* UringProactor::GetSqeOrFlush() {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (__builtin_expect(sqe != nullptr, 1)) {
        return sqe;  // 大多数情况下有可用 SQE
    }

    // 环形队列已满 → 提交待处理的 SQEs 以释放条目
    io_uring_submit(&ring_);
    pending_sqes_ = 0;
    return io_uring_get_sqe(&ring_);
}

// 检查是否需要提交待处理的 SQEs
// 当待处理的 SQE 数量达到批次大小时，批量提交以减少系统调用
void UringProactor::SubmitIfNeeded() {
    uint32_t prev = pending_sqes_++;
    if (prev + 1 >= SqeBatchSize_) {
        io_uring_submit(&ring_);
        pending_sqes_ = 0;
    }
}

int UringProactor::Flush() {
    uint32_t n = pending_sqes_;
    pending_sqes_ = 0;
    if (n > 0) {
        return io_uring_submit(&ring_);
    }
    return 0;
}

AcceptAwaitable UringProactor::AsyncAccept(int listen_fd) {
    uint32_t slot_idx = AllocSlot();

    struct io_uring_sqe* sqe = GetSqeOrFlush();
    io_uring_prep_accept(sqe, listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    sqe->user_data = slot_idx;

    return AcceptAwaitable(this, slot_idx);
}



RecvAwaitable UringProactor::AsyncRecvFixed(int fd, int buf_idx) {
    uint32_t slot_idx = AllocSlot();
    struct io_uring_sqe* sqe = GetSqeOrFlush();
    io_uring_prep_read_fixed(sqe, fd,
                             reg_bufs_[buf_idx].memory,
                             static_cast<unsigned>(config_.registered_buf_size),
                             0, buf_idx);
    sqe->user_data = slot_idx;
    {
        auto& slot = GetSlot(slot_idx);
        slot.flags = buf_idx;  // also in flags for await_resume
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

// ============================================================
// Registered Buffer Management
// ============================================================

int UringProactor::AcquireRegBuf() {
    int re = next_buf_;
    next_buf_ = reg_bufs_[re].next;
    return re;
}

void UringProactor::ReleaseRegBuf(int index) {
    reg_bufs_[index].next = next_buf_;
    next_buf_ = index;
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
// 轮询 / 事件循环
// ============================================================

// 主轮询函数：提交待处理的 SQEs 并等待完成事件
// 参数：
//   min_cqe: 最小等待的完成事件数（默认1，避免忙等待）
//   timeout_ms: 超时时间（毫秒），0=无限等待
// 返回值：处理的 CQE 数量（>=0），或负的错误码
int UringProactor::PollOnce(unsigned min_cqe, unsigned timeout_ms) {
    // 在等待前刷新待处理的 SQEs
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
    int submitted;
    struct io_uring_cqe* timeout_cqe = nullptr;
    submitted = io_uring_submit_and_wait_timeout(&ring_, &timeout_cqe,
                                                    static_cast<unsigned>(wait_nr), pts, nullptr);
    if (submitted < 0) {
        int err = -submitted;
        if (err != ETIME && err != EINTR) {
            return -err;
        }
        submitted = 0;
    }
    unsigned processed = 0;

    // 先处理 io_uring_submit_and_wait_timeout 通过 cqe_ptr 返回的单个 CQE
    if (timeout_cqe != nullptr) {
        ProcessCqe(timeout_cqe);
        io_uring_cqe_seen(&ring_, timeout_cqe);
        ++processed;
    }

    // 批量处理 CQEs
    struct io_uring_cqe* cqe = nullptr;
    unsigned head;
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
// Proactor 主事件循环
// 运行在独立线程中，处理所有异步IO事件和任务
void UringProactor::Run() {
    // 存储循环线程的线程ID，用于线程安全检查
    loop_thread_id_ = pthread_self();
    
    //int count = 0;

    // 主事件循环：持续运行直到收到关闭信号
    while (!shutdown_) {
        // 优先处理任务队列 — 用户任务具有最高优先级


        // if (count % 10000 == 0 || count < 100)
        // {
        //     LOG(INFO) << "task_queue_.TryDrain" << pool_index_;
        // }
        // count++;


        task_queue_.TryDrain();

        // 轮询至少1个完成事件，使用10ms超时避免忙等待
        // 关键优化：等待至少1个CQE，避免空转循环
        // 之前的实现使用 PollOnce(0, 1) 导致忙等待和高CPU使用率
        int processed = PollOnce(1, 1);

        if (processed < 0) {
            // 真正的错误（不是超时或中断）
            LOG(ERROR) << "Proactor poll error: " << -processed;
            if (processed == -ENOMEM) {
                break;  // 内存不足，无法继续
            }
            // 对于其他错误，继续运行但记录日志
        }
        
        // 如果 processed == 0，表示超时（ETIME）或中断（EINTR）
        // 这两种情况下都继续循环，等待下一个事件
    }

    // 最终清理：处理剩余的任务和IO事件
    task_queue_.TryDrain();
    PollOnce(0, 0);  // 提交并处理任何剩余的SQEs，不等待
}

void UringProactor::Shutdown() noexcept {
    DispatchBrief([this] {
        shutdown_ = true;
    });
    task_queue_.Shutdown();
}

}  // namespace base

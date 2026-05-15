#include "uring_proactor.hpp"
#include <liburing.h>
#include <errno.h>
#include <cstring>
#include <glog/logging.h>

namespace base {

struct UringProactor::io_uring_ring {
    struct io_uring ring;
    io_uring_ring() { memset(&ring, 0, sizeof(ring)); }
    ~io_uring_ring() { io_uring_queue_exit(&ring); }
};



UringProactor::UringProactor(uint32_t index, size_t queue_size, size_t ring_size)
    : thread_index_(index)
    , task_queue_(std::make_unique<base::mpmc_bounded_queue<TaskPtr>>(queue_size))
    , pending_ops_(std::make_unique<base::mpmc_bounded_queue<PendingOp>>(queue_size))
    , running_(false)
    , stop_(false)
{ 
    LOG(INFO) << "Initializing UringProactor index=" << index 
              << ", queue_size=" << queue_size 
              << ", ring_size=" << ring_size;
    
    // 初始化 io_uring
    ring_ = std::make_unique<io_uring_ring>();
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    
    int ret = io_uring_queue_init_params(ring_size, &ring_->ring, &params);
    if (ret < 0) {
        LOG(FATAL) << "io_uring_init failed: " << strerror(-ret);
    }
    ring_fd_ = ring_->ring.ring_fd;
    VLOG(1) << "io_uring initialized, ring_fd=" << ring_fd_;
    
    // 创建 eventfd
    event_fd_ = eventfd(0, EFD_NONBLOCK);
    if (event_fd_ < 0) {
        LOG(FATAL) << "eventfd failed: " << strerror(errno);
    }
    VLOG(1) << "eventfd created, fd=" << event_fd_;
    
    // 注册 eventfd 到 io_uring
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_->ring);
    io_uring_prep_poll_add(sqe, event_fd_, POLLIN);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(WAKEUP_COOKIE));
    io_uring_submit(&ring_->ring);
    
    LOG(INFO) << "UringProactor index=" << index << " initialized successfully";
}

UringProactor::~UringProactor() {
    stop();
    if (event_fd_ >= 0) {
        close(event_fd_);
    }
}

void UringProactor::Wakeup() {
    uint64_t val = 1;
    write(event_fd_, &val, sizeof(val));
}

void UringProactor::DrainTasks() {
    TaskPtr task;
    while (task_queue_->try_dequeue(task)) {
        if (task) {
            (*task)();
        }
    }
}

void UringProactor::SubmitPendingOps() {
    PendingOp op;
    while (pending_ops_->try_dequeue(op)) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_->ring);
        if (!sqe) {
            io_uring_submit(&ring_->ring);
            sqe = io_uring_get_sqe(&ring_->ring);
            if (!sqe) {
                pending_ops_->try_enqueue(std::move(op));
                continue;
            }
        }
        
        switch (op.op_type) {
        case 0: // accept
            io_uring_prep_accept(sqe, op.fd, nullptr, nullptr, 0);
            break;
        case 1: // read
            io_uring_prep_read(sqe, op.fd, op.buf, op.len, op.offset);
            break;
        case 2: // write
            io_uring_prep_write(sqe, op.fd, op.buf, op.len, op.offset);
            break;
        case 3: // close
            io_uring_prep_close(sqe, op.fd);
            break;
        }
        
        // 存储协程句柄和 awaitable 指针
        auto* data = new PendingOp{op};
        io_uring_sqe_set_data(sqe, data);


    }
    
    int ret = io_uring_submit(&ring_->ring); 

    if ( ret != 0 ) {
        // TODO
    }

}

void UringProactor::HandleCqe(struct io_uring_cqe* cqe) {
    void* data = io_uring_cqe_get_data(cqe);
    int res = cqe->res;
    
    

    if (data == reinterpret_cast<void*>(WAKEUP_COOKIE)) {
        // eventfd 唤醒事件，消耗掉它
        uint64_t val;
        read(event_fd_, &val, sizeof(val));
        // 重新注册 poll
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_->ring);
        io_uring_prep_poll_add(sqe, event_fd_, POLLIN);
        io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(WAKEUP_COOKIE));
        io_uring_submit(&ring_->ring);
        return;
    }
    

    auto* op = static_cast<PendingOp*>(data);
    if (!op) return;
    
    // 将结果设置到 Awaitable 对象中
    if (op->awaitable) {
        switch (op->op_type) {
        case 0: { // accept
            auto* awaitable = static_cast<UringSocket::AcceptAwaitable*>(op->awaitable);
            awaitable->fd_ = res;
            break;
        }
        case 1: { // read
            auto* awaitable = static_cast<UringSocket::ReadAwaitable*>(op->awaitable);
            awaitable->result_ = res;
            break;
        }
        case 2: { // write
            auto* awaitable = static_cast<UringSocket::WriteAwaitable*>(op->awaitable);
            awaitable->result_ = res;
            break;
        }
        }
    }
    
    if (op->handle) {
        op->handle.resume();
    }
    
    delete op;
}

void UringProactor::loop() {
    loop_thread_id_ = util::Thread::current_tid();
    running_ = true;
    stop_ = false;
    
    LOG(INFO) << "UringProactor index=" << thread_index_ << " event loop starting";
    
    struct __kernel_timespec timeout;
    timeout.tv_sec = 0;
    timeout.tv_nsec = 1000000; // 1ms
    
    while (!stop_) {
        // 1. 处理任务队列（优先级最高）
        DrainTasks();
        
        // 2. 提交待处理的 I/O 操作
        SubmitPendingOps();
        
        // 3. 等待完成事件
        struct io_uring_cqe* cqe = nullptr;
        int ret = io_uring_wait_cqe_timeout(&ring_->ring, &cqe, &timeout);
        
        if (ret == 0 && cqe) {
            HandleCqe(cqe);
            io_uring_cqe_seen(&ring_->ring, cqe);
        }
        // 超时或错误则继续循环，再次处理任务队列
    }
    
    running_ = false;
    LOG(INFO) << "UringProactor index=" << thread_index_ << " event loop stopped";
}

void UringProactor::stop() {
    stop_ = true;
    Wakeup();
    
    if (util::Thread::current_tid() != loop_thread_id_ && running_) {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

void UringProactor::SubmitAccept(int fd, std::coroutine_handle<> handle, void* awaitable) {
    PendingOp op;
    op.fd = fd;
    op.op_type = 0;
    op.handle = handle;
    op.awaitable = awaitable;
    
    if (!pending_ops_->try_enqueue(std::move(op))) {
        // 队列满，直接恢复协程并返回错误
        if (awaitable) {
            auto* a = static_cast<UringSocket::AcceptAwaitable*>(awaitable);
            a->fd_ = -EBUSY;
        }
        handle.resume();
        return;
    }
    
    Wakeup();
}

void UringProactor::SubmitRead(int fd, void* buf, size_t len, off_t offset,
                                std::coroutine_handle<> handle, void* awaitable) {
    PendingOp op;
    op.fd = fd;
    op.op_type = 1;
    op.buf = buf;
    op.len = len;
    op.offset = offset;
    op.handle = handle;
    op.awaitable = awaitable;
    
    if (!pending_ops_->try_enqueue(std::move(op))) {
        if (awaitable) {
            auto* a = static_cast<UringSocket::ReadAwaitable*>(awaitable);
            a->result_ = -EBUSY;
        }
        handle.resume();
        return;
    }
    
    Wakeup();
}

void UringProactor::SubmitWrite(int fd, const void* buf, size_t len, off_t offset,
                                 std::coroutine_handle<> handle, void* awaitable) {
    PendingOp op;
    op.fd = fd;
    op.op_type = 2;
    op.buf = const_cast<void*>(buf);
    op.len = len;
    op.offset = offset;
    op.handle = handle;
    op.awaitable = awaitable;
    
    if (!pending_ops_->try_enqueue(std::move(op))) {
        if (awaitable) {
            auto* a = static_cast<UringSocket::WriteAwaitable*>(awaitable);
            a->result_ = -EBUSY;
        }
        handle.resume();
        return;
    }
    
    Wakeup();
}



} // namespace base

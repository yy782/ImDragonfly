#pragma once

#include "util/lock_free_queue.hpp"
#include "util/thread.hpp"
#include "util/task_queue.hpp"
#include "socket.hpp"
#include <liburing.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <functional>
#include <memory>
#include <atomic>
#include <thread>
#include <mutex>
#include <system_error>
#include <coroutine>
#include <chrono>
#include <poll.h>
#include <vector>

namespace base {

class UringProactor {
public:
    using CbFunc = util::TaskQueue::CbFunc;
    using IoResult = int;

    explicit UringProactor(uint32_t index, size_t queue_size = 1024, size_t ring_size = 256);
    ~UringProactor();
    
    UringProactor(const UringProactor&) = delete;
    UringProactor& operator=(const UringProactor&) = delete;

    template<typename Func>
    bool DispatchBrief(Func&& f) {
        if (!task_queue_.TryAdd(std::forward<Func>(f))) {
            return false;
        }
        WakeupIfNeeded();
        return true;
    }
    
    template<typename Func>
    cppcoro::AsyncTask AsyncAdd(Func&& f) {
        co_await task_queue_.AsyncAdd(std::forward<Func>(f));
        co_return;
    }

    using CbType = std::function<void(IoResult, uint32_t)>;

    struct SubmitEntry {
        explicit SubmitEntry(io_uring_sqe* sqe) : sqe_(sqe) {}
        
        io_uring_sqe* sqe() { return sqe_; }
        const io_uring_sqe* sqe() const { return sqe_; }
        
        void PrepAccept(int fd) {
            io_uring_prep_accept(sqe_, fd, nullptr, nullptr, 0);
        }
        
        void PrepRead(int fd, void* buf, size_t len, off_t offset = 0) {
            io_uring_prep_read(sqe_, fd, buf, len, offset);
        }
        
        void PrepWrite(int fd, const void* buf, size_t len, off_t offset = 0) {
            io_uring_prep_write(sqe_, fd, buf, len, offset);
        }
        
        void PrepClose(int fd) {
            io_uring_prep_close(sqe_, fd);
        }
        
        void PrepPollAdd(int fd, uint32_t events) {
            io_uring_prep_poll_add(sqe_, fd, events);
        }
        
    private:
        io_uring_sqe* sqe_;
    };

    SubmitEntry GetSubmitEntry(CbType cb, uint32_t submit_tag = 0);

    void SubmitAccept(int fd, std::coroutine_handle<> handle, void* awaitable);
    void SubmitRead(int fd, void* buf, size_t len, off_t offset, 
                    std::coroutine_handle<> handle, void* awaitable);
    void SubmitWrite(int fd, const void* buf, size_t len, off_t offset,
                     std::coroutine_handle<> handle, void* awaitable);
    void SubmitClose(int fd, std::coroutine_handle<> handle, void* awaitable);
    
    void loop();
    void stop();
    
    uint32_t GetPoolIndex() const { return thread_index_; }
    pthread_t GetLoopThreadId() const { return loop_thread_id_; }
    util::TaskQueue& GetTaskQueue() { return task_queue_; }

private:
    struct CompletionEntry {
        CbType cb;
        int64_t index = -1;
    };

    void WakeupIfNeeded();
    void WakeRing();
    void ArmWakeupEvent();
    bool DrainTasks();
    void ProcessCqeBatch(unsigned count, io_uring_cqe** cqes);
    void ReapCompletions(unsigned count, io_uring_cqe** cqes);
    void RegrowCentries();

    uint32_t thread_index_;

    struct io_uring_ring {
        struct io_uring ring;
        io_uring_ring() { memset(&ring, 0, sizeof(ring)); }
        ~io_uring_ring() { io_uring_queue_exit(&ring); }
    };
    std::unique_ptr<io_uring_ring> ring_;

    uint8_t msgring_supported_f_ : 1;
    uint8_t poll_first_ : 1;
    uint8_t taskrun_flag_f_ : 1;
    uint8_t : 5;

    int ring_fd_;
    int event_fd_;
    
    util::TaskQueue task_queue_;
    
    std::atomic<bool> running_;
    std::atomic<bool> stop_;
    std::atomic<uint32_t> tq_seq_{0};
    std::atomic<uint32_t> tq_wakeup_ev_{0};
    std::atomic<uint32_t> tq_wakeup_skipped_ev_{0};
    pthread_t loop_thread_id_;

    static constexpr uint64_t kIgnoreIndex = 0;
    static constexpr uint64_t kWakeIndex = 1;
    static constexpr uint64_t kUserDataCbIndex = 1024;
    static constexpr uint32_t WAIT_SECTION_STATE = 1UL << 31;

    std::vector<CompletionEntry> centries_;
    int32_t next_free_ce_ = -1;
    uint32_t pending_cb_cnt_ = 0;
};

} // namespace base
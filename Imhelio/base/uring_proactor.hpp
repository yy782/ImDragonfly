#pragma once


namespace base{




class ProactorBase{

    

    ProactorBase(const ProactorBase&) = delete;
    void operator=(const ProactorBase&) = delete;
public:

    bool InMyThread() const { return pthread_self() = thread_id_; }



protected:
    ProactorBase();
    pthread_t thread_id_ = 0U;
    int32_t pool_index_ = 0;
    int wake_fd_ = -1;
    
    using Fun = std::function<void()>;

    util::mpmc_bounded_queue<Fun> task_queue_;
    util::EventCount task_queue_avail_;

    std::atomic<uint32_t> tq_seq_;

// ┌─────────────────┬─────────────────────────────────────────┐
// │  bit 31         │           bits 0-30                     │
// │  WAIT_SECTION   │           序列计数器（奇偶标志）          │
// └─────────────────┴─────────────────────────────────────────┘


};


class UringProactor : public ProactorBase{


public:    
    UringProactor();
    ~UringProactor();    
    void Init(unsigned pool_index, size_t ring_size, int wq_fd = -1);

    void loop();
    void stop();

    template<typename Cb>
    void submit_accept_sqe(int fd, Cb&& cb);

    template<typename Cb>
    void submit_read_sqe(int fd, char* buf, ssize_t size, off_t offset, Cb&& cb);

    template<typename Cb>
    void submit_write_sqe(int fd, char* buf, ssize_t size, off_t offset, Cb&& cb);


    void sqe(struct io_uring_sqe** sqe, uint32_t* index = nullptr, struct CompletionEntry* e = nullptr);

    static UringProactor* me() {
        return owner_;
    }

    template <typename Func> 
    bool DispatchBrief(Func&& f);


private:
    template <typename Func> 
    bool EmplaceTaskQueue(Func&& f);

    void WakeupIfNeeded();
    void WakeRing();
    void ProcessCqeBatch(unsigned count, io_uring_cqe** cqes);
    void ReapCompletions(unsigned init_count, io_uring_cqe** cqes);

    void RegrowCentries();
    CompletionEntry& NextEntry();

    using CbType =
        fu2::function_base<true /*owns*/, false /*non-copyable*/, fu2::capacity_fixed<16, 8>,
                            false /* non-throwing*/, false /* strong exceptions guarantees*/,
                            void(struct io_uring_cqe*)>;
    
    struct CompletionEntry {
        CbType cb;
        int64_t index = -1;
    };
    std::vector<CompletionEntry> centries_;
    int32_t next_free_ce_ = -1;

    bool is_stopped_ = true;

    struct io_uring ring_;

    thread_local UringProactor* owner_;

};





template<typename Cb>
void UringProactor::submit_accept_sqe(int fd, Cb&& cb){
    io_uring_sqe* sqe = nullptr;
    uint32_t index = -1;
    struct CompletionEntry e;

    sqe(&sqe, &index, &e);

    e.cb = std::move(cb);


    io_uring_sqe_set_data(sqe, index);
    io_uring_prep_accept(sqe, fd, NULL, NULL);
    io_uring_submit(&ring);
}

template<typename Cb>
void UringProactor::submit_read_sqe(int fd, char* buf, ssize_t size, off_t offset, Cb&& cb){
    io_uring_sqe* sqe = nullptr;
    uint32_t index = -1;
    struct CompletionEntry e;
    
    sqe(&sqe, &index, &e);
    e.cb = std::move(cb);


    io_uring_sqe_set_data(sqe, index);
    io_uring_prep_read(sqe, fd, buf, size, offset);
    io_uring_submit(&ring);
}


template<typename Cb>
void UringProactor::submit_write_sqe(int fd, char* buf, ssize_t size, off_t offset, Cb&& cb){
    io_uring_sqe* sqe = nullptr;
    uint32_t index = -1;
    struct CompletionEntry e;
    
    sqe(&sqe, &index, &e);

    e.cb = std::move(cb);


    io_uring_sqe_set_data(sqe, index);
    io_uring_prep_write(sqe, fd, buf, size, offset);
    io_uring_submit(&ring);

}


template <typename Func> 
bool UringProactor::EmplaceTaskQueue(Func&& f) {
    if (task_queue_.try_enqueue(std::forward<Func>(f))) {
        WakeupIfNeeded();

        return true;
    }
    return false;
}

template <typename Func> 
bool UringProactor::DispatchBrief(Func&& f) {
    if (EmplaceTaskQueue(std::forward<Func>(f)))
        return false;
    while (true) {
        EventCount::Key key = task_queue_avail_.prepareWait();

        if (EmplaceTaskQueue(std::forward<Func>(f))) {
            break;
        }
        task_queue_avail_.wait(key.epoch());
    }

    return true;
}


}
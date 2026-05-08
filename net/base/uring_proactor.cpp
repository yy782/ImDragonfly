
#include "uring_proactor.hpp"


namespace base{
thread_local UringProactor* UringProactor::owner_ = nullptr;
namespace {
    constexpr uint16_t kCqeBatchLen = 128;

    void wait_for_cqe(io_uring* ring, unsigned wait_nr, __kernel_timespec* ts, sigset_t* sig = NULL) {
        struct io_uring_cqe* cqe_ptr = nullptr;
        io_uring_wait_cqes(ring, &cqe_ptr, wait_nr, ts, sig);
    }

    constexpr uint32_t kTaskQueueLen = 256;

    constexpr uint32_t WAIT_SECTION_STATE = 1UL << 31;  // 0x80000000
    const static uint64_t wake_val = 1;
}
 
ProactorBase::ProactorBase()  {
}






UringProactor::UringProactor() : task_queue_(kTaskQueueLen){
}


UringProactor::UringProactor(unsigned pool_index, size_t ring_size) : task_queue_(kTaskQueueLen){
    Init(pool_index, ring_size);
}

UringProactor::~UringProactor() {  
}

void UringProactor::Init(unsigned pool_index, size_t ring_size) {

    assert(ring_size & (ring_size-1));

    pool_index_ = pool_index;
    thread_id_ = pthread_self();

    io_uring_params params;
    memset(&params, 0, sizeof(params));
    // params.flags |= IORING_SETUP_SUBMIT_ALL; // 提交失败也绝不掉队
    // params.flags |=
    //     (IORING_SETUP_DEFER_TASKRUN  // 推迟任务执行，不要立即触发异步任务的处理
    //         | IORING_SETUP_TASKRUN_FLAG  // 提供一个用户态可检查的标志位，告诉应用程序"有推迟的任务等待处理"
    //         | IORING_SETUP_SINGLE_ISSUER // 只有一个线程会向这个 io_uring 实例提交请求
    //     );
    int init_res = io_uring_queue_init_params(ring_size, &ring_, &params);

    if(init_res < 0)
    {
        assert(false); // TODO
    }

    centries_.resize(params.sq_entries);  // .val = -1
    next_free_ce_ = 0;
    for (size_t i = 0; i < centries_.size() - 2; ++i) {
        centries_[i].index = i + 1;
    }
    centries_.back().index = -1;

    UringProactor::me() = this;
}

void UringProactor::loop(){

    struct io_uring_cqe* cqes[kCqeBatchLen];
    is_stopped_ = false;

    struct __kernel_timespec ts;
    ts.tv_sec = 5;
    ts.tv_nsec = 0;    

    uint32_t tq_seq = 0;

    while(!is_stopped_){
        int num_submitted = io_uring_submit_and_get_events(&ring_);       
        bool ring_busy = num_submitted == -EBUSY ? true : false;
        if(num_submitted == -ETIME) continue;

        uint32_t cqe_count = io_uring_peek_batch_cqe(&ring_, cqes, kCqeBatchLen); 
        if (cqe_count) {
            ReapCompletions(cqe_count, cqes);
        }

        tq_seq = tq_seq_.load(std::memory_order_acquire);
        DoingTaskQueue();

        if (task_queue_.empty() &&     
        tq_seq_.compare_exchange_weak(tq_seq, WAIT_SECTION_STATE, std::memory_order_acq_rel,
                                  std::memory_order_relaxed)){
            wait_for_cqe(&ring_, 1, &ts);
            tq_seq_.store(0, std::memory_order_release);
        }
        
    }
}



void UringProactor::stop(){
    DispatchBrief([this] {
        is_stopped_ = true;
    });
}


void UringProactor::ProcessCqeBatch(unsigned count, io_uring_cqe** cqes
                                    ){
    for (unsigned i = 0; i < count; ++i){
        io_uring_cqe cqe = *cqes[i];
        uint32_t idx = cqe.user_data & 0xFFFFFFFF;

        if (idx < centries_.size()){
            CbType cb = std::move(centries_[idx].cb);

            centries_[idx].index = static_cast<uint64_t>(next_free_ce_);
            next_free_ce_ = idx;
            cb(&cqe);
        }
    }

}

void UringProactor::ReapCompletions(unsigned init_count, io_uring_cqe** cqes
                                    ){
    unsigned batch_count = init_count;
    while (batch_count > 0) {
        ProcessCqeBatch(batch_count, cqes);
        io_uring_cq_advance(&ring_, batch_count);
        if (batch_count < kCqeBatchLen)
            break;
        batch_count = io_uring_peek_batch_cqe(&ring_, cqes, kCqeBatchLen);
    }                                 
                                        
}

void UringProactor::RegrowCentries() {
    size_t prev = centries_.size();

    centries_.resize(prev * 2);  // grow by 2.
    next_free_ce_ = prev;
    for (; prev < centries_.size() - 1; ++prev)
        centries_[prev].index = prev + 1;
}

UringProactor::CompletionEntry& UringProactor::NextEntry(){
    if (next_free_ce_ < 0){
        RegrowCentries();
    }
    return centries_[next_free_ce_];
}

void UringProactor::WakeRing(){
    UringProactor* caller = UringProactor::me();

    struct io_uring_sqe* sqe = nullptr;
    caller->Sqe(&sqe); // sqe 的回调是空，可能有问题

    io_uring_prep_msg_ring(sqe, ring_.ring_fd, 0, 0, 0);
}

void UringProactor::Sqe(struct io_uring_sqe** sqe, uint32_t* index, struct CompletionEntry** e){
    *sqe = io_uring_get_sqe(&ring_);
    if (*sqe == nullptr) {
        do{
            io_uring_submit(&ring_);
            *sqe = io_uring_get_sqe(&ring_);
        }while(*sqe == nullptr);        
    }
    memset(*sqe, 0, sizeof(io_uring_sqe));

    
    auto& ce = NextEntry();
    uint32_t dx = next_free_ce_;
    next_free_ce_ = ce.index;

    if (index != nullptr) *index = dx;
    if (e != nullptr) *e = &ce;

}

void UringProactor::WakeupIfNeeded() {
    auto current = tq_seq_.fetch_add(1, std::memory_order_acq_rel);
    if (current == WAIT_SECTION_STATE) {
        WakeRing();
    } 
}


void UringProactor::DoingTaskQueue(){
    Fun task;
    while(task_queue_.try_dequeue(task)){
        task();
        
    }
    task_queue_avail_.notifyAll();
}




}









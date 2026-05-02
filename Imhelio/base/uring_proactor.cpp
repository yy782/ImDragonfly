
#include "uring_proactor.hpp"


namespace base{

 
SubmitEntry UringProactor::GetSubmitEntry(CbType cb, uint32_t submit_tag) {
    io_uring_sqe* res = io_uring_get_sqe(&ring_);
    if (res == NULL) {
        int submitted = io_uring_submit(&ring_);
        if (submitted > 0) {
            res = io_uring_get_sqe(&ring_);
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
        DCHECK(!e.cb);  // cb is undefined.
        DVLOG(3) << "GetSubmitEntry: index: " << next_free_ce_;

        next_free_ce_ = e.index;
        e.cb = std::move(cb);
        ++pending_cb_cnt_;
    } else {
        res->user_data = kIgnoreIndex | (uint64_t(submit_tag) << 32);
    }

    return SubmitEntry{res};
}    


}
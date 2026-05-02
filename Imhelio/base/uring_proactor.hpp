#pragma once


#include "submit_entry.hpp"


namespace base{


class ProactorBase{

};


class UringProactor : public ProactorBase{
    UringProactor(const UringProactor&) = delete;
    void operator=(const UringProactor&) = delete;

    using IoResult = int;
    using CbType =
    fu2::function_base<true /*owns*/, false /*non-copyable*/, fu2::capacity_fixed<16, 8>,
                        false /* non-throwing*/, false /* strong exceptions guarantees*/,
                        void(detail::FiberInterface*, IoResult, uint32_t, uint32_t)>;
public:    
    UringProactor();
    ~UringProactor();    
    void Init(unsigned pool_index, size_t ring_size, int wq_fd = -1);

    SubmitEntry GetSubmitEntry(CbType cb, uint32_t submit_tag = 0);

    uint32_t GetSubmitRingAvailability() const {
        return io_uring_sq_space_left(&ring_);
    }


};




}
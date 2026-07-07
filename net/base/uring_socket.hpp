#pragma once

#include <cstdint>
#include <memory>

#include "uring_proactor.hpp"
#include "fd_wrapper.hpp"

namespace base {

// A socket wrapper that integrates with UringProactor
class UringSocket {
public:
    UringSocket(UringProactorPtr proactor, int fd)
        : proactor_(std::move(proactor)), fd_(fd), buf_idx_(proactor_->AcquireRegBuf()) {
            assert(buf_idx_ >= 0 && buf_idx_ < static_cast<int>(proactor_->reg_buf_count()));
        }
    
    ~UringSocket() {
        assert(std::uncaught_exceptions() == 0);
        if (fd_ >= 0) {
            Close();
        }
        proactor_->ReleaseRegBuf(buf_idx_);
    }
    
    // Socket operations using the proactor
    IoAwaitable AsyncAccept() {
        return proactor_->AsyncAccept(fd_);
    }
    
    RecvAwaitable AsyncRead() {
        return proactor_->AsyncRecvFixed(fd_, buf_idx_);
    }
    
    IoAwaitable AsyncWrite(const void* buf, size_t len, int flags = 0) {
        return proactor_->AsyncSend(fd_, buf, len);
    }
    
    
    // Utility methods
    int fd() const { return fd_; }
    UringProactorPtr Proactor() { return proactor_; }
    
    void Close() {
        if (fd_ >= 0) {
            close(fd_);
            fd_ = -1;
        }
    }
    
private:
    UringProactorPtr proactor_;
    int fd_ = -1;
    int buf_idx_ = -1;  // Index of the registered buffer used for zero-copy receive
};

} // namespace base
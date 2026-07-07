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
        : proactor_(std::move(proactor)), fd_(fd) {}
    
    // Move constructor
    UringSocket(UringSocket&& other) noexcept
        : proactor_(std::move(other.proactor_)), fd_(other.fd_) {
        other.fd_ = -1;
    }
    
    // Move assignment
    UringSocket& operator=(UringSocket&& other) noexcept {
        if (this != &other) {
            Close();
            proactor_ = std::move(other.proactor_);
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }
    
    // Non-copyable
    UringSocket(const UringSocket&) = delete;
    UringSocket& operator=(const UringSocket&) = delete;
    
    ~UringSocket() {
        if (fd_ >= 0) {
            Close();
        }
    }
    
    // Socket operations using the proactor
    AcceptAwaitable AsyncAccept() {
        return proactor_->AsyncAccept(fd_);
    }
    
    IoAwaitable AsyncRead(void* buf, size_t len, int flags = 0) {
        return proactor_->AsyncRecvRaw(fd_, buf, len);
    }
    
    IoAwaitable AsyncWrite(const void* buf, size_t len, int flags = 0) {
        return proactor_->AsyncSend(fd_, buf, len);
    }
    
    IoAwaitable AsyncClose() {
        return proactor_->AsyncClose(fd_);
    }
    
    // Utility methods
    int fd() const { return fd_; }
    UringProactorPtr Proactor() { return proactor_; }
    
    void Close() {
        if (fd_ >= 0) {
            auto close_awaitable = AsyncClose();
            // We can't wait for the close to complete here, but we can
            // at least initiate it and mark the fd as closed
            fd_ = -1;
        }
    }
    
private:
    UringProactorPtr proactor_;
    int fd_ = -1;
};

} // namespace base
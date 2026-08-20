

#pragma once

#include <glog/logging.h>
#include <unistd.h>

#include "uring_proactor.hpp"

namespace base {

class UringSocket {
 public:
  UringSocket() = default;

  // proactor 生命周期由 UringProactorPool 保证（覆盖整个程序期间），
  // 这里持有裸指针，不增加引用计数开销。
  UringSocket(UringProactor* p, int fd) noexcept : proactor_(p), fd_(fd) {}
  void RegisterRecvBuf() { recv_buf_idx_ = proactor_->AcquireRegBuf(); }

  ~UringSocket() {
    if (recv_buf_idx_ >= 0) {
      proactor_->ReleaseRegBuf(recv_buf_idx_);
    }
    Close();
  }

  int fd() const noexcept { return fd_; }
  UringProactor* Proactor() const noexcept { return proactor_; }

  RecvAwaitable AsyncRead(size_t offset = 0) {
    return proactor_->AsyncRecvFixed(fd_, recv_buf_idx_, offset);
  }

  IoAwaitable AsyncWrite(const void* data, size_t len, int /*flags*/ = 0) {
    return proactor_->AsyncSend(fd_, data, len);
  }
  IoAwaitable AsyncWriteV(const struct msghdr* msg) {
    return proactor_->AsyncSendV(fd_, msg);
  }
  AcceptAwaitable AsyncAccept() { return proactor_->AsyncAccept(fd_); }

  void Close() {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

 private:
  UringProactor* proactor_ = nullptr;
  int fd_ = -1;
  int recv_buf_idx_ = -1;
};

}  // namespace base

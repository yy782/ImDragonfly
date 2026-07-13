// 基于 UringProactor 的异步 Socket 封装
// 将 fd 与 Proactor 绑定，提供高层异步 IO 接口（co_await 友好）
//
// 使用示例:
//   base::UringSocket sock(proactor, fd);
//   auto [bytes, data, buf_idx] = co_await sock.AsyncRead();
//   int written = co_await sock.AsyncWrite(msg.data(), msg.size());
//   int client_fd = co_await listen_sock.AsyncAccept();

#pragma once

#include "uring_proactor.hpp"

#include <unistd.h>

namespace base {

class UringSocket {
public:
    UringSocket() = default;


    UringSocket(std::shared_ptr<UringProactor> p, int fd) noexcept
        : proactor_(p), fd_(fd) {}
    void RegisterRecvBuf() {
        recv_buf_idx_ = proactor_->AcquireRegBuf();
    }

    ~UringSocket() {
        if (recv_buf_idx_ >= 0) {
            proactor_->ReleaseRegBuf(recv_buf_idx_);
        }
         Close(); 
        }

    // ---- 基础属性 ----
    int fd() const noexcept { return fd_; }
    UringProactorPtr Proactor() const noexcept { return proactor_; }

    // ---- 异步 IO 操作（均返回 awaitable，支持 co_await）----

    // 异步读取（零拷贝，使用注册缓冲区）
    // co_await 返回 RecvResult { .bytes, .data, .buf_index }
    RecvAwaitable AsyncRead() {
        return proactor_->AsyncRecvFixed(fd_, recv_buf_idx_);
    }

    // 异步写入
    // co_await 返回实际写入的字节数（或负的错误码）
    IoAwaitable AsyncWrite(const void* data, size_t len, int /*flags*/ = 0) {
        return proactor_->AsyncSend(fd_, data, len);
    }

    // 异步接受新连接（仅 listen socket）
    // co_await 返回新的客户端 fd（或负的错误码）
    AcceptAwaitable AsyncAccept() {
        return proactor_->AsyncAccept(fd_);
    }

    // ---- 关闭 ----
    void Close() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

private:
    std::shared_ptr<UringProactor> proactor_ = nullptr;
    int fd_ = -1;
    int recv_buf_idx_ = -1;
};

}  // namespace base

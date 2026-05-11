#pragma once // socket.hpp  

#include <functional>
#include <variant>
#include <system_error>
#include "util/expected.hpp"
#include <sys/socket.h>
#include <glog/logging.h>
#include <coroutine>
#include <memory>

namespace base{

class UringProactor;
using UringProactorPtr = std::shared_ptr<UringProactor>;

class UringSocket {
public:
    UringSocket(UringProactorPtr proactor, int fd = -1);
    ~UringSocket();
    
    int fd() const { return fd_; }
    UringProactorPtr Proactor() const { return proactor_; }
    void Close() {
        ::close(fd_);
        fd_ = -1;
    }
    struct AcceptAwaitable;
    struct ReadAwaitable;
    struct WriteAwaitable;
    
    AcceptAwaitable AsyncAccept();
    ReadAwaitable AsyncRead(char* buf, size_t len, off_t offset = 0);
    WriteAwaitable AsyncWrite(const char* buf, size_t len, off_t offset = 0);
    
private:
    UringProactorPtr proactor_;
    int fd_;
};

// ============== Awaitable 定义 ==============

struct UringSocket::AcceptAwaitable {
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept;
    int await_resume() noexcept { return fd_; }
    
    UringSocket* socket_;
    int fd_;
    struct sockaddr_storage addr_;
    socklen_t addrlen_;
};

struct UringSocket::ReadAwaitable {
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept;
    ssize_t await_resume() noexcept { return result_; }
    
    UringSocket* socket_;
    char* buf_;
    size_t len_;
    off_t offset_;
    ssize_t result_;
};

struct UringSocket::WriteAwaitable {
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept;
    ssize_t await_resume() noexcept { return result_; }
    
    UringSocket* socket_;
    const char* buf_;
    size_t len_;
    off_t offset_;
    ssize_t result_;
};






}
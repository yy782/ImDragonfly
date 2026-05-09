#pragma once

#include <functional>
#include <variant>
#include <system_error>
#include "util/expected.hpp"
#include <sys/socket.h>
#include "base/uring_proactor_pool.hpp"
namespace base{
inline bool posix_err_wrap(ssize_t res, std::error_code* ec) {
  if (res == -1) {
    *ec = std::error_code(errno, std::system_category());
    return true;
  } else if (res < 0) {
  }
  return false;
}
    template <typename T, typename E = ::std::error_code> 
    using Result = util::expected<T, E>;

class SocketBase{
    SocketBase(const SocketBase&) = delete;
    void operator=(const SocketBase&) = delete;
    SocketBase(SocketBase&& other) = delete;
    SocketBase& operator=(SocketBase&& other) = delete;
public:

    int fd() const { return fd_;}
  

protected:
    SocketBase(int fd) : fd_(fd) {}
    int fd_;
};


class UringProactor;

class UringSocket : public SocketBase{
public:
    UringSocket(UringProactorPtr proactor,int fd) :SocketBase(fd), proactor_(proactor) {}
    ~UringSocket() { ::close(fd_); }
    struct AcceptAwaitable{
        bool await_ready() const noexcept
        {
            return false;
        }            
        void await_suspend(
                std::coroutine_handle<> awaitingCoroutine) noexcept
        {
            auto proactor = socket_->Proactor();
            proactor->submit_accept_sqe(socket_->fd(), [awaitingCoroutine, this](struct io_uring_cqe* cqe) mutable {
                fd = cqe->res;
                awaitingCoroutine.resume();
            } /*data*/);

        }
        Result<int> await_resume(){
            std::error_code ec;
            if(posix_err_wrap(fd, &ec)){
                return {util::unexpected(ec)};
            }
            return {fd};
        }
        UringSocket* socket_;
        int fd;
    };
    struct ReadAwaitable{
        bool await_ready() const noexcept
        {
            return false;
        } 
        void await_suspend(
                std::coroutine_handle<> awaitingCoroutine) noexcept
        {
            auto proactor = socket_->Proactor();
            proactor->submit_read_sqe(socket_->fd(), buf, size, offset , [awaitingCoroutine, this](struct io_uring_cqe* cqe) mutable {
                n = cqe->res;
                awaitingCoroutine.resume();
            } /*data*/);

        }
        int await_resume(){
            return n;
        }
        UringSocket* socket_;
        char* buf;
        ssize_t size;
        off_t offset;
        ssize_t n;
    };
    struct WriteAwaitable{
        bool await_ready() const noexcept
        {
            return false;
        } 
        void await_suspend(
                std::coroutine_handle<> awaitingCoroutine) noexcept
        {
            auto proactor = socket_->Proactor();
            proactor->submit_write_sqe(socket_->fd(), buf, size, offset , [awaitingCoroutine, this](struct io_uring_cqe* cqe) mutable {
                n = cqe->res;
                awaitingCoroutine.resume();
            } /*data*/);

        }
        int await_resume(){
            return n;
        }
        UringSocket* socket_;
        char* buf;
        ssize_t size;
        off_t offset;
        ssize_t n;
    };
    Result<int> Create(unsigned short protocol_family = AF_INET);

    [[nodiscard]] auto AsyncAccept() -> AcceptAwaitable;

    [[nodiscard]] Result<void> Close();

    [[nodiscard]] auto AsyncRead(char* buf, ssize_t size, off_t offset) -> ReadAwaitable;

    [[nodiscard]] auto AsyncWrite(char* buf, ssize_t size, off_t offset) -> WriteAwaitable;

    [[nodiscard]] UringProactorPtr Proactor() { return proactor_; }
private:
    UringProactorPtr proactor_;
};






}
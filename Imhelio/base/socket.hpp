#pragma once

// for tcp::endpoint. Consider introducing our own.
#include <boost/asio/ip/tcp.hpp>
#include <functional>
#include <variant>

#include "base/io.h"

namespace base{

    template <typename T, typename E = ::std::error_code> 
    using Result = utils::expected<T, E>;

class SocketBase{
    SocketBase(const SocketBase&) = delete;
    void operator=(const SocketBase&) = delete;
    SocketBase(SocketBase&& other) = delete;
    SocketBase& operator=(SocketBase&& other) = delete;
    int fd() const { return fd_;}
  

protected:
    int fd_;
};


class UringProactor;

class UringSocket : public SocketBase{
public:
    UringSocket(UringProactor* proactor,int fd) :fd_(fd), proactor_(proactor) {}
    ~UringProactor();


    Result<int> Create(unsigned short protocol_family = AF_INET);

    [[nodiscard]] auto Accept();

    [[nodiscard]] Result<void> Close();

    [[nodiscard]] auto AsyncRead(char* buf, ssize_t size, off_t offset);

    [[nodiscard]] auto AsyncWrite(char* buf, ssize_t size, off_t offset);


private:
    UringProactor* proactor_;
};

inline bool posix_err_wrap(ssize_t res, std::error_code* ec) {
    if (res == -1) {
        *ec = std::error_code(errno, std::system_category());
        return true;
    } else if (res < 0) {
        LOG(WARNING) << "Bad posix error " << res;
    }
    return false;
}





}
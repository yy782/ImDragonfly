#pragma once

// for tcp::endpoint. Consider introducing our own.
#include <boost/asio/ip/tcp.hpp>
#include <functional>
#include <variant>

#include "base/io.h"

namespace base{



class SocketBase: public io::Sink,
                        public io::AsyncSink,
                        public io::Source,
                        public io::AsyncSource {
    FiberSocketBase(const FiberSocketBase&) = delete;
    void operator=(const FiberSocketBase&) = delete;
    FiberSocketBase(FiberSocketBase&& other) = delete;
    FiberSocketBase& operator=(FiberSocketBase&& other) = delete;
    int fd() const { return fd_;}

    using endpoint_type = ::boost::asio::ip::tcp::endpoint;
    using error_code = std::error_code;
    using AcceptResult = io::Result<SocketBase*>;

    template <typename T> 
    using Result = io::Result<T>;    




protected:
    int fd_;
};


class UringProactor;

class UringSocket : public SocketBase{

    UringSocket(UringProactor*);
    ~UringProactor();


    error_code Create(unsigned short protocol_family = 2);

    [[nodiscard]] AcceptResult Accept();

    [[nodiscard]] error_code Connect(const endpoint_type& ep,
                                            std::function<void(int)> on_pre_connect);
    [[nodiscard]] error_code Close();

    io::Result<size_t> WriteSome(const iovec* v, uint32_t len);
    void AsyncWriteSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb);
    void AsyncReadSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb);

    Result<size_t> RecvMsg(const msghdr& msg, int flags);
    Result<size_t> Recv(const io::MutableBytes& mb, int flags = 0);


private:
    UringProactor* proactor_;
};


}
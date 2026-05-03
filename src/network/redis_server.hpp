// network/redis_server.h
#pragma once

using namespace boost;

namespace dfly{


class RedisSession : public std::enable_shared_from_this<RedisSession> {
public:
    RedisSession(int fd, UringProactor* proactor)
        : socket_(proactor, fd) {
    }
    
    auto DoRead(){

        while (true) {
            auto n = co_await socket_.AsyncRead(RecvBuf_.BeginWrite(), RecvBuf_.writable_size(), -1);
            // 处理逻辑

            auto n = co_await socket_.AsyncWrite(SendBuf_.peek(), SendBuf_.readable_size(), -1);

            // 处理逻辑            
        }
        // 关闭连接了
    }   
    
    
private:
    UringSocket socket_; 
    base::IoBuf RecvBuf_;
    base::IoBuf SendBuf_;

    Namespace* ns_ = &GetDefaultNamespace(); 
    DbIndex index_ = 0;
};


// 这里IO用的是io_uring, 回调也只是恢复线程，还需要IO线程吗，没有IO需求了?
class RedisServer {
public:
    RedisServer(int listenFd, uint32_t size)
        : pool_(size),
          socket_(pool_.NextProactor(), listenfd)
    {

    }
    
    cppcoro::task<void> Start() {
        


        isRuning = true;

        pool_.loop();

        while (isRuning) {
            auto r = co_await socket_.Accept();

            if (r.has_value()){
                auto session = RedisSession(r, pool_.NextProactor());

                // 这里应该交给线程池处理session::DoRead()吗?
            }
        }
    }
    
    void Stop() {
        isRuning = false;
        pool_.stop();
    }
    
private:
    UringProactorPool pool_;
    UringSocket ListenSocket_;

    bool isRuning = false;
};



}
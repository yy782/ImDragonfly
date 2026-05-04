// network/redis_server.h
#pragma once

using namespace boost;

namespace dfly{

class RedisSession : public std::enable_shared_from_this<RedisSession> {
public:
    RedisSession(int fd, UringProactor* proactor)
        : socket_(proactor, fd) {
    }
    
    cppcoro::task<void> DoRead(){

        while (true) {
            auto n = co_await socket_.AsyncRead(RecvBuf_.BeginWrite(), RecvBuf_.writable_size(), -1);
            // 处理逻辑

            auto n = co_await socket_.AsyncWrite(SendBuf_.peek(), SendBuf_.readable_size(), -1);

            // 处理逻辑            
        }
        // 关闭连接了
        co_return;
    }   
    
    
private:
    UringSocket socket_; 
    base::IoBuf RecvBuf_;
    base::IoBuf SendBuf_;

    Namespace* ns_ = &GetDefaultNamespace(); 
    DbIndex index_ = 0;
};

class RedisServer {
public:
    RedisServer(int listenFd, uint32_t size)
        : pool_(size),
          socket_(&main_proactor_, listenfd)
    {

    }
    
    void Start() {
        isRuning = true;
        pool_.AsyncLoop();

        main_proactor_.DispatchBrief([this]{
            while(isRunning){
                auto r = co_await socket_.AsyncAccept();

                if (r.has_value()){
                    auto session = std::make_shared<RedisSession>(r, pool_.NextProactor());
                    
                    auto& p = NextProactor();
                    p->DispatchBrief([session](){
                        session->DoRead();
                    });                    
                }                 
            }
        });
        main_proactor_.loop();
    }
    
    void Stop() {
        isRuning = false;
        pool_.stop();
    }
    
private:

    auto& NextProactor() const {
        return pool_[NextProIndex_%pool_.size()];
    }

    ssize_t NextProIndex_ = 0;


    UringProactor main_proactor_;
    UringProactorPool pool_;
    UringSocket ListenSocket_;
    bool isRuning = false;
};



}
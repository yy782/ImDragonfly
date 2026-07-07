// network/redis_server.h
#pragma once // redis_server.hpp  
#include <glog/logging.h>
#include "sharding/engine_shard_set.hpp"
#include "redis/facade/resp_buf.hpp"
#include "redis/facade/reply_builder.hpp"

#include "command_layer/command_registry.hpp"
#include "command_layer/command_families.hpp"

#include "command_layer/multi_family.hpp"
#include "sharding/namespaces.hpp"
#include "base/uring_proactor_pool.hpp"
#include "base/uring_socket.hpp"
#include "transaction_layer/transaction.hpp"
#include "base/fd_wrapper.hpp"
#include "util/thread.hpp"
#include <cstring>
#include <exception>
namespace dfly{
using base::UringProactorPtr;
inline CommandRegistry* CIs = nullptr;
class RedisServer;
inline RedisServer* ser = nullptr;

class RedisSession : public std::enable_shared_from_this<RedisSession> {
public:
    RedisSession(int fd, UringProactorPtr proactor)
        : socket_(proactor, fd)

         {
            
    }

    ~RedisSession() {
       assert(std::uncaught_exceptions() == 0);
    }
    
    UringProactorPtr GetProactor() { return socket_.Proactor(); }
    Transaction* GetTransaction() { return transaction_.get(); }

    cppcoro::AsyncTask DoRead(){
        
        pId_ = socket_.Proactor()->GetLoopThreadId();
        context_ = ConnectionContext(shared_from_this(), &namespaces->GetDefaultNamespace(), 0);
        int fd = socket_.fd();
        // LOG(INFO) << "New session created for fd: " << fd;
        while (true) {
            auto r = co_await socket_.AsyncRead(RecvBuf_.BeginWrite(), RecvBuf_.writable_size(), -1);
            assert(util::Thread::current_tid() == pId_);
            if (r > 0) {            
                RecvBuf_.hasWritten(r);
                auto& com = RecvBuf_.ParseRESP();
                if (com.empty()) continue;
                args_ = ::cmn::CmdArgList(com);
                
                // VLOG(1) << "Received command: " << args_[0] << " with " << args_.size() << " arguments";
                
                auto ci = CIs->Find(args_[0]);
                if (!ci) { 
                    LOG(WARNING) << "Unknown command: " << args_[0] << " from fd: " << fd;
                    SendERROR("unknown command:" + std::string(args_[0]));
                    continue;
                }

                std::string cmd_name(args_[0]);
                
                is_multi_command = (cmd_name == "MULTI" || cmd_name == "EXEC" || 
                                        cmd_name == "DISCARD" || cmd_name == "WATCH" || cmd_name == "UNWATCH");

                if (!transaction_ || transaction_->GetState() == Transaction::State::IDLE) {
                    transaction_.reset(new Transaction(ci));
                    transaction_->InitByArgs(&context_, args_);
                }else {
                    if (transaction_->GetState() == Transaction::State::MULTI && !is_multi_command) {
                        transaction_->QueueCommand(ci, args_);
                        SendStatus("QUEUED");
                        continue;
                    }               
                }

                ci->Invoke(&transaction_->GetCommandContext(), args_); 
            }
            else if (r == 0) { 
                LOG(INFO) << "Connection closed by client, fd: " << fd;
                break;
            }
            else {
                LOG(ERROR) << "Read error on fd: " << fd << ", error" << r;
                break;
            }            
        }
        socket_.Close();
        context_.owner().reset();
        co_return;  
    }    

    void SendERROR(std::string err = "NULL") {
        SendImp(BuildError(err));
    }
    void Send(int64_t n) {
        SendImp(BuildInteger(n));
    }

    void Send(const std::string& s){
        SendImp(BuildBulkString(s));
    }
    void Send(const std::string_view& s){
        Send(std::string(s));
    }
    void SendVec(const std::vector<std::string>& v) {
        SendImp(BuildArray(v));
    }
    void SendStatus(const std::string& s){
        SendImp(BuildSimpleString(s));
    }
    void SendStatus(const std::string_view& s){
        SendStatus(std::string(s)); 
    }
    void SendStatus(const char* s){
        SendStatus(std::string(s)); 
    }
    void SendInteger(int64_t n) {
        SendImp(BuildInteger(n));
    }
    void SendNULL() {
        Send(std::string());
    }
    base::UringSocket& socket() { return socket_; }
private:
    void SendImp(std::string&& s) {
        auto p = socket_.Proactor(); 
        // if (transaction_->GetState() == Transaction::State::EXEC && args_[0] == "EXEC") {// 这里如果DoRead意外恢复了，可能不同线程操作transaction_
        //     if (!transaction_->collectMultiRes(s)) co_return; // 可能多线程操作同一个容器
        //     s = BuildMultiArray(transaction_->SwapOrClearMultiRes());
        //     transaction_->FinishOrDiscardMulti();
        // }
        p->DispatchBrief([this, s = std::move(s)](){
            LOG(INFO) << "CI:" << args_[0] << " Send: " << s;
            SendBuf_.append(s);
            DoWrite();     
        });
        return;         
    }
    cppcoro::AsyncTask DoWrite() {
        assert(util::Thread::current_tid() == pId_);
        while (SendBuf_.readable_size()) {
            auto wr = co_await socket_.AsyncWrite(SendBuf_.BeginRead(), SendBuf_.readable_size(), -1);
            if (wr>0) {
                SendBuf_.retrieve(wr);
            }else {
                // TODO
            }
        }
        co_return;
    }


    friend class ConnectionContext;
    
    base::UringSocket socket_; 
    RESP_Buf RecvBuf_;
    RESP_Buf SendBuf_;
    ::cmn::CmdArgList args_;
    ConnectionContext context_;
    std::unique_ptr<Transaction> transaction_;   
    bool is_multi_command = false;

    pthread_t pId_;
};

class RedisServer {
public:
    RedisServer(int listenFd, uint32_t size)
        :   main_proactor_(new base::UringProactor()),
            pool_(size, CreateOptimizedRedisConfig()),
            ListenSocket_(main_proactor_, listenFd)
    {
        CIs = new CommandRegistry();
        RegisterStringFamily(CIs);
        RegisterGeneric(CIs);
        RegisterMulti(CIs);
        RegisterListFamily(CIs);
        RegisterHashFamily(CIs);
        RegisterSetFamily(CIs);
        RegisterZSetFamily(CIs);
        ser = this;
    }
    static base::UringConfig CreateOptimizedRedisConfig() {
        base::UringConfig config;
        
        // 针对Redis服务器的高性能优化配置
        config.queue_depth = 4096;            // 大队列深度：支持更多并发操作
        config.use_defer_taskrun = true;      // 必须启用：确保协程正确性
        config.use_single_issuer = true;      // 单发布者：更好性能
        config.use_sqpoll = false;            // 禁用SQPOLL：Redis需要低延迟响应，而非纯吞吐量
        config.use_registered_bufs = true;    // 启用注册缓冲区：零拷贝接收
        config.registered_buf_count = 1024;   // 更多缓冲区：支持高并发连接
        config.registered_buf_size = 16384;   // 16KB缓冲区：适合Redis命令大小（通常小于16KB）
        config.cqe_batch_size = 128;          // 大批次处理：提高吞吐量
        
        return config;
    }

    ~RedisServer() {
        delete CIs;
        CIs = nullptr;

        if (shard_set) {
            delete shard_set;
            shard_set = nullptr;
        }
    }
    
    void Start() {
        LOG(INFO) << "Starting RedisServer...";
        isRuning = true;
        pool_.AsyncLoop();
        sleep(1);
        shard_set = new EngineShardSet(&pool_);
        shard_set->Init(pool_.size());
        main_proactor_->DispatchBrief([this]{
            listen();
        });
        main_proactor_->loop();
    }
    
    void Stop() {
        main_proactor_->DispatchBrief([this](){
            isRuning = false;
            main_proactor_->stop();
            shard_set->Shutdown();
            pool_.stop();
            
        });
    }
    
private:
    cppcoro::AsyncTask listen() {
        
        while(isRuning){
            auto fd = co_await ListenSocket_.AsyncAccept();
            if (fd > 0){
                std::string addr = base::AddressToString(base::Address(fd));
                LOG(INFO) << "Accepted new connection, addr: " << addr;

                auto p = NextProactor();
                VLOG(1) << "Assigning connection to proactor: " << p->GetPoolIndex();

                auto session = std::make_shared<RedisSession>(fd, p);
                
                bool success = p->DispatchBrief([session](){
                    session->DoRead();
                });
                if (!success) {
                    LOG(ERROR) << "Failed to dispatch session to proactor: " << p->GetPoolIndex();
                    session->socket().Close();
                }                    
            } else if (fd < 0) {
                LOG(WARNING) << "Failed to accept connection, error: " << strerror(errno);
            }                    
        }

        co_return;
    }
    auto NextProactor() -> UringProactorPtr {
        NextProIndex_ = (NextProIndex_ + 1) % pool_.size();
        return pool_[NextProIndex_];
    }

    ssize_t NextProIndex_ = 0;


    base::UringProactorPtr main_proactor_;
    base::UringProactorPool pool_;
    base::UringSocket ListenSocket_;
    bool isRuning = false;

};



}

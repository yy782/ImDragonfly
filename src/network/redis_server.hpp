#pragma once
#include <glog/logging.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include "sharding/engine_shard_set.hpp"
#include "redis/facade/reply_builder.hpp"
#include "redis/facade/ParseRESP.hpp"
#include "command_layer/command_registry.hpp"
#include "command_layer/command_families.hpp"
#include <netinet/tcp.h>
#include "command_layer/multi_family.hpp"
#include "sharding/namespaces.hpp"
#include "transaction_layer/transaction.hpp"
#include "net/uring_socket.hpp"
namespace dfly{

inline CommandRegistry* CIs = nullptr;
class RedisServer;
inline RedisServer* ser = nullptr;

class RedisSession : public std::enable_shared_from_this<RedisSession> {
public:
    RedisSession(int fd, base::UringProactorPtr p)
        : socket_(p, fd)
    {
    }

    ~RedisSession() {
        assert(std::uncaught_exceptions() == 0);
    }
    
    base::UringProactorPtr GetProactor() { return socket_.Proactor(); }
    Transaction* GetTransaction() { return &transaction_; }

    cppcoro::AsyncTask DoRead(){        
        socket_.RegisterRecvBuf();
        pId_ = socket_.Proactor()->GetLoopThreadId();
        context_ = ConnectionContext(shared_from_this(), &namespaces->GetDefaultNamespace(), 0);
        int fd = socket_.fd();
        // LOG(INFO) << "New session created for fd: " << fd;
        while (true) {
            res_ = co_await socket_.AsyncRead();
            assert(util::Thread::current_tid() == pId_);
            if (res_.bytes > 0) {                            
                auto& com = p.Parse(res_.data, res_.bytes);
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

                if (transaction_.GetState() == Transaction::State::IDLE) {
                    // C++20原地构建
                    std::destroy_at(&transaction_);
                    std::construct_at(&transaction_, ci);
                    transaction_.InitByArgs(&context_, args_);
                }else {
                    // if (transaction_.GetState() == Transaction::State::MULTI && !is_multi_command) {
                    //     transaction_.QueueCommand(ci, args_);
                    //     SendStatus("QUEUED");
                    //     continue;
                    // }               
                }

                ci->Invoke(&transaction_.GetCommandContext(), args_); 
            }
            else if (res_.bytes == 0) { 
                LOG(INFO) << "Connection closed by client, fd: " << fd;
                break;
            }
            else {
                LOG(ERROR) << "Read error on fd: " << fd << ", error" << res_.bytes;
                break;
            }            
        }
        auto t = &transaction_;

        close.store(true, std::memory_order_release);
        if (t && (t->GetCoordinatorState() != Transaction::COORD_CANCELLED))
            co_return;
        socket_.Close();
        context_.owner().reset();
        co_return;  
    }    

    void SendERROR(std::string err = "NULL") {
        SendImp(BuildError(err));
    }

    void SendString(const std::string& s){
        SendImp(BuildBulkString(s));
    }
    void SendViewStr(const std::string_view& s){
        SendImp(std::string(s));
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
        SendString(std::string());
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
        multi_res_ = std::move(s);
        p->DispatchBrief([this] () mutable {
            DoWrite();
            if (close.load(std::memory_order_acquire)) {
                socket_.Close();
                context_.owner().reset();
            }     
        });
        return;         
    }
    cppcoro::AsyncTask DoWrite() { 
        assert(util::Thread::current_tid() == pId_);
        auto wr = co_await socket_.AsyncWrite(multi_res_.data(), multi_res_.size(), -1);
        assert(wr == multi_res_.size());
        co_return;
    }

    friend class ConnectionContext;
    
    base::UringSocket socket_; 
    ::cmn::CmdArgList args_;
    ConnectionContext context_;
    Transaction transaction_;  
    ParseRESP p; 
    bool is_multi_command = false;
    base::RecvResult res_;
    std::string multi_res_;
    pthread_t pId_;
    std::atomic<bool> close = false;
};

class RedisServer {
public:
    RedisServer(int listenFd, uint32_t size)
        :   main_proactor_(std::make_shared<base::UringProactor>(CreateOptimizedRedisConfig())),
            pool_(size, CreateOptimizedRedisConfig()),
            ListenSocket_(main_proactor_, listenFd)
    {
        CIs = new CommandRegistry();
        RegisterStringFamily(CIs);
        RegisterGeneric(CIs);
        //RegisterMulti(CIs);
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
        config.registered_buf_size = 100;   
        config.cqe_batch_size = 100;          // 大批次处理：提高吞吐量
        config.task_queue_size = 1024;    
        config.sqe_batch_size = 32;   
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
            LOG(INFO) << "Starting ListenSocket...";
            listen();
        });
        main_proactor_->loop();
    }
    
    void Stop() {
        pool_.stop();
        main_proactor_->Shutdown();
        isRuning = false;
        if (shard_set) {
            shard_set->Shutdown();
        }
    }
    
private:
    cppcoro::AsyncTask listen() {
        
        while(isRuning){
            auto fd = co_await ListenSocket_.AsyncAccept();
            if (fd > 0){
                LOG(INFO) << "Accepted connection, fd: " << fd;
                // 禁用 Nagle 算法，Redis 响应多为小包，避免 40ms+ 延迟
                int nodelay = 1;
                setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
                // 启用 Quick ACK，进一步降低延迟
                int quickack = 1;
                setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));

                auto p = NextProactor();
                
                bool success = p->DispatchBrief([fd, p] () {
                    auto session = std::make_shared<RedisSession>(fd, p);
                    session->DoRead();
                });
                if (!success) {
                    LOG(ERROR) << "Failed to dispatch session to proactor: " << p->GetPoolIndex();
                    close(fd);
                }                    
            } else if (fd < 0) {
                LOG(WARNING) << "Failed to accept connection, error: " << strerror(errno);
            }                    
        }

        co_return;
    }
    auto NextProactor() -> base::UringProactorPtr {
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

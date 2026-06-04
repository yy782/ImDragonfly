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
#include "transaction_layer/transaction.hpp"
#include "base/fd_wrapper.hpp"
#include <cstring>

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
       
    }
    
    cppcoro::AsyncTask DoRead(){
        try{
            context_ = ConnectionContext(shared_from_this(), &namespaces->GetDefaultNamespace(), 0);
            int fd = socket_.fd();
            // LOG(INFO) << "New session created for fd: " << fd;
                        
            while (true) {
                auto r = co_await socket_.AsyncRead(RecvBuf_.BeginWrite(), RecvBuf_.writable_size(), -1);

                if (r > 0) {
                    RecvBuf_.hasWritten(r);

                    assert(debug_com_deal_one_Com);
                    debug_com_deal_one_Com = false;

                    Com_ = RecvBuf_.ParseRESP();
                    if (Com_.empty()) continue;
                    args_ = ::cmn::CmdArgList(Com_);
                    
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
                    // VLOG(2) << "Executing command: " << ci->name();
                    ci->Invoke(&transaction_->GetCommandContext(), args_); 
                }
                else if (r == 0) { 
                    LOG(INFO) << "Connection closed by client, fd: " << fd;
                    socket_.Close();
                    break;
                }
                else {
                    LOG(ERROR) << "Read error on fd: " << fd << ", error: " << strerror(errno);
                }            
            }
            context_.owner().reset();         
        } catch(const std::exception& e) {
            std::cerr << "Exception in session fd:" << socket_.fd() << ": " << e.what() << std::endl;
        }
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
                
        if (transaction_->GetState() == Transaction::State::EXEC && args_[0] == "EXEC") {// 这里如果DoRead意外恢复了，可能不同线程操作transaction_
            if (!transaction_->collectMultiRes(s)) return; // 可能多线程操作同一个容器
            s = BuildMultiArray(transaction_->SwapOrClearMultiRes());
            transaction_->FinishOrDiscardMulti();
        }

        p->DispatchBrief([this, s = std::move(s)](){
            SendBuf_.append(s);
            DoWrite();     
            debug_com_deal_one_Com = true;
        });         
    }

    cppcoro::AsyncTask DoWrite() {
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
    std::vector<std::string_view> Com_;
    ::cmn::CmdArgList args_;

    ConnectionContext context_;
    std::unique_ptr<Transaction> transaction_;   


    bool debug_com_deal_one_Com = true;
    bool is_multi_command = false;
};

class RedisServer {
public:
    RedisServer(int listenFd, uint32_t size)
        :   main_proactor_(new base::UringProactor(0, 4096)),
            pool_(size),
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
        try {
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
        }catch(const std::exception& e) {
            std::cerr << "Exception in accept: " << e.what() << std::endl;

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

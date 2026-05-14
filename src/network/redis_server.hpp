// network/redis_server.h
#pragma once // redis_server.hpp  
#include <glog/logging.h>
#include "sharding/engine_shard_set.hpp"
#include "redis/facade/resp_buf.hpp"
#include "redis/facade/reply_builder.hpp"

#include "command_layer/command_registry.hpp"
#include "command_layer/command_families.hpp"
#include "command_layer/conn_context.hpp"
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
        : socket_(proactor, fd) {
            
    }
    
    cppcoro::AsyncTask<cppcoro::AsyncPromise> DoRead(){
        int fd = socket_.fd();
        LOG(INFO) << "New session created for fd: " << fd;

        try{
            ctxt_.owner_ = shared_from_this();

            while (true) {
                auto r = co_await socket_.AsyncRead(RecvBuf_.BeginWrite(), RecvBuf_.writable_size(), -1);

                if (r > 0) {
                    RecvBuf_.hasWritten(r);
                    assert(debug_com_deal_one_Com);
                    debug_com_deal_one_Com = false;
                    Com = RecvBuf_.ParseRESP();
                    if (Com.empty()) continue;
                    args = ::cmn::CmdArgList(Com);
                    
                    VLOG(1) << "Received command: " << args[0] << " with " << args.size() << " arguments";
                    
                    ci = CIs->Find(args[0]);
                    if (!ci) { 
                        LOG(WARNING) << "Unknown command: " << args[0] << " from fd: " << fd;
                        SendERROR();
                        continue;
                    }

                    t.reset(new Transaction(ci));
                    t->InitByArgs(ns_, index_, args);
                    t->debug_owner() = this;
                    cm_txt = CommandContext(&ctxt_, t.get(), ci);
                    
                    VLOG(2) << "Executing command: " << ci->name();
                    ci->Invoke(args, &cm_txt);
                    VLOG(2) << "Command " << ci->name() << " executed successfully";
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
                      
        } catch(const std::exception& e) {
            LOG(ERROR) << "Exception in session fd:" << fd << ": " << e.what();
        }
        co_return;  
    }    

    void SendERROR() {
        SendImp(BuildError({"NULL"}));
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

    void SendStatus(const std::string& s){
        SendImp(BuildSimpleString(s));
    }
    void SendStatus(const std::string_view& s){
        SendStatus(std::string(s)); 
    }
    void SendStatus(const char* s){
        SendStatus(std::string(s)); 
    }
private:


    void SendImp(std::string&& s) {
        auto p = socket_.Proactor();
        p->DispatchBrief([this, s = std::move(s)](){
            SendBuf_.append(s);
            DoWrite();     
            
            debug_com_deal_one_Com = true;
        });
    }

    cppcoro::AsyncTask<cppcoro::AsyncPromise> DoWrite() {
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


    

    base::UringSocket socket_; 
    RESP_Buf RecvBuf_;
    RESP_Buf SendBuf_;

    Namespace* ns_ = &namespaces->GetDefaultNamespace(); 
    DbIndex index_ = 0;

    ConnectionContext ctxt_;

    std::vector<std::string> Com;

    ::cmn::CmdArgList args;
    CommandId* ci = nullptr;
    std::unique_ptr<Transaction> t;
    CommandContext cm_txt;

    std::atomic<bool> debug_com_deal_one_Com = true;
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
            auto cb = [](RedisServer* server) -> cppcoro::AsyncTask<cppcoro::AsyncPromise> {
                while(server->isRuning){
                    auto fd = co_await server->ListenSocket_.AsyncAccept();

                    if (fd > 0){
                        std::string addr = base::AddressToString(base::Address(fd));
                        LOG(INFO) << "Accepted new connection, addr: " << addr;

                        auto p = server->NextProactor();
                        VLOG(1) << "Assigning connection to proactor: " << p->GetPoolIndex();

                        auto session = std::make_shared<RedisSession>(fd, p);
                        
                        p->DispatchBrief([session](){
                            session->DoRead();
                        });                    
                    } else if (fd < 0) {
                        LOG(WARNING) << "Failed to accept connection, error: " << strerror(errno);
                    }
                }
                co_return;
            };
            cb(this);
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

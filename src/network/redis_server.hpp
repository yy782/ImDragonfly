// network/redis_server.h
#pragma once

#include "sharding/engine_shard_set.hpp"
#include "redis/facade/resp_buf.hpp"
#include "redis/facade/reply_builder.hpp"
#include "command_layer/parsed_command.hpp"
#include "command_layer/command_registry.hpp"
#include "command_layer/command_families.hpp"
#include "command_layer/conn_context.hpp"
#include "cppcoro/task.hpp"
#include "cppcoro/detail/task_promise.hpp"
#include "base/socket.hpp"
#include "sharding/namespaces.hpp"
#include "base/uring_proactor_pool.hpp"
namespace dfly{
using base::UringProactorPtr;
CommandRegistry* CIs = nullptr;

class RedisSession : public std::enable_shared_from_this<RedisSession> {
public:
    RedisSession(int fd, UringProactorPtr proactor)
        : socket_(proactor, fd) {
            
    }
    
    cppcoro::task<void, cppcoro::detail::task_promise<void, false>> DoRead(){

        ctxt_.owner_ = shared_from_this();
        while (true) {
            auto r = co_await socket_.AsyncRead(RecvBuf_.BeginWrite(), RecvBuf_.writable_size(), -1);
            if (r>0) {
                RecvBuf_.hasWritten(r);
                auto res = RecvBuf_.ParseRESP();
                if (res.empty()) continue;
                CommandId* ci = CIs->Find(res[0]);
                CmdArgList args = ::cmn::ParsedCommand(res.begin(), res.end(), res.size()).ToCmdArgList();
                Transaction t(ci);
                t.InitByArgs(ns_, index_, args);
                CommandContext cm_txt(&ctxt_, &t, ci);
                ci->Invoke(args, &cm_txt);
            }
            else if (r == 0 ) { 
                (void)socket_.Close();

                break;
            }
            else {
                // TODO
            }            
        }
        co_return;
    }    

    void SendOK() {
        SendBuf_.append(BuildSimpleString({"OK"}));

        DoWrite();
    }

    void SendERROR() {
        SendBuf_.append(BuildError({"NULL"}));

        DoWrite();
    }

    void Send(int64_t n) {
        SendBuf_.append(BuildInteger(n));

        DoWrite();
    }

    void Send(const std::string& s){
        SendBuf_.append(BuildBulkString(s));

        DoWrite();
    }
    void Send(const std::string_view& s){
        SendBuf_.append(BuildBulkString(std::string(s)));

        DoWrite();

    }

private:

    cppcoro::task<void, cppcoro::detail::task_promise<void, false>> DoWrite() {
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
};

class RedisServer {
public:
    RedisServer(int listenFd, uint32_t size)
        :   main_proactor_(std::make_shared<base::UringProactor>(0, 4096)),
            pool_(size),
            ListenSocket_(main_proactor_, listenFd)
    {
        shard_set = new EngineShardSet(&pool_);
        shard_set->Init(size);

        CIs = new CommandRegistry();
        RegisterStringFamily(CIs);
    }

    ~RedisServer() {
        delete CIs;
        CIs = nullptr;
    }
    
    void Start() {
        isRuning = true;
        pool_.AsyncLoop();

        main_proactor_->DispatchBrief([this]{
            auto cb = [this]() -> cppcoro::task<void, cppcoro::detail::task_promise<void, false>> {
                while(isRuning){
                    auto r = co_await ListenSocket_.AsyncAccept();

                    if (r.has_value()){

                        auto p = NextProactor();

                        auto session = std::make_shared<RedisSession>(r.value(), p);
                        
                        p->DispatchBrief([session](){
                            session->DoRead();
                        });                    
                    }                 
                }
                co_return;
            };
            cb();
        });
        main_proactor_->loop();
    }
    
    void Stop() {
        isRuning = false;
        pool_.stop();

        shard_set->Shutdown();
    }
    
private:

    auto NextProactor() const -> UringProactorPtr {
        return pool_[NextProIndex_%pool_.size()];
    }

    ssize_t NextProIndex_ = 0;


    base::UringProactorPtr main_proactor_;
    base::UringProactorPool pool_;
    base::UringSocket ListenSocket_;
    bool isRuning = false;

};



}
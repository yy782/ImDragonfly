// network/redis_server.h
#pragma once

#include "sharding/engine_shard_set.hpp"
#include "redis/facade/resp_buf.hpp"
#include "redis/facade/redis_parser.hpp"
#include "command_layer/parsed_command.hpp"
#include "command_layer/command_registry.hpp"
#include "command_layer/command_families.hpp"
#include "command_layer/conn_context.hpp"


namespace dfly{

CommandRegistry* CIs = nullptr;

class RedisSession : public std::enable_shared_from_this<RedisSession> {
public:
    RedisSession(int fd, UringProactor* proactor)
        : socket_(proactor, fd) {
            
    }
    
    cppcoro::task<void> DoRead(){

        ctxt_.owner = shared_from_this();
        while (true) {
            auto r = co_await socket_.AsyncRead(RecvBuf_.BeginWrite(), RecvBuf_.writable_size(), -1);
            if (r>0) {
                RecvBuf_.hasWritten(r);
                auto res = RecvBuf_.ParseRESP();
                if (res.empty()) continue;
                cmn::BackedArguments bdArgments(res.begin(), res.end(), res.size());
                CommandId* ci = CIs.Find(bdArgments.Front());
                CmdArgList args = ParsedCommand(bdArgments).ToCmdArgList();
                Transaction t(ci);
                t.InitByArgs(ns_, index_, args);
                CommandContext cm_txt(ctxt_, &t, ci);
                ci->Invoke(args, cm_txt);
            }
            else if (n ==0 ) {
                socket_.Close();

                break;
            }
            else {
                // TODO
            }            
        }
        co_return;
    }    

    void SendOk() {
        SendBuf_.append(BuildSimpleString({"OK"}));

        DoWrite();
    }

    void SendERROR() {
        SendBuf_.append(BuildError({"NULL"}));

        DoWrite();
    }

    template<typename T>
    void Send(T&& t) {
        if constexpr (std::is_constructible<std::string, T>::value) {
            SendBuf_.append(BuildBulkString({t}));
        } else {
            SendBuf_.append(BuildInteger(static_cast<int64_t>(t)));
        }

        DoWrite();
    }
private:

    cppcoro::task<void> DoWrite() {
        while (SendBuf_.readable_size()) {
            auto wr = co_await socket_.AsyncWrite(SendBuf_.peek(), SendBuf_.readable_size(), -1);
            if (wr>0) {
                SendBuf_.retrieve(wr);
            }else {
                // TODO
            }
        }
        co_return;
    }


    UringSocket socket_; 
    RESP_Buf RecvBuf_;
    RESP_Buf SendBuf_;

    Namespace* ns_ = &GetDefaultNamespace(); 
    DbIndex index_ = 0;

    ConnextionContext ctxt_;
};

class RedisServer {
public:
    RedisServer(int listenFd, uint32_t size)
        : pool_(size),
          socket_(&main_proactor_, listenfd)
    {
        shard_set = new EngineShardSet(&pool_);
        shard_set->Init(4);

        CIs = new CommandRegistry();
        RegisterStringFamily(cis_);
    }

    ~RedisServer() {
        delete CIs;
        CIs = nullptr;
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
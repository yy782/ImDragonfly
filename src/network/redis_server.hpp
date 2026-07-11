#pragma once
#include <glog/logging.h>
#include <netinet/tcp.h>
#include "sharding/engine_shard_set.hpp"
#include "redis/facade/resp_buf.hpp"
#include "redis/facade/reply_builder.hpp"

#include "command_layer/command_registry.hpp"
#include "command_layer/command_families.hpp"

#include "command_layer/multi_family.hpp"
#include "sharding/namespaces.hpp"
#include "transaction_layer/transaction.hpp"
#include "YY/net/TcpServer.h"
#include "YY/net/TcpConnection.h"
#include "YY/net/EventLoop.h"
#include "YY/net/InetAddress.h"
#include <memory>
#include <cstring>
#include <exception>

namespace dfly{

inline CommandRegistry* CIs = nullptr;
class RedisServer;
inline RedisServer* ser = nullptr;

class RedisSession : public yy::net::TcpConnection {
public:
    RedisSession(int fd, const yy::net::Address& addr, yy::net::EventLoop* loop)
        : TcpConnection(fd, addr, loop)
    {
    }

    ~RedisSession() {
        assert(std::uncaught_exceptions() == 0);
    }

    Transaction* GetTransaction() { return &transaction_; }
    yy::net::EventLoop* GetProactor() { return loop(); }

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
    void OnMessage() {
        int client_fd = this->fd();
        

        auto self = std::static_pointer_cast<RedisSession>(shared_from_this());
        context_ = ConnectionContext(self, &namespaces->GetDefaultNamespace(), 0);
        
        auto& com = parser_.ParseRESP(recvBuffer());
        
        if (com.empty())
            return;
            
        args_ = ::cmn::CmdArgList(com);
        
        std::string cmd_name(args_[0]);
        std::transform(cmd_name.begin(), cmd_name.end(), cmd_name.begin(), ::toupper);
        
        auto ci = CIs->Find(cmd_name);
        if (!ci) {
            LOG(WARNING) << "Unknown command: " << args_[0] << " from fd: " << client_fd;
            SendERROR("unknown command:" + std::string(args_[0]));
            return;
        }
        
        is_multi_command = (cmd_name == "MULTI" || cmd_name == "EXEC" || 
                                cmd_name == "DISCARD" || cmd_name == "WATCH" || cmd_name == "UNWATCH");

        if (transaction_.GetState() == Transaction::State::IDLE) {
            std::destroy_at(&transaction_);                
            std::construct_at(&transaction_, ci);         
            transaction_.InitByArgs(&context_, args_);
        } else {
            if (transaction_.GetState() == Transaction::State::MULTI && !is_multi_command) {
                transaction_.QueueCommand(ci, args_);
                SendStatus("QUEUED");
                return;
            }               
        }
        
        ci->Invoke(&transaction_.GetCommandContext(), args_);
        
    }

    void OnClose() {
        LOG(INFO) << "Connection closed by client, fd: " << fd();
        context_.owner().reset();
    }

    void OnError() {
        LOG(ERROR) << "Error on connection, fd: " << fd() <<" errno:" <<errno;
    }
    int fd() const noexcept { return fd_; }
private:
    void SendImp(std::string&& s) {
        yy::net::sockets::send(fd(), s.data(), s.size(), MSG_NOSIGNAL);
    }

    friend class ConnectionContext;
    
    RESP_Buf parser_;
    ::cmn::CmdArgList args_;
    ConnectionContext context_;
    Transaction transaction_;   
    bool is_multi_command = false;
};

class RedisServer {
public:
    RedisServer(uint16_t port, int acceptorNum, int workThreadNum)
        : server_(yy::net::Address(port), acceptorNum, workThreadNum)
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

        server_.setConnectCallBack([this](int fd, const yy::net::Address& addr, yy::net::EventLoop* loop) {
            auto session = std::make_shared<RedisSession>(fd, addr, loop);
            LOG(INFO) << "New connection from " << addr.sockaddrToString() << ", fd: " << fd;
            session->setTcpNoDelay(true);
            session->setMessageCallBack([this, se = session](yy::net::TcpConnectionPtr) {
                
                se->OnMessage();
            });
            session->setCloseCallBack([this, se = session](yy::net::TcpConnectionPtr) {
                se->OnClose();
            });
            session->setErrorCallBack([this, se = session](yy::net::TcpConnectionPtr) {
                se->OnError();
            });
            session->setReading();
            return session;
        });
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
      

        server_.loop();
        shard_set = new EngineShardSet(server_.getWorkThreadPool());
        shard_set->Init(server_.getWorkThreadPool()->size());         
        server_.wait();
    }
    
    void Stop() {
        server_.stop();
        isRuning = false;
        if (shard_set) {
            shard_set->Shutdown();
        }
    }
    
private:
    yy::net::TcpServer server_;
    bool isRuning = false;
};

}

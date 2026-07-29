#pragma once
#include <glog/logging.h>
#include <netinet/tcp.h>

#include <cstring>
#include <exception>
#include <memory>

#include "YY/net/EventLoop.h"
#include "YY/net/InetAddress.h"
#include "YY/net/OneAccSer.h"
#include "YY/net/TcpConnection.h"
#include "YY/net/TcpServer.h"
#include "command_layer/command_families.hpp"
#include "command_layer/command_registry.hpp"
#include "command_layer/multi_family.hpp"
#include "redis/facade/reply_builder.hpp"
#include "redis/facade/resp_buf.hpp"
#include "sharding/engine_shard_set.hpp"
#include "sharding/namespaces.hpp"
#include "transaction_layer/transaction.hpp"
#include "detail/conn_context.hpp"
namespace dfly {

inline CommandRegistry* CIs = nullptr;
class RedisServer;
inline RedisServer* ser = nullptr;

class RedisSession : public yy::net::TcpConnection {
 public:
  RedisSession(int fd, const yy::net::Address& addr, yy::net::EventLoop* loop)
      : TcpConnection(fd, addr, loop) {

      }
  void init() {
    auto self = std::static_pointer_cast<RedisSession>(shared_from_this());
    rb_.SetSendCallback([self](std::string&& s) {
      self->RedisSend(std::move(s));
    });

    context_ = ConnectionContext(self, &namespaces->GetDefaultNamespace(), 0);    
  }
  ~RedisSession() { assert(std::uncaught_exceptions() == 0); }

  Transaction* GetTransaction() { return &transaction_; }
  yy::net::EventLoop* GetProactor() { return loop(); }
  cppcoro::AsyncTask OnMessage() {
    int client_fd = this->fd();

    auto& com = parser_.ParseRESP(recvBuffer());

    if (com.empty()) co_return;

    args_ = ::cmn::CmdArgList(com);
    auto ci = CIs->Find(args_[0]);
    if (!ci) {
      LOG(WARNING) << "Unknown command: " << args_[0]
                   << " from fd: " << client_fd;
      rb_.BuildError("unknown command:" + std::string(args_[0]));
      co_return;
    }

    LOG(INFO) << "CmdArgListToString: " << CmdArgListToString(args_) << " fd: " << client_fd;
    
    std::destroy_at(&transaction_); // 事务可能还没结束
    std::construct_at(&transaction_, ci);
    transaction_.InitByArgs(context_.GetNamespace(), context_.GetDbIndex(),
                            args_);
    cmd_cntx_ = CommandContext(&transaction_, ci, &rb_);
    ci->Invoke(&cmd_cntx_, args_);
    co_return;
  }

  void OnClose() {
    LOG(INFO) << "Connection closed by client, fd: " << fd();
    context_.owner().reset();
  }

  void OnError() {
    if (errno == 0) return;
    LOG(ERROR) << "Error on connection, fd: " << fd() << " errno:" << errno;
    context_.owner().reset();  // 可能有问题，如果当前事务没有结束
    disconnect();
  }

  void RedisSend(std::string&& s) {
    yy::net::sockets::send(fd(), s.data(), s.size(), MSG_NOSIGNAL);
  }
  int fd() const noexcept { return fd_; }

 private:
  void SendImp(std::string&& s) {}
  friend class ConnectionContext;
  RESP_Buf parser_;
  ::cmn::CmdArgList args_;
  ConnectionContext context_;
  Transaction transaction_;
  ReplyBuilder rb_;
  CommandContext cmd_cntx_;
};

class RedisServer {
 public:
  RedisServer(uint16_t port, yy::net::EventLoop* loop, int workThreadNum)
      : server_(yy::net::Address(port), loop, workThreadNum) {
    CIs = new CommandRegistry();
    RegisterStringFamily(CIs);
    RegisterGeneric(CIs);
    // // RegisterMulti(CIs);
    RegisterListFamily(CIs);
    RegisterHashFamily(CIs);
    RegisterSetFamily(CIs);
    RegisterZSetFamily(CIs);
    ser = this;

    server_.setConnectCallBack([this](int fd, const yy::net::Address& addr,
                                      yy::net::EventLoop* loop) {
      auto session = std::make_shared<RedisSession>(fd, addr, loop);
      session->init();
      LOG(INFO) << "New connection from " << addr.sockaddrToString()
                << ", fd: " << fd;
      session->setTcpNoDelay(true);
      session->setMessageCallBack(
          [this, se = session](yy::net::TcpConnectionPtr) { se->OnMessage(); });
      session->setCloseCallBack(
          [this, se = session](yy::net::TcpConnectionPtr) { se->OnClose(); });
      session->setErrorCallBack(
          [this, se = session](yy::net::TcpConnectionPtr) { se->OnError(); });
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
  }

  void Stop() {
    server_.stop();
    isRuning = false;
    if (shard_set) {
      shard_set->Shutdown();
    }
  }

 private:
  yy::net::Server server_;
  bool isRuning = false;
};

}  // namespace dfly

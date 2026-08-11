#pragma once
#include <glog/logging.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <cstring>
#include <exception>
#include <memory>

#include "command_layer/command_families.hpp"
#include "command_layer/command_registry.hpp"
#include "command_layer/multi_family.hpp"
#include "detail/conn_context.hpp"
#include "net/fd_wrapper.hpp"
#include "net/uring_proactor.hpp"
#include "net/uring_proactor_pool.hpp"
#include "net/uring_socket.hpp"
#include "redis/facade/ParseRESP.hpp"
#include "redis/facade/reply_builder.hpp"
#include "sharding/engine_shard_set.hpp"
#include "sharding/namespaces.hpp"
#include "transaction_layer/transaction.hpp"
#include "util/Strings.hpp"
namespace dfly {

inline CommandRegistry* CIs = nullptr;
class RedisServer;
inline RedisServer* ser = nullptr;

class RedisSession : public std::enable_shared_from_this<RedisSession> {
 public:
  RedisSession(int fd, base::UringProactorPtr p) : socket_(p, fd) {}

  ~RedisSession() { assert(std::uncaught_exceptions() == 0); }

  base::UringProactorPtr GetProactor() { return socket_.Proactor(); }
  std::shared_ptr<Transaction> GetTransaction() { return transaction_; }

  void init() {
    auto self = shared_from_this();
    std::weak_ptr<RedisSession> weak_self = self;
    rb_.SetSendCallback([weak_self](std::string&& s) {
      auto self = weak_self.lock();
      if (!self) return;
      VLOG(3) << "fd:" << self->fd() << " send:" << s;
      self->SendImp(std::move(s));
    });
    context_ = ConnectionContext(self, &namespaces->GetDefaultNamespace(), 0);
  }

  cppcoro::AsyncTask DoRead() {  // 不支持管道， 要支持管道，复杂度高
    socket_.RegisterRecvBuf();
    pId_ = socket_.Proactor()->GetLoopThreadId();
    int fd = socket_.fd();
    size_t recv_offset = 0;
    while (true) {
      auto res = co_await socket_.AsyncRead(recv_offset);
      assert(util::Thread::current_tid() == pId_);
      if (res.bytes > 0) {
        size_t total = recv_offset + res.bytes;
        auto& com = parser_.Parse(res.data, total);  // todo 没有记忆
        if (com.empty()) {
          recv_offset = total;
          continue;
        }
        recv_offset = 0;

        VLOG(3) << " fd: " << fd << " com: " << util::VecToStr(com);

        args_ = ::cmn::CmdArgList(com);
        auto ci = CIs->Find(args_[0]);
        if (!ci) {
          LOG(WARNING) << "Unknown command: " << args_[0] << " from fd: " << fd;
          rb_.BuildError("unknown command:" + std::string(args_[0]));
          continue;
        }

        VLOG(3) << " fd: " << fd
                << " CmdArgListToString: " << CmdArgListToString(args_);

        transaction_.reset();
        transaction_ = std::make_shared<Transaction>(ci);
        transaction_->InitByArgs(context_.GetNamespace(), context_.GetDbIndex(),
                                 args_);
        cmd_cntx_ = CommandContext(transaction_, ci, &rb_);
        ci->Invoke(&cmd_cntx_, args_);
      } else if (res.bytes == 0 || res.bytes == -104) {
        LOG(INFO) << "Connection closed by client, fd: " << fd;
        break;
      } else {
        LOG(WARNING) << "Read error on fd: " << fd << ", error" << res.bytes;
        break;
      }
    }
    socket_.Close();
    context_.owner().reset();
    co_return;
  }

  int fd() const noexcept { return socket_.fd(); }

  friend class ConnectionContext;

 private:
  void SendImp(std::string&& s) {
    auto p = socket_.Proactor();
    multi_res_ = std::move(s);
    p->DispatchBrief([this]() mutable { DoWrite(); });
  }

  cppcoro::AsyncTask DoWrite() {
    assert(util::Thread::current_tid() == pId_);
    size_t written = 0;
    while (written < multi_res_.size()) {
      auto wr = co_await socket_.AsyncWrite(multi_res_.data() + written,
                                            multi_res_.size() - written);
      if (wr <= 0) {
        LOG(WARNING) << "Write error on fd: " << fd() << ", error: " << wr;
        break;
      }
      written += static_cast<size_t>(wr);
    }
    co_return;
  }

  base::UringSocket socket_;
  ::cmn::CmdArgList args_;
  ConnectionContext context_;
  std::shared_ptr<Transaction> transaction_;
  ReplyBuilder rb_;
  CommandContext cmd_cntx_;
  ParseRESP parser_;
  std::string multi_res_;
  pthread_t pId_;
};

class RedisServer {
 public:
  RedisServer(int listenFd, uint32_t size)
      : main_proactor_(std::make_shared<base::UringProactor>(
            CreateOptimizedRedisConfig())),
        pool_(size, CreateOptimizedRedisConfig()),
        ListenSocket_(main_proactor_, listenFd) {
    CIs = new CommandRegistry();
    RegisterStringFamily(CIs);
    RegisterGeneric(CIs);
    // // RegisterMulti(CIs);
    RegisterListFamily(CIs);
    RegisterHashFamily(CIs);
    RegisterSetFamily(CIs);
    RegisterZSetFamily(CIs);
    ser = this;
  }

  static base::UringConfig CreateOptimizedRedisConfig() {
    base::UringConfig config;
    config.queue_depth = 4096;
    config.use_defer_taskrun = true;
    config.use_single_issuer = true;
    config.use_sqpoll = false;
    config.use_registered_bufs = true;
    config.registered_buf_count = 1024;
    config.registered_buf_size = 4096;
    config.cqe_batch_size = 100;
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
    main_proactor_->DispatchBrief([this] {
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
    while (isRuning) {
      auto fd = co_await ListenSocket_.AsyncAccept();
      if (fd > 0) {
        LOG(INFO) << "Accepted connection, fd: " << fd;
        int nodelay = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
        int quickack = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));

        auto p = NextProactor();
        bool success = p->DispatchBrief([fd, p]() {
          auto session = std::make_shared<RedisSession>(fd, p);
          session->init();
          session->DoRead();
        });
        if (!success) {
          LOG(ERROR) << "Failed to dispatch session to proactor: "
                     << p->GetPoolIndex();
          close(fd);
        }
      } else if (fd < 0) {
        LOG(WARNING) << "Failed to accept connection, error: "
                     << strerror(errno);
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

}  // namespace dfly
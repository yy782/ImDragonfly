#pragma once
#include <glog/logging.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <deque>
#include <exception>
#include <memory>
#include <mutex>

#include "command_layer/command_families.hpp"
#include "command_layer/command_registry.hpp"
#include "command_layer/multi_family.hpp"
#include "detail/conn_context.hpp"
#include "net/fd_wrapper.hpp"
#include "net/uring_proactor.hpp"
#include "net/uring_proactor_pool.hpp"
#include "net/uring_socket.hpp"
#include "network/pipeline_squasher.hpp"
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
    send_cb_ = [weak_self](std::string&& s) {
      auto self = weak_self.lock();
      if (!self) return;
      VLOG(3) << "fd:" << self->fd() << " send:" << s;
      auto p = self->GetProactor();
      p->DispatchBrief(
          [self, s = std::move(s)]() mutable { self->SendImp(std::move(s)); });
    };
    context_ = ConnectionContext(self, &namespaces->GetDefaultNamespace(), 0);
    // 一次构造、连接生命周期内复用：避免每批命令都重建
    // dispatched_ 数组（按 shard 数 resize）与 ReplyBuilder 回调。
    squasher_.Init(context_.GetNamespace(), context_.GetDbIndex(), send_cb_,
                   socket_.Proactor().get());
  }
  // 不打算实现持久化，MUTLI,EXEC等实现以后完成
  // 目前DashTable,事务调度，协程，SIMD，以及各种第三方boost,function_base都够吃一壶的了
  // 一个人开发难度大，AI还看不懂代码
  cppcoro::AsyncTask DoRead() {
    socket_.RegisterRecvBuf();
    pId_ = socket_.Proactor()->GetLoopThreadId();
    int fd = socket_.fd();
    size_t recv_offset = 0;
    std::vector<QCmd> queue;
    size_t qidx = 0;

    while (true) {
      if (qidx < queue.size()) {
        co_await squasher_.Run(std::move(queue));
        queue.clear();
        qidx = 0;
      }

      auto res = co_await socket_.AsyncRead(recv_offset);
      assert(util::Thread::current_tid() == pId_);
      if (res.bytes > 0) {
        recv_count++;
        size_t total = recv_offset + res.bytes;
        auto pr = parser_.ParseAll(res.data, total);

        for (auto& cmd_args : pr.cmds) {
          ::cmn::CmdArgList args(cmd_args);
          std::string upper_cmd = util::ToUpperIfNeeded(args[0]);
          auto ci = CIs->Find(upper_cmd.empty() ? args[0] : upper_cmd);
          if (!ci) {
            LOG(WARNING) << "Unknown command: " << args[0]
                         << " from fd: " << fd;
            send_cb_("-ERR unknown command:" + std::string(args[0]) + "\r\n");
            continue;
          }
          queue.push_back({ci, std::move(cmd_args)});
        }

        if (pr.partial_offset < total) {  // 可能要优化，
          size_t partial_len = total - pr.partial_offset;
          std::memmove(const_cast<char*>(res.data),
                       res.data + pr.partial_offset, partial_len);
          recv_offset = partial_len;
        } else {
          recv_offset = 0;
        }
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
    VLOG(2) << "[SEND] fd:" << fd() << " size:" << s.size()
            << " head:" << s.substr(0, s.find('\r'));
    write_queue_.push_back(std::move(s));
    if (write_in_progress_) {
      return;
    }
    write_in_progress_ = true;
    DoWrite();
  }

  cppcoro::AsyncTask DoWrite() {
    assert(util::Thread::current_tid() == pId_);
    // 一次 sendmsg 合并的回复条数上限：≤ UIO_FASTIOV(8)，内核 __import_iovec
    // 走栈上 iovec[8] 快路径（无 kmalloc、无 iovec 数组全量拷贝），
    // 同时保留批量合并减少 syscall 次数（比逐条发送少 kMaxWriteBatch 倍）。
    constexpr size_t kMaxWriteBatch = 8;
    while (true) {
      // 批量出队：batch 内的 string 存活于协程帧，iovec 指向其 data() 期间安全
      std::vector<std::string> batch;
      if (write_queue_.empty()) {
        write_in_progress_ = false;
        break;
      }
      size_t n = std::min(write_queue_.size(), kMaxWriteBatch);
      batch.reserve(n);
      for (size_t i = 0; i < n; ++i) {
        batch.push_back(std::move(write_queue_.front()));
        write_queue_.pop_front();
      }
      size_t total = 0;
      for (auto& s : batch) {
        total += s.size();
      }

      size_t sent = 0;
      while (sent < total) {
        std::vector<struct iovec> rem;
        size_t skip = sent;
        for (auto& s : batch) {
          if (skip >= s.size()) {
            skip -= s.size();
            continue;
          }
          rem.push_back({s.data() + skip, s.size() - skip});
          skip = 0;
        }
        if (rem.empty()) break;
        struct msghdr msg = {};
        msg.msg_iov = rem.data();
        msg.msg_iovlen = rem.size();
        auto wr = co_await socket_.AsyncWriteV(&msg);
        if (wr <= 0) {
          LOG(WARNING) << "Write error on fd: " << fd() << ", error: " << wr;
          write_queue_.clear();
          write_in_progress_ = false;
          co_return;
        }
        sent += static_cast<size_t>(wr);
      }
      send_count++;
    }
    co_return;
  }

  base::UringSocket socket_;
  ConnectionContext context_;
  std::shared_ptr<Transaction> transaction_;
  ReplyBuilder::SendCallback send_cb_;
  ParseRESP parser_;
  PipelineSquasher squasher_;
  std::deque<std::string> write_queue_;
  bool write_in_progress_ = false;
  pthread_t pId_;

  int recv_count = 0;
  int send_count = 0;
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
    // RegisterMulti(CIs);
    RegisterListFamily(CIs);
    RegisterHashFamily(CIs);
    RegisterSetFamily(CIs);
    RegisterZSetFamily(CIs);
    ser = this;
  }

  static base::UringConfig CreateOptimizedRedisConfig() {
    // TODO 改用文件读取配置更好
    // 应用层必须保证目前有多少连接在使用，这样可以使用io_uring的注册缓冲区，性能更好,不然连接超过registered_buf_count就UB了
    base::UringConfig config;
    config.queue_depth = 4096;
    config.use_defer_taskrun = true;
    config.use_single_issuer = true;
    config.use_sqpoll = false;
    config.use_registered_bufs = true;
    config.registered_buf_count = 1024;
    config.registered_buf_size = 4096;
    config.cqe_batch_size = 100;
    config.task_queue_size = 16384;  // 要求2的幂
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
    main_proactor_->Run();
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
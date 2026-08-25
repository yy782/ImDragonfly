#pragma once
#include <glog/logging.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "command_layer/command_registry.hpp"
#include "command_layer/generic_family.hpp"
#include "command_layer/multi_family.hpp"
#include "detail/conn_context.hpp"
#include "io/fd_wrapper.hpp"
#include "io/uring_proactor.hpp"
#include "io/uring_proactor_pool.hpp"
#include "io/uring_socket.hpp"
#include "redis/facade/ParseRESP.hpp"
#include "redis/facade/reply_builder.hpp"
#include "server/pipeline_squasher.hpp"
#include "server/write_batcher.hpp"
#include "sharding/shard_pool.hpp"
#include "sharding/synchronization.hpp"
#include "transaction_layer/transaction.hpp"
#include "util/Strings.hpp"
#include "util/json_config.hpp"
namespace dfly {

inline CommandRegistry* CIs = nullptr;

class RedisSession : public std::enable_shared_from_this<RedisSession> {
 public:
  // proactor 由 UringProactorPool 管理生命周期，传裸指针即可。
  RedisSession(int fd, base::UringProactor* p)
      : socket_(p, fd),
        write_batcher_(&socket_, p->GetLoopThreadId()),
        pId_(p->GetLoopThreadId()) {}

  ~RedisSession() { assert(std::uncaught_exceptions() == 0); }

  base::UringProactor* GetProactor() { return socket_.Proactor(); }

  void init() {
    auto self = shared_from_this();
    std::weak_ptr<RedisSession> weak_self = self;
    send_cb_ = [weak_self](std::vector<std::string>&& batch) {
      if (batch.empty()) return;
      auto self = weak_self.lock();
      if (!self) return;
      self->SendBatchImp(std::move(batch));
    };
    context_ = ConnectionContext(self, 0);
    // context_ 是 RedisSession 的成员，这里又把 self（指向自己）塞进
    // context_.owner_， 形成 RedisSession → context_ → owner_ → RedisSession
    // 的循环引用。
    // 为什么不用哈希表管理所有连接？我又不需要"当前有多少连接"这种信息，
    // 上个哈希表还要配 erase 路径，erase 漏了一样泄漏——和循环引用 reset
    // 漏了等价。 循环引用的代价是 DoRead 退出时必须手动调 NotifyClose
    // 断环，否则 RedisSession 不析构。 项目已禁用异常，DoRead 的 break
    // 路径都在函数末尾统一走 NotifyClose，没有跳过风险。

    squasher_.Init(context_.GetDbIndex(), send_cb_, socket_.Proactor());
  }

  cppcoro::AsyncTask DoRead() {
    socket_.RegisterRecvBuf();
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
        size_t total = recv_offset + res.bytes;
        auto pr = parser_.ParseAll(res.data, total);

        for (auto& cmd_args : pr.cmds) {
          ::dfly::CmdArgList args(cmd_args);
          std::string upper_cmd = util::ToUpperIfNeeded(args[0]);
          std::string_view cmd = upper_cmd.empty() ? args[0] : upper_cmd;
          const CommandId* ci = CIs->Find(cmd);
          if (!ci) {
            LOG(WARNING) << "Unknown command: " << args[0]
                         << " from fd: " << fd;
            SendImp("-ERR unknown command:" + std::string(args[0]) + "\r\n");
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
    context_.NotifyClose();
    co_return;
  }

  int fd() const noexcept { return socket_.fd(); }

  friend class ConnectionContext;

  void SendBatchImp(std::vector<std::string>&& batch) {
    write_batcher_.EnqueueBatch(std::move(batch));
  }

  void SendImp(std::string&& s) { write_batcher_.Enqueue(std::move(s)); }

  base::UringSocket socket_;
  ConnectionContext context_;
  ReplyBuilder::SendCallback send_cb_;
  ParseRESP parser_;
  PipelineSquasher squasher_;
  WriteBatcher write_batcher_;
  pthread_t pId_;
};

class RedisServer {
 public:
  RedisServer(int listenFd, uint32_t size,
              const util::JsonConfig* config = nullptr)
      : main_proactor_(
            new base::UringProactor(CreateOptimizedRedisConfig(config))),
        pool_(size, CreateOptimizedRedisConfig(config)),
        ListenSocket_(main_proactor_, listenFd) {
    CIs = new CommandRegistry();
    RegisterStringFamily(CIs);
    RegisterGeneric(CIs);
    // RegisterMulti(CIs);
    RegisterListFamily(CIs);
    RegisterHashFamily(CIs);
    RegisterSetFamily(CIs);
    RegisterZSetFamily(CIs);
  }

  // 有配置文件则从文件读取，否则使用内置默认值。
  // 应用层必须保证连接数不超过 registered_buf_count，
  // 否则 io_uring 注册缓冲区的读路径会越界（UB）。
  static base::UringConfig CreateOptimizedRedisConfig(
      const util::JsonConfig* cfg = nullptr) {
    base::UringConfig config;
    config.queue_depth = 4096;
    config.use_defer_taskrun = true;
    config.use_single_issuer = true;
    config.use_sqpoll = false;
    config.registered_buf_count = 1024;
    config.registered_buf_size = 4096;
    config.cqe_batch_size = 100;
    config.task_queue_size = 16384;  // 要求2的幂
    config.sqe_batch_size = 32;

    if (!cfg) return config;

    config.queue_depth =
        static_cast<int>(cfg->GetInt("queue_depth", config.queue_depth));
    config.use_defer_taskrun =
        cfg->GetBool("use_defer_taskrun", config.use_defer_taskrun);
    config.use_single_issuer =
        cfg->GetBool("use_single_issuer", config.use_single_issuer);
    config.use_sqpoll = cfg->GetBool("use_sqpoll", config.use_sqpoll);
    config.sqpoll_idle_ms = static_cast<uint32_t>(
        cfg->GetInt("sqpoll_idle_ms", config.sqpoll_idle_ms));
    config.registered_buf_count = static_cast<int>(
        cfg->GetInt("registered_buf_count", config.registered_buf_count));
    config.registered_buf_size = static_cast<int>(
        cfg->GetInt("registered_buf_size", config.registered_buf_size));
    config.cqe_batch_size =
        static_cast<int>(cfg->GetInt("cqe_batch_size", config.cqe_batch_size));
    config.task_queue_size = static_cast<int>(
        cfg->GetInt("task_queue_size", config.task_queue_size));
    config.sqe_batch_size = static_cast<uint32_t>(
        cfg->GetInt("sqe_batch_size", config.sqe_batch_size));
    config.poll_timeout_ms = static_cast<int>(
        cfg->GetInt("poll_timeout_ms", config.poll_timeout_ms));
    config.poll_min_cqe =
        static_cast<int>(cfg->GetInt("poll_min_cqe", config.poll_min_cqe));
    return config;
  }

  ~RedisServer() {
    delete CIs;
    CIs = nullptr;

    if (shard_pool) {
      delete shard_pool;
      shard_pool = nullptr;
    }

    delete main_proactor_;
    main_proactor_ = nullptr;
  }

  void Start() {
    LOG(INFO) << "Starting RedisServer...";
    isRuning = true;
    pool_.AsyncLoop();
    sleep(1);
    shard_pool = new ShardPool(&pool_);
    shard_pool->Init(pool_.size());

    dfly::InitShardQueueIfSpsc(main_proactor_->GetTaskQueue(), 0, 1);

    main_proactor_->DispatchBriefFromMain([this] {
      LOG(INFO) << "Starting ListenSocket...";
      listen();
    });
    main_proactor_->Run();
  }

  void Stop() {
    if (shard_pool) {
      shard_pool->Shutdown();
    }
    pool_.stop();
    main_proactor_->Shutdown();
    isRuning = false;
  }

  size_t ShardCount() const { return pool_.size(); }

  base::UringProactor* MainProactor() { return main_proactor_; }

  static RedisServer* Init(int listen_fd, uint32_t size,
                           const util::JsonConfig* config = nullptr) {
    if (!instance_) {
      instance_ = new RedisServer(listen_fd, size, config);
    }
    return instance_;
  }

  static RedisServer& Instance() { return *instance_; }

  static void Destroy() {
    delete instance_;
    instance_ = nullptr;
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
        // main 线程向分片 p 投递新连接任务（段模式下写分片 p 队列的 main 段）
        bool success = p->DispatchBriefFromMain([fd, p]() {
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

  auto NextProactor() -> base::UringProactor* {
    NextProIndex_ = (NextProIndex_ + 1) % pool_.size();
    return pool_[NextProIndex_];
  }

  ssize_t NextProIndex_ = 0;
  base::UringProactor* main_proactor_ = nullptr;
  base::UringProactorPool pool_;
  base::UringSocket ListenSocket_;
  bool isRuning = false;

  inline static RedisServer* instance_ = nullptr;
};

}  // namespace dfly
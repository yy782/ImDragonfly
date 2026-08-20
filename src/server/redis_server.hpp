#pragma once
#include <glog/logging.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "command_layer/command_families.hpp"
#include "command_layer/command_registry.hpp"
#include "command_layer/generic_family.hpp"
#include "command_layer/multi_family.hpp"
#include "detail/conn_context.hpp"
#include "io/fd_wrapper.hpp"
#include "io/uring_proactor.hpp"
#include "io/uring_proactor_pool.hpp"
#include "io/uring_socket.hpp"
#include "persistence/rdb_serializer.hpp"
#include "redis/facade/ParseRESP.hpp"
#include "redis/facade/reply_builder.hpp"
#include "server/pipeline_squasher.hpp"
#include "server/write_batcher.hpp"
#include "sharding/engine_shard_set.hpp"
#include "sharding/namespaces.hpp"
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
      auto p = self->GetProactor();
      p->DispatchBrief([self, batch = std::move(batch)]() mutable {
        self->SendBatchImp(std::move(batch));
      });
    };
    context_ = ConnectionContext(self, &namespaces->GetDefaultNamespace(), 0);
    // context_ 是 RedisSession 的成员，这里又把 self（指向自己）塞进
    // context_.owner_， 形成 RedisSession → context_ → owner_ → RedisSession
    // 的循环引用。
    // 为什么不用哈希表管理所有连接？我又不需要"当前有多少连接"这种信息，
    // 上个哈希表还要配 erase 路径，erase 漏了一样泄漏——和循环引用 reset
    // 漏了等价。 循环引用的代价是 DoRead 退出时必须手动调 NotifyClose
    // 断环，否则 RedisSession 不析构。 项目已禁用异常，DoRead 的 break
    // 路径都在函数末尾统一走 NotifyClose，没有跳过风险。

    squasher_.Init(context_.GetNamespace(), context_.GetDbIndex(), send_cb_,
                   socket_.Proactor());
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
          ::cmn::CmdArgList args(cmd_args);
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
  RedisServer(const RedisServer&) = delete;
  RedisServer& operator=(const RedisServer&) = delete;
  RedisServer(RedisServer&&) = delete;
  RedisServer& operator=(RedisServer&&) = delete;

  // 单例初始化：main 启动时调一次。重复调用会 assert 失败。
  static void Init(int listenFd, uint32_t size, bool enable_rdb = true,
                   const util::JsonConfig* config = nullptr) {
    assert(instance_ == nullptr);
    instance_ = new RedisServer(listenFd, size, enable_rdb, config);
  }

  // 单例访问：Init 之后才能调。未初始化会 assert 失败。
  static RedisServer& Instance() {
    assert(instance_ != nullptr);
    return *instance_;
  }

  // 销毁单例：main 退出前调用，释放 RedisServer 资源。
  static void Destroy() {
    delete instance_;
    instance_ = nullptr;
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

    if (shard_set) {
      delete shard_set;
      shard_set = nullptr;
    }

    delete main_proactor_;
    main_proactor_ = nullptr;
  }

  void Start() {
    LOG(INFO) << "Starting RedisServer...";
    isRuning = true;
    pool_.AsyncLoop();
    sleep(1);
    shard_set = new EngineShardSet(&pool_);
    shard_set->Init(pool_.size());

    if (enable_rdb_) {
      if (mkdir(data_dir_.c_str(), 0755) != 0 && errno != EEXIST) {
        LOG(ERROR) << "Failed to create rdb dir " << data_dir_ << ": "
                   << strerror(errno);
      }
      LoadPersistentData();
      StartPeriodicSnapshot();
    }

    main_proactor_->DispatchBrief([this] {
      LOG(INFO) << "Starting ListenSocket...";
      listen();
    });
    main_proactor_->Run();
  }

  void Stop() {
    if (shard_set) {
      shard_set->Shutdown();
    }
    pool_.stop();
    main_proactor_->Shutdown();
    isRuning = false;
  }

  void SaveAndStop() {
    if (enable_rdb_ && isRuning && shard_set) {
      std::string dir = data_dir_;
      shard_set->RunBlockingInParallel([&dir](EngineShard* es) {
        RdbSerializer::SaveShard(es->shard_id(), dir);
      });
    }
    Stop();
  }

  std::string_view data_dir() const { return data_dir_; }

  base::UringProactor* MainProactor() { return main_proactor_; }

 private:
  // 构造函数私有：单例模式，必须通过 Init() 创建实例。
  RedisServer(int listenFd, uint32_t size, bool enable_rdb = true,
              const util::JsonConfig* config = nullptr)
      : main_proactor_(
            new base::UringProactor(CreateOptimizedRedisConfig(config))),
        pool_(size, CreateOptimizedRedisConfig(config)),
        ListenSocket_(main_proactor_, listenFd),
        enable_rdb_(config ? config->GetBool("enable_rdb", enable_rdb)
                           : enable_rdb),
        data_dir_(config ? config->GetString("data_dir", "./.rdb") : "./.rdb") {
    CIs = new CommandRegistry();
    RegisterStringFamily(CIs);
    RegisterGeneric(CIs);
    // RegisterMulti(CIs);
    RegisterListFamily(CIs);
    RegisterHashFamily(CIs);
    RegisterSetFamily(CIs);
    RegisterZSetFamily(CIs);
  }

  void LoadPersistentData() {
    std::atomic_bool rdb_ok{true};
    std::string dir = data_dir_;
    shard_set->RunBlockingInParallel([&rdb_ok, &dir](EngineShard* es) {
      if (!RdbSerializer::LoadShard(es->shard_id(), dir)) {
        rdb_ok.store(false);
      }
    });
    if (!rdb_ok.load()) {
      LOG(ERROR)
          << "RDB load failed: refusing to load, starting with empty data";
    }
  }

  void StartPeriodicSnapshot() {
    // 在 shard 0 线程上运行定时快照，保证 Transaction 的 BlockingCounter 协程
    // 控制器在创建协程的分片线程上挂起/恢复，避免跨线程恢复。
    shard_set->Add(0, [] {
      auto snapshot_task = []() -> cppcoro::AsyncTask {
        auto* cid = CIs->Find("SAVE");
        ReplyBuilder rb;
        rb.SetSendCallback([](std::vector<std::string>&&) {});
        auto proactor = shard_set->pool()->at(0);
        while (true) {
          co_await proactor->ArmPeriodicTimer(kSnapshotIntervalMs);
          util::intrusive_ptr<Transaction> txn{new Transaction(cid)};
          std::vector<std::string_view> save_args = {"SAVE"};
          txn->InitByArgs(&namespaces->GetDefaultNamespace(), 0,
                          ::cmn::CmdArgList(save_args));
          CommandContext cmd_cntx(txn, cid, &rb);
          co_await cid->Invoke(&cmd_cntx, ::cmn::CmdArgList(save_args));
          rb.Flush();
        }
        co_return;
      };
      snapshot_task();
    });
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

  auto NextProactor() -> base::UringProactor* {
    NextProIndex_ = (NextProIndex_ + 1) % pool_.size();
    return pool_[NextProIndex_];
  }

  ssize_t NextProIndex_ = 0;
  base::UringProactor* main_proactor_ = nullptr;
  base::UringProactorPool pool_;
  base::UringSocket ListenSocket_;
  bool isRuning = false;

  bool enable_rdb_{true};
  std::string data_dir_{"./.rdb"};
  static constexpr uint64_t kSnapshotIntervalMs = 3'000;

  // 单例实例指针：Init 时赋值，Destroy 时清空。
  inline static RedisServer* instance_ = nullptr;
};

}  // namespace dfly
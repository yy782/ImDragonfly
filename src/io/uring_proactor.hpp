#pragma once

#include <liburing.h>
#include <liburing/io_uring.h>
#include <sys/socket.h>
#include <sys/uio.h>

#include <coroutine>
#include <cstdint>
#include <memory>
#include <vector>

#include "detail/task_queue.hpp"
#include "util/cppcoro/async_task.hpp"

namespace base {

struct UringConfig {
  int queue_depth = 256;
  bool use_defer_taskrun = true;
  bool use_single_issuer = true;
  bool use_sqpoll = false;
  uint32_t sqpoll_idle_ms = 1000;
  int registered_buf_count = 256;
  // 这里应该要应用层自己感知，一个事件循环只能注册那么多连接，需要多少缓冲区应用层决定
  int registered_buf_size = 65536;
  int cqe_batch_size = 32;
  int task_queue_size = 1024;
  uint32_t sqe_batch_size = 32;
  int poll_timeout_ms = 1;  // Run() 每轮 PollOnce 的等待超时
  int poll_min_cqe = 1;  // Run() 每轮 PollOnce 期望至少完成的 CQE 数
};

class UringProactor;

struct IoCompletionSlot {
  std::coroutine_handle<> coro;
  int32_t result = 0;
  uint32_t flags = 0;
  // ArmPeriodicTimer 用：io_uring_prep_timeout 只记录 &timespec，内核在超时
  // 到期时才读它，因此该结构不能是调用方的栈变量 —— 必须活到 CQE 到达。
  // 挂在 slot 上，随 slot 的生命周期存活。
  __kernel_timespec timeout_ts{};
};

class IoAwaitable {
 public:
  IoAwaitable(UringProactor* p, uint32_t idx) noexcept
      : proactor_(p), slot_idx_(idx) {}

  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> h) noexcept;
  int await_resume() noexcept;

 protected:
  UringProactor* proactor() const noexcept { return proactor_; }
  uint32_t slot_idx() const noexcept { return slot_idx_; }

 private:
  UringProactor* proactor_;
  uint32_t slot_idx_;
};

struct RecvResult {
  int bytes;
  const char* data;
  int buf_index;
};

class RecvAwaitable : public IoAwaitable {
 public:
  using IoAwaitable::IoAwaitable;
  RecvResult await_resume() noexcept;
};

class AcceptAwaitable : public IoAwaitable {
 public:
  using IoAwaitable::IoAwaitable;
};

class UringProactor {
 public:
  explicit UringProactor(UringConfig cfg = {}, int pool_index = -1);
  ~UringProactor();

  UringProactor(const UringProactor&) = delete;
  UringProactor& operator=(const UringProactor&) = delete;
  UringProactor(UringProactor&&) = delete;
  UringProactor& operator=(UringProactor&&) = delete;

  AcceptAwaitable AsyncAccept(int listen_fd);
  RecvAwaitable AsyncRecvFixed(int fd, int buf_idx, size_t offset = 0);
  IoAwaitable AsyncSend(int fd, const void* buf, size_t len);

  // 主动连接（raft peer 用）。addr 必须活到 co_await 返回（sqe 只记指针）。
  IoAwaitable AsyncConnect(int fd, const struct sockaddr* addr,
                           socklen_t addrlen);

  // 普通 recv，不走注册缓冲区。raft peer 的连接数很少（就几个），
  // 不值得占用 registered_buf_count 的配额（那是给客户端连接的）。
  IoAwaitable AsyncRecv(int fd, void* buf, size_t len);

  IoAwaitable AsyncSendV(int fd, const struct msghdr* msg);

  // 文件 io（raft 日志落盘用）。AsyncSend 是 prep_send，只能用于 socket。
  // buf 必须活到 co_await 返回（sqe 只记指针）。
  IoAwaitable AsyncWriteFile(int fd, const void* buf, size_t len,
                             uint64_t offset);
  IoAwaitable AsyncWriteFileV(int fd, const struct iovec* iov, unsigned nr_vecs,
                              uint64_t offset);
  IoAwaitable AsyncFsync(int fd, bool datasync = true);

  IoAwaitable ArmPeriodicTimer(uint64_t interval_ms);

  int PollOnce(unsigned min_cqe = 1, unsigned timeout_ms = 0);
  void Run();
  void Shutdown() noexcept;

  void Wake() noexcept;

  dfly::TaskQueue& GetTaskQueue() { return task_queue_; }

  pthread_t GetLoopThreadId() const { return loop_thread_id_; }
  int GetPoolIndex() const { return pool_index_; }

 private:
  friend class IoAwaitable;
  friend class RecvAwaitable;
  friend class AcceptAwaitable;
  friend class UringSocket;

  void InitRing();
  void InitRegisteredBuffers();
  void ArmWakePoll();
  cppcoro::AsyncTask WakeLoop();
  IoAwaitable AsyncPoll(int fd, unsigned poll_mask);

  uint32_t AllocSlot();
  void FreeSlot(uint32_t slot_idx);
  IoCompletionSlot& GetSlot(uint32_t idx) { return pending_slots_[idx].slot; }
  void ResumeSlot(uint32_t slot_idx, int32_t result, int32_t extra = 0);

  struct io_uring_sqe* GetSqeOrFlush();
  void SubmitIfNeeded();

  void ProcessCqe(struct io_uring_cqe* cqe);

  int AcquireRegBuf();
  void ReleaseRegBuf(int index);

  struct io_uring ring_;
  UringConfig config_;
  int pool_index_ = -1;
  struct IoCompletionNode {
    IoCompletionSlot slot;
    uint32_t next = -1;
  };
  std::vector<IoCompletionNode> pending_slots_;
  int32_t next_free_IoCompletionNode_ = 0;
  struct RegBufSlot {
    char* memory;
    int next = -1;
  };
  std::vector<RegBufSlot> reg_bufs_;
  int next_buf_ = 0;
  dfly::TaskQueue task_queue_;
  pthread_t
      loop_thread_id_;  // TODO 多余，应该用分片ID检查检查状态而不是线程ID检查
  bool shutdown_{false};
  uint32_t pending_sqes_{0};

  int wake_fd_ = -1;
};

}  // namespace base

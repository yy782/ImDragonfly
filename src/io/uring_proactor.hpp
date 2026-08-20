#pragma once

#include <liburing.h>
#include <liburing/io_uring.h>
#include <sys/socket.h>
#include <sys/uio.h>

#include <coroutine>
#include <cstdint>
#include <memory>
#include <vector>

#include "util/task_queue.hpp"

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
  // 批量发送：一次 sendmsg 写多个不连续缓冲（零拷贝聚散写）。
  // msg 由调用方持有并保证存活到 CQE 完成（io_uring 异步读取 msghdr/iovec/
  // 数据），典型场景为调用方协程帧内的局部变量（挂起期间帧保活）。
  IoAwaitable AsyncSendV(int fd, const struct msghdr* msg);

  // 协程式一次性超时：co_await 返回的 IoAwaitable 时注册一次 IORING_TIMEOUT，
  // CQE 到点走通用 slot 恢复路线（generation 校验 + resume 协程）。每次
  // co_await 注册一次，周期性由调用方循环实现。必须在本 proactor 的事件
  // 循环线程上调用（SINGLE_ISSUER 语义），协程被 CQE resume 后自然满足。
  IoAwaitable ArmPeriodicTimer(uint64_t interval_ms);

  int PollOnce(unsigned min_cqe = 1, unsigned timeout_ms = 0);
  void Run();
  void Shutdown() noexcept;

  util::TaskQueue& GetTaskQueue() { return task_queue_; }

  template <typename F>
  bool DispatchBrief(F&& f) {
    return task_queue_.TryAdd(std::forward<F>(f));
  }

  pthread_t GetLoopThreadId() const { return loop_thread_id_; }
  int GetPoolIndex() const { return pool_index_; }

 private:
  friend class IoAwaitable;
  friend class RecvAwaitable;
  friend class AcceptAwaitable;
  friend class UringSocket;

  void InitRing();
  void InitRegisteredBuffers();

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
  util::TaskQueue task_queue_;
  pthread_t
      loop_thread_id_;  // TODO 多余，应该用分片ID检查检查状态而不是线程ID检查
  bool shutdown_{false};
  uint32_t pending_sqes_{0};
};

// UringProactor 生命周期覆盖整个程序期间（由 UringProactorPool 持有到 stop），
// 全局使用裸指针避免 shared_ptr 的原子引用计数开销。
// 使用约束：proactor 必须存活到所有引用它的 UringSocket / RedisSession
// 释放之后。

}  // namespace base

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
  bool use_registered_bufs = true;
  // 为减少代码复杂度简化设计了，这里false就UB，目前实现只能使用缓冲区注册，
  // todo 删了 bool use_registered_bufs 直接走true分支
  // 这是一个事务调度和存储为核心的作品，网络层要完整实现要写网络库了，目前的代码量已经很大了
  int registered_buf_count = 256;
  // 这里应该要应用层自己感知，一个事件循环只能注册那么多连接，需要多少缓冲区应用层决定
  int registered_buf_size = 65536;
  int cqe_batch_size = 32;
  int task_queue_size = 1024;
  uint32_t sqe_batch_size = 32;
  int poll_timeout_ms = 1;  // Run() 每轮 PollOnce 的等待超时
};

class UringProactor;

struct IoCompletionSlot {
  std::coroutine_handle<> coro;
  int32_t result = 0;
  uint32_t flags = 0;
  // 代际号：每次 AllocSlot 复用时递增。user_data 编码 (gen << slot_idx_bits_) |
  // idx，ProcessCqe 据此丢弃"slot 已复用后迟到的旧 CQE"（幽灵 CQE），避免误
  // resume 新操作协程或访问已清空 coro 的 slot。
  uint32_t generation = 0;
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
  IoCompletionSlot& GetSlot(uint32_t idx) { return pending_slots_[idx]; }
  void ResumeSlot(uint32_t slot_idx, int32_t result, int32_t extra = 0);

  struct io_uring_sqe* GetSqeOrFlush();
  void SubmitIfNeeded();

  void ProcessCqe(struct io_uring_cqe* cqe);

  int AcquireRegBuf();
  void ReleaseRegBuf(int index);

  struct io_uring ring_;
  UringConfig config_;
  int pool_index_ = -1;
  size_t MaxPendingSlots_;
  std::vector<IoCompletionSlot> pending_slots_;
  // 空闲 slot 池：slot 只通过"CQE 完成 → ResumeSlot → FreeSlot"归还，
  // 绝不循环覆盖在途操作，因此同一 slot 上不可能有多个未完成操作，
  // 幽灵 CQE（slot 复用后迟到的旧 CQE）在结构上不可能出现。
  std::vector<uint32_t> free_slots_;
  uint32_t slot_mask_;
  uint32_t slot_idx_bits_ =
      0;  // slot 索引占用的位数，user_data 高位移 generation
  struct RegBufSlot {
    char* memory;
    int next = -1;
  };
  std::vector<RegBufSlot> reg_bufs_;
  int next_buf_ = 0;
  util::TaskQueue task_queue_;
  pthread_t loop_thread_id_;
  bool shutdown_{false};
  uint32_t pending_sqes_{0};
  uint32_t SqeBatchSize_;
  uint32_t reg_buf_count_;
};

using UringProactorPtr = std::shared_ptr<UringProactor>;

}  // namespace base

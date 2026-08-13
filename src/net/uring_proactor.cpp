#include "uring_proactor.hpp"

#include <glog/logging.h>
#include <sys/socket.h>

#include <cerrno>
#include <cstring>
#include <exception>

namespace base {

void IoAwaitable::await_suspend(std::coroutine_handle<> h) noexcept {
  proactor_->GetSlot(slot_idx_).coro = h;
  proactor_->SubmitIfNeeded();
}

int IoAwaitable::await_resume() noexcept {
  return proactor_->GetSlot(slot_idx_).result;
}

RecvResult RecvAwaitable::await_resume() noexcept {
  auto& slot = proactor()->GetSlot(slot_idx());
  RecvResult r;
  r.bytes = slot.result;
  r.buf_index = static_cast<int32_t>(slot.flags);
  if (r.buf_index < 0 ||
      r.buf_index >= static_cast<int>(proactor()->reg_bufs_.size())) {
    r.data = "";
    return r;
  }
  r.data = proactor()->reg_bufs_[r.buf_index].memory;
  return r;
}

UringProactor::UringProactor(UringConfig cfg, int pool_index)
    : config_(cfg),
      pool_index_(pool_index),
      task_queue_(config_.task_queue_size) {
  MaxPendingSlots_ = cfg.queue_depth;
  SqeBatchSize_ = cfg.sqe_batch_size;
  size_t slots = 1;
  while (slots < MaxPendingSlots_) slots <<= 1;
  if (slots > MaxPendingSlots_) slots = MaxPendingSlots_;
  slot_mask_ = static_cast<uint32_t>(slots - 1);

  // 计算 slot 索引占用位数（如 4096 -> 12），user_data 高位移 generation
  uint32_t m = slot_mask_;
  slot_idx_bits_ = 0;
  while (m) {
    ++slot_idx_bits_;
    m >>= 1;
  }

  pending_slots_.resize(slots);
  free_slots_.reserve(slots);
  for (uint32_t i = 0; i < slots; ++i) {
    free_slots_.push_back(i);
  }

  InitRing();

  if (config_.use_registered_bufs) {
    InitRegisteredBuffers();
  }
}

UringProactor::~UringProactor() {
  Shutdown();

  if (!reg_bufs_.empty()) {
    io_uring_unregister_buffers(&ring_);
    for (auto& slot : reg_bufs_) {
      delete[] slot.memory;
    }
  }

  io_uring_queue_exit(&ring_);
}

void UringProactor::InitRing() {
  struct io_uring_params params;
  std::memset(&params, 0, sizeof(params));

  if (config_.use_defer_taskrun) {
    params.flags |= IORING_SETUP_DEFER_TASKRUN;
  }
  if (config_.use_single_issuer) {
    params.flags |= IORING_SETUP_SINGLE_ISSUER;
  }
  if (config_.use_sqpoll) {
    params.flags |= IORING_SETUP_SQPOLL;
    params.sq_thread_idle = config_.sqpoll_idle_ms;
  }

  int ret = io_uring_queue_init_params(config_.queue_depth, &ring_, &params);
  if (ret < 0) {
    if (config_.use_sqpoll) {
      LOG(WARNING) << "SQPOLL not supported, falling back to interrupt mode";
      params.flags &= ~IORING_SETUP_SQPOLL;
      config_.use_sqpoll = false;
      ret = io_uring_queue_init_params(config_.queue_depth, &ring_, &params);
    }
  }
  CHECK_GE(ret, 0) << "io_uring_queue_init_params failed: " << -ret;

  LOG(INFO) << "Proactor ring: depth=" << config_.queue_depth
            << " defer_tw=" << config_.use_defer_taskrun
            << " single_issuer=" << config_.use_single_issuer
            << " sqpoll=" << config_.use_sqpoll
            << " reg_bufs=" << config_.use_registered_bufs;
}

void UringProactor::InitRegisteredBuffers() {
  reg_buf_count_ = static_cast<uint32_t>(config_.registered_buf_count);
  reg_bufs_.resize(reg_buf_count_);

  std::vector<struct iovec> iovecs;
  iovecs.reserve(reg_buf_count_);

  for (uint32_t i = 0; i < reg_buf_count_; ++i) {
    auto& slot = reg_bufs_[i];
    slot.memory = new char[config_.registered_buf_size];
    slot.next = i + 1;
    struct iovec iov;
    iov.iov_base = slot.memory;
    iov.iov_len = static_cast<size_t>(config_.registered_buf_size);
    iovecs.push_back(iov);
  }
  reg_bufs_[reg_buf_count_ - 1].next = -1;

  int ret = io_uring_register_buffers(&ring_, iovecs.data(), iovecs.size());
  if (ret < 0) {
    LOG(WARNING) << "io_uring_register_buffers failed: " << -ret
                 << ", falling back to standard recv";
    config_.use_registered_bufs = false;
    reg_bufs_.clear();
    reg_buf_count_ = 0;
  } else {
    LOG(INFO) << "Registered " << reg_buf_count_ << " fixed buffers ("
              << config_.registered_buf_size << "B each)";
  }
}

uint32_t UringProactor::AllocSlot() {
  // slot 只来自"已完成 CQE"归还的空闲池，绝不循环覆盖在途操作。
  // 池空 = 在途操作数已达上限（正常压测不可能，256 连接 × 2 在途 << 4096），
  // 视为编程错误立即暴露。
  CHECK(!free_slots_.empty())
      << "io_uring slot pool exhausted: in-flight ops = " << MaxPendingSlots_;
  uint32_t idx = free_slots_.back();
  free_slots_.pop_back();
  auto& slot = pending_slots_[idx];
  slot.coro = nullptr;
  slot.result = -ECANCELED;
  slot.flags = -1;
  ++slot.generation;
  // user_data 编码：(generation << slot_idx_bits_) | idx，
  // ProcessCqe 用 generation 识别"slot 复用后迟到的旧 CQE"并丢弃（防御性）
  return (slot.generation << slot_idx_bits_) | idx;
}

void UringProactor::FreeSlot(uint32_t slot_idx) {
  free_slots_.push_back(slot_idx);
}

void UringProactor::ResumeSlot(uint32_t slot_idx, int32_t result,
                               int32_t extra) {
  auto& slot = pending_slots_[slot_idx];
  slot.result = result;
  slot.flags = extra;
  assert(slot.coro);
  if (slot.coro) {
    try {
      slot.coro.resume();  // 同步执行到协程下次挂起/结束
    } catch (const std::exception& e) {
      // 协程体内异常（如命令回调）不能让 slot 泄漏：否则空闲池被破坏，
      // 后续 AllocSlot 可能重复分配/耗尽。
      LOG(ERROR) << "coroutine threw: slot=" << slot_idx
                 << " what=" << e.what();
    } catch (...) {
      LOG(ERROR) << "coroutine threw (non-std): slot=" << slot_idx;
    }
  }
  slot.coro = nullptr;
  // CQE 已处理、协程已恢复后才归还 slot，之后才能被复用。
  // resume() 内部若立即发起新操作，本 slot 尚未入池，不会被同一协程复用，
  // 因此不存在重复分配或覆盖未读 result 的竞态（单线程同步 resume）。
  FreeSlot(slot_idx);
}

struct io_uring_sqe* UringProactor::GetSqeOrFlush() {
  struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (__builtin_expect(sqe != nullptr, 1)) {
    return sqe;
  }

  io_uring_submit(&ring_);
  pending_sqes_ = 0;
  return io_uring_get_sqe(&ring_);
}

void UringProactor::SubmitIfNeeded() {
  uint32_t prev = pending_sqes_++;
  if (prev + 1 >= SqeBatchSize_) {
    io_uring_submit(&ring_);
    pending_sqes_ = 0;
  }
}

AcceptAwaitable UringProactor::AsyncAccept(int listen_fd) {
  uint32_t ud = AllocSlot();
  uint32_t slot_idx = ud & slot_mask_;

  struct io_uring_sqe* sqe = GetSqeOrFlush();
  io_uring_prep_accept(sqe, listen_fd, nullptr, nullptr,
                       SOCK_NONBLOCK | SOCK_CLOEXEC);
  sqe->user_data = ud;

  return AcceptAwaitable(this, slot_idx);
}

RecvAwaitable UringProactor::AsyncRecvFixed(int fd, int buf_idx,
                                            size_t offset) {
  uint32_t ud = AllocSlot();
  uint32_t slot_idx = ud & slot_mask_;
  struct io_uring_sqe* sqe = GetSqeOrFlush();
  io_uring_prep_read_fixed(
      sqe, fd, reg_bufs_[buf_idx].memory + offset,
      static_cast<unsigned>(config_.registered_buf_size - offset), 0, buf_idx);
  sqe->user_data = ud;
  GetSlot(slot_idx).flags = buf_idx;
  return RecvAwaitable(this, slot_idx);
}

IoAwaitable UringProactor::AsyncSend(int fd, const void* buf, size_t len) {
  uint32_t ud = AllocSlot();
  uint32_t slot_idx = ud & slot_mask_;

  struct io_uring_sqe* sqe = GetSqeOrFlush();
  io_uring_prep_send(sqe, fd, buf, len, MSG_NOSIGNAL);
  sqe->user_data = ud;
  return IoAwaitable(this, slot_idx);
}

IoAwaitable UringProactor::AsyncSendV(int fd, const struct msghdr* msg) {
  CHECK(msg != nullptr && msg->msg_iovlen > 0);
  uint32_t ud = AllocSlot();
  uint32_t slot_idx = ud & slot_mask_;

  struct io_uring_sqe* sqe = GetSqeOrFlush();
  // MSG_NOSIGNAL：对端关闭时返回 EPIPE 而非发 SIGPIPE 杀进程。
  // prep_sendmsg 仅把 msg 指针存入 SQE，内核在提交后异步读取，因此
  // msg 及其 iovec/数据必须由调用方保活到 CQE 完成。
  io_uring_prep_sendmsg(sqe, fd, msg, MSG_NOSIGNAL);
  sqe->user_data = ud;
  return IoAwaitable(this, slot_idx);
}

int UringProactor::AcquireRegBuf() {
  int re = next_buf_;
  if (re < 0 || re >= static_cast<int>(reg_bufs_.size())) {
    return re;
  }
  auto& slot = reg_bufs_[re];
  next_buf_ = slot.next;
  return re;
}

void UringProactor::ReleaseRegBuf(int index) {
  if (index < 0 || index >= static_cast<int>(reg_bufs_.size())) {
    return;
  }
  auto& slot = reg_bufs_[index];
  slot.next = next_buf_;
  next_buf_ = index;
}

void UringProactor::ProcessCqe(struct io_uring_cqe* cqe) {
  // liburing 内部超时事件（user_data = LIBURING_UDATA_TIMEOUT）没有对应
  // slot，直接丢弃，避免被当成 slot 索引导致越界访问/崩溃。
  if (cqe->user_data == LIBURING_UDATA_TIMEOUT) {
    return;
  }
  uint32_t ud = static_cast<uint32_t>(cqe->user_data);
  uint32_t slot_idx = ud & slot_mask_;
  uint32_t gen = ud >> slot_idx_bits_;
  auto& slot = GetSlot(slot_idx);

  // 防御层：代际不匹配的迟到 CQE（幽灵 CQE）直接丢弃。
  // 空闲 slot 池下同一 slot 同一时刻只属于一个在途操作，正常不会触发。
  if (slot.generation != gen) {
    return;
  }
  if (!slot.coro) {
    return;
  }

  ResumeSlot(slot_idx, cqe->res, slot.flags);
}

int UringProactor::PollOnce(unsigned min_cqe, unsigned timeout_ms) {
  unsigned pending = pending_sqes_;
  pending_sqes_ = 0;

  struct __kernel_timespec ts;
  struct __kernel_timespec* pts = nullptr;

  if (timeout_ms > 0) {
    ts.tv_sec = timeout_ms / 1000;
    ts.tv_nsec = (timeout_ms % 1000) * 1000000UL;
    pts = &ts;
  }

  int wait_nr = (pending > 0 || min_cqe > 0) ? static_cast<int>(min_cqe) : 0;
  int submitted;
  struct io_uring_cqe* timeout_cqe = nullptr;
  submitted = io_uring_submit_and_wait_timeout(
      &ring_, &timeout_cqe, static_cast<unsigned>(wait_nr), pts, nullptr);
  if (submitted < 0) {
    int err = -submitted;
    if (err != ETIME && err != EINTR) {
      return -err;
    }
    submitted = 0;
  }
  unsigned processed = 0;

  if (timeout_cqe != nullptr) {
    ProcessCqe(timeout_cqe);
    io_uring_cqe_seen(&ring_, timeout_cqe);
    ++processed;
  }

  struct io_uring_cqe* cqe = nullptr;
  unsigned head;
  unsigned batch_count = 0;

  io_uring_for_each_cqe(&ring_, head, cqe) {
    ProcessCqe(cqe);
    ++processed;
    ++batch_count;

    if (batch_count >= config_.cqe_batch_size) {
      io_uring_cq_advance(&ring_, batch_count);
      batch_count = 0;
    }
  }
  if (batch_count > 0) {
    io_uring_cq_advance(&ring_, batch_count);
  }

  return static_cast<int>(processed);
}

void UringProactor::Run() {
  loop_thread_id_ = pthread_self();

  while (!shutdown_) {
    task_queue_.TryDrain();

    int processed = PollOnce(
        1, config_.poll_timeout_ms);  // todo 这里的 1 应该外面决定，用config_

    if (processed < 0) {
      LOG(ERROR) << "Proactor poll error: " << -processed;
      if (processed == -ENOMEM) {
        break;
      }
    }
  }

  task_queue_.TryDrain();
  PollOnce(0, 0);
}

void UringProactor::Shutdown() noexcept {
  DispatchBrief([this] { shutdown_ = true; });
  task_queue_.Shutdown();
}

}  // namespace base

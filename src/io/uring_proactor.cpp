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
  r.data = proactor()->reg_bufs_[r.buf_index].memory;
  return r;
}

UringProactor::UringProactor(UringConfig cfg, int pool_index)
    : config_(cfg),
      pool_index_(pool_index),
      task_queue_(config_.task_queue_size) {
  size_t slots = static_cast<size_t>(config_.queue_depth);

  pending_slots_.resize(slots);
  // 使用侵入式链表组织空闲槽：pending_slots_[i].next 串起空闲表，
  // next_free_IoCompletionNode_ 为链表头（空表时为 -1）。
  for (uint32_t i = 0; i < slots; ++i) {
    pending_slots_[i].next = i + 1;
  }
  pending_slots_[slots - 1].next = static_cast<uint32_t>(-1);
  next_free_IoCompletionNode_ = 0;

  InitRing();

  InitRegisteredBuffers();
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
            << " sqpoll=" << config_.use_sqpoll;
}

void UringProactor::InitRegisteredBuffers() {
  const uint32_t count = static_cast<uint32_t>(config_.registered_buf_count);
  reg_bufs_.resize(count);

  std::vector<struct iovec> iovecs;
  iovecs.reserve(count);

  for (uint32_t i = 0; i < count; ++i) {
    auto& slot = reg_bufs_[i];
    slot.memory = new char[config_.registered_buf_size];
    slot.next = i + 1;
    struct iovec iov;
    iov.iov_base = slot.memory;
    iov.iov_len = static_cast<size_t>(config_.registered_buf_size);
    iovecs.push_back(iov);
  }
  reg_bufs_[count - 1].next = -1;

  int ret = io_uring_register_buffers(&ring_, iovecs.data(), iovecs.size());
  // 读路径固定走 AsyncRecvFixed（直接索引 reg_bufs_），没有标准 recv 回退，
  // 注册失败会让后续读取越界，故直接失败而非回退。
  CHECK_GE(ret, 0) << "io_uring_register_buffers failed: " << -ret;
  LOG(INFO) << "Registered " << config_.registered_buf_count
            << " fixed buffers (" << config_.registered_buf_size << "B each)";
}

uint32_t UringProactor::AllocSlot() {
  DCHECK(next_free_IoCompletionNode_ >= 0);
  uint32_t idx = static_cast<uint32_t>(next_free_IoCompletionNode_);
  DCHECK(idx != static_cast<uint32_t>(-1))
      << "io_uring slot pool exhausted: in-flight ops = "
      << config_.queue_depth;
  auto& node = pending_slots_[idx];
  // 摘下链表头：头指针前进到 node.next
  next_free_IoCompletionNode_ = static_cast<int32_t>(node.next);
  node.next = static_cast<uint32_t>(
      -1);  // 已分配节点的 next 置为哨兵，便于调试检测重复归还。
  return idx;
}

void UringProactor::FreeSlot(uint32_t slot_idx) {
  DCHECK(slot_idx < pending_slots_.size());
  auto& node = pending_slots_[slot_idx];
  DCHECK(node.next == static_cast<uint32_t>(-1))
      << "Double FreeSlot detected: slot_idx=" << slot_idx;
  // 头插法归还到空闲链表
  node.next = static_cast<uint32_t>(next_free_IoCompletionNode_);
  next_free_IoCompletionNode_ = static_cast<int32_t>(slot_idx);
}

void UringProactor::ResumeSlot(uint32_t slot_idx, int32_t result,
                               int32_t extra) {
  auto& slot = GetSlot(slot_idx);
  slot.result = result;
  slot.flags = extra;
  DCHECK(slot.coro);
  slot.coro.resume();
  slot.coro = nullptr;
  slot.result = 0;
  slot.flags = 0;
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
  if (prev + 1 >= config_.sqe_batch_size) {
    io_uring_submit(&ring_);
    pending_sqes_ = 0;
  }
}

AcceptAwaitable UringProactor::AsyncAccept(int listen_fd) {
  uint32_t slot_idx = AllocSlot();

  struct io_uring_sqe* sqe = GetSqeOrFlush();
  io_uring_prep_accept(sqe, listen_fd, nullptr, nullptr,
                       SOCK_NONBLOCK | SOCK_CLOEXEC);
  sqe->user_data = slot_idx;

  return AcceptAwaitable(this, slot_idx);
}

RecvAwaitable UringProactor::AsyncRecvFixed(int fd, int buf_idx,
                                            size_t offset) {
  uint32_t slot_idx = AllocSlot();
  struct io_uring_sqe* sqe = GetSqeOrFlush();
  io_uring_prep_read_fixed(
      sqe, fd, reg_bufs_[buf_idx].memory + offset,
      static_cast<unsigned>(config_.registered_buf_size - offset), 0, buf_idx);
  sqe->user_data = slot_idx;
  GetSlot(slot_idx).flags = buf_idx;
  return RecvAwaitable(this, slot_idx);
}

IoAwaitable UringProactor::AsyncSend(int fd, const void* buf, size_t len) {
  uint32_t slot_idx = AllocSlot();

  struct io_uring_sqe* sqe = GetSqeOrFlush();
  io_uring_prep_send(sqe, fd, buf, len, MSG_NOSIGNAL);
  sqe->user_data = slot_idx;
  return IoAwaitable(this, slot_idx);
}

IoAwaitable UringProactor::AsyncSendV(int fd, const struct msghdr* msg) {
  DCHECK(msg != nullptr && msg->msg_iovlen > 0);
  uint32_t slot_idx = AllocSlot();

  struct io_uring_sqe* sqe = GetSqeOrFlush();
  // MSG_NOSIGNAL：对端关闭时返回 EPIPE 而非发 SIGPIPE 杀进程。
  // prep_sendmsg 仅把 msg 指针存入 SQE，内核在提交后异步读取，因此
  // msg 及其 iovec/数据必须由调用方保活到 CQE 完成。
  io_uring_prep_sendmsg(sqe, fd, msg, MSG_NOSIGNAL);
  sqe->user_data = slot_idx;
  return IoAwaitable(this, slot_idx);
}

IoAwaitable UringProactor::ArmPeriodicTimer(uint64_t interval_ms) {
  uint32_t slot_idx = AllocSlot();

  struct io_uring_sqe* sqe = GetSqeOrFlush();
  __kernel_timespec ts{
      static_cast<__kernel_time64_t>(interval_ms / 1000),
      static_cast<__kernel_time64_t>(interval_ms % 1000) * 1000000};
  io_uring_prep_timeout(sqe, &ts, 0, 0);
  sqe->user_data = slot_idx;
  io_uring_submit(&ring_);
  pending_sqes_ = 0;
  VLOG(3) << "[timer] ArmPeriodicTimer armed slot=" << slot_idx
          << " interval_ms=" << interval_ms;
  return IoAwaitable(this, slot_idx);
}

int UringProactor::AcquireRegBuf() {
  int re = next_buf_;
  auto& slot = reg_bufs_[re];
  next_buf_ = slot.next;
  return re;
}

void UringProactor::ReleaseRegBuf(int index) {
  DCHECK(index >= 0 && index < static_cast<int>(reg_bufs_.size()));
  auto& slot = reg_bufs_[index];
  slot.next = next_buf_;
  next_buf_ = index;
}

void UringProactor::ProcessCqe(struct io_uring_cqe* cqe) {
  uint32_t slot_idx = static_cast<uint32_t>(cqe->user_data);
  auto& slot = GetSlot(slot_idx);
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

    if (batch_count >= static_cast<unsigned>(config_.cqe_batch_size)) {
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

    int processed = PollOnce(static_cast<unsigned>(config_.poll_min_cqe),
                             static_cast<unsigned>(config_.poll_timeout_ms));

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

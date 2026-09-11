#include "raft/raft_peer.hpp"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>

namespace dfly {

namespace {
constexpr size_t kRecvChunk = 64 * 1024;
}

RaftPeer::~RaftPeer() {
  closing_ = true;
  DropConnection();
}

void RaftPeer::Start() {
  if (connecting_) return;
  ConnectLoop();
}

struct RaftPeer::ResponseAwaiter {
  RaftPeer* peer;
  uint64_t seq;
  AppendEntriesResp result{};
  Waiter w;

  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> h) noexcept {
    w.h = h;
    w.out_append = &result;
    peer->waiters_[seq] = &w;
  }
  AppendEntriesResp await_resume() noexcept { return result; }
};

struct RaftPeer::VoteAwaiter {
  RaftPeer* peer;
  uint64_t seq;
  RequestVoteResp result{};
  Waiter w;

  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> h) noexcept {
    w.h = h;
    w.out_vote = &result;
    peer->waiters_[seq] = &w;
  }
  RequestVoteResp await_resume() noexcept { return result; }
};

struct RaftPeer::ReadIndexAwaiter {
  RaftPeer* peer;
  uint64_t seq;
  ReadIndexResp result{};
  Waiter w;

  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> h) noexcept {
    w.h = h;
    w.out_read = &result;
    peer->waiters_[seq] = &w;
  }
  ReadIndexResp await_resume() noexcept { return result; }
};

cppcoro::task<bool> RaftPeer::SendFramed(RaftMsgType type, uint64_t seq,
                                         std::string_view body) {
  send_buf_.clear();
  FrameMessage(&send_buf_, type, seq, body);

  size_t sent = 0;
  while (sent < send_buf_.size()) {
    const int n = co_await proactor_->AsyncSend(fd_, send_buf_.data() + sent,
                                                send_buf_.size() - sent);
    if (n <= 0) {
      LOG(WARNING) << "raft: send to peer " << Describe() << " failed: " << n;
      DropConnection();
      co_return false;
    }
    sent += static_cast<size_t>(n);
  }
  co_return true;
}

cppcoro::task<AppendEntriesResp> RaftPeer::SendAppendEntries(
    std::string_view body, uint64_t timeout_ms) {
  AppendEntriesResp fail{};
  if (fd_ < 0) co_return fail;

  const uint64_t seq = next_seq_++;
  if (!co_await SendFramed(RaftMsgType::kAppendEntries, seq, body))
    co_return fail;

  ResponseAwaiter aw{this, seq};
  TimeoutGuard(seq, timeout_ms);
  co_return co_await aw;
}

cppcoro::task<RequestVoteResp> RaftPeer::SendRequestVote(std::string_view body,
                                                         uint64_t timeout_ms) {
  RequestVoteResp fail{};
  if (fd_ < 0) co_return fail;

  const uint64_t seq = next_seq_++;
  if (!co_await SendFramed(RaftMsgType::kRequestVote, seq, body))
    co_return fail;

  VoteAwaiter aw{this, seq};
  TimeoutGuard(seq, timeout_ms);
  co_return co_await aw;
}

cppcoro::task<ReadIndexResp> RaftPeer::SendReadIndex(uint64_t term,
                                                     uint64_t timeout_ms) {
  ReadIndexResp fail{};
  if (fd_ < 0) co_return fail;

  const uint64_t seq = next_seq_++;
  std::string body;
  PutU64(&body, term);
  if (!co_await SendFramed(RaftMsgType::kReadIndex, seq, body)) co_return fail;

  ReadIndexAwaiter aw{this, seq};
  TimeoutGuard(seq, timeout_ms);
  co_return co_await aw;
}

cppcoro::AsyncTask RaftPeer::TimeoutGuard(uint64_t seq, uint64_t ms) {
  co_await proactor_->ArmPeriodicTimer(ms);
  if (waiters_.count(seq) == 0) co_return;
  LOG(WARNING) << "raft: peer " << Describe() << " seq " << seq << " timed out";
  FailWaiter(seq);
  co_return;
}

void RaftPeer::FailWaiter(uint64_t seq) {
  auto it = waiters_.find(seq);
  if (it == waiters_.end()) return;
  Waiter* w = it->second;
  waiters_.erase(it);
  if (w->out_append) *w->out_append = AppendEntriesResp{};
  if (w->out_vote) *w->out_vote = RequestVoteResp{};
  if (w->h) w->h.resume();
}

cppcoro::AsyncTask RaftPeer::ConnectLoop() {
  connecting_ = true;
  std::memset(&addr_, 0, sizeof(addr_));
  addr_.sin_family = AF_INET;
  addr_.sin_port = htons(port_);
  if (::inet_pton(AF_INET, host_.c_str(), &addr_.sin_addr) != 1) {
    LOG(ERROR) << "raft: bad peer address " << Describe();
    connecting_ = false;
    co_return;
  }

  while (!closing_) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
      LOG(ERROR) << "raft: socket() failed: " << std::strerror(errno);
      co_await proactor_->ArmPeriodicTimer(backoff_ms_);
      continue;
    }

    const int rc = co_await proactor_->AsyncConnect(
        fd, reinterpret_cast<struct sockaddr*>(&addr_), sizeof(addr_));
    if (rc < 0) {
      ::close(fd);
      backoff_ms_ = std::min(backoff_ms_ * 2, kMaxBackoffMs);
      VLOG(1) << "raft: connect to " << Describe() << " failed (" << rc
              << "), retry in " << backoff_ms_ << "ms";
      co_await proactor_->ArmPeriodicTimer(backoff_ms_);
      continue;
    }

    int nodelay = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    fd_ = fd;
    backoff_ms_ = 50;
    LOG(INFO) << "raft: connected to peer " << Describe();
    co_await ReadLoop(fd);
  }
  connecting_ = false;
  co_return;
}

cppcoro::task<> RaftPeer::ReadLoop(int fd) {
  recv_buf_.resize(kRecvChunk);
  recv_len_ = 0;

  while (fd == fd_ && !closing_) {
    if (recv_len_ == recv_buf_.size()) recv_buf_.resize(recv_buf_.size() * 2);

    const int n = co_await proactor_->AsyncRecv(
        fd, recv_buf_.data() + recv_len_, recv_buf_.size() - recv_len_);
    if (n <= 0) {
      LOG(WARNING) << "raft: peer " << Describe() << " read ended: " << n;
      break;
    }
    recv_len_ += static_cast<size_t>(n);

    size_t pos = 0;
    bool proto_ok = true;
    while (recv_len_ - pos >= kFrameHeaderSize) {
      const char* p = recv_buf_.data() + pos;
      const uint32_t len = GetU32(p);
      if (len < 1 + 8 || len - 9 > kMaxFrameBody) {
        LOG(ERROR) << "raft: peer " << Describe() << " bad frame len " << len;
        proto_ok = false;
        break;
      }
      if (recv_len_ - pos < kFrameLenSize + len) break;

      const auto type = static_cast<RaftMsgType>(GetU8(p + 4));
      const uint64_t seq = GetU64(p + 5);
      const uint32_t body_len = len - 9;
      if (!OnFrame(type, seq, p + kFrameHeaderSize, body_len)) {
        proto_ok = false;
        break;
      }
      pos += kFrameLenSize + len;
    }
    if (!proto_ok) break;

    if (pos > 0) {
      std::memmove(recv_buf_.data(), recv_buf_.data() + pos, recv_len_ - pos);
      recv_len_ -= pos;
    }
  }

  if (fd == fd_) DropConnection();
  co_return;
}

bool RaftPeer::OnFrame(RaftMsgType type, uint64_t seq, const char* body,
                       uint32_t len) {
  auto it = waiters_.find(seq);
  if (it == waiters_.end()) {
    VLOG(2) << "raft: peer " << Describe() << " late resp seq " << seq;
    return true;
  }
  Waiter* w = it->second;

  if (type == RaftMsgType::kAppendEntriesResp) {
    if (len < kAppendRespBodySize) {
      LOG(ERROR) << "raft: short AppendEntries resp " << len;
      return false;
    }
    if (!w->out_append) {
      LOG(ERROR) << "raft: resp type mismatch for seq " << seq;
      return false;
    }
    w->out_append->term = GetU64(body);
    w->out_append->success = GetU8(body + 8) != 0;
    w->out_append->conflict_index = GetU64(body + 9);
    w->out_append->match_index = GetU64(body + 17);
  } else if (type == RaftMsgType::kRequestVoteResp) {
    if (len < kVoteRespBodySize) {
      LOG(ERROR) << "raft: short RequestVote resp " << len;
      return false;
    }
    if (!w->out_vote) {
      LOG(ERROR) << "raft: resp type mismatch for seq " << seq;
      return false;
    }
    w->out_vote->term = GetU64(body);
    w->out_vote->granted = GetU8(body + 8) != 0;
  } else if (type == RaftMsgType::kReadIndexResp) {
    if (len < kReadIndexRespBodySize) {
      LOG(ERROR) << "raft: short ReadIndex resp " << len;
      return false;
    }
    if (!w->out_read) {
      LOG(ERROR) << "raft: resp type mismatch for seq " << seq;
      return false;
    }
    w->out_read->term = GetU64(body);
    w->out_read->success = GetU8(body + 8) != 0;
    w->out_read->commit_index = GetU64(body + 9);
  } else {
    LOG(WARNING) << "raft: peer " << Describe() << " unexpected msg type "
                 << static_cast<int>(type);
    return true;
  }

  waiters_.erase(it);
  if (w->h) w->h.resume();
  return true;
}

void RaftPeer::DropConnection() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  while (!waiters_.empty()) FailWaiter(waiters_.begin()->first);
}

}  // namespace dfly

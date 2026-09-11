#pragma once

#include <netinet/in.h>

#include <coroutine>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "io/uring_proactor.hpp"
#include "raft/raft_proto.hpp"
#include "util/cppcoro/async_mutex.hpp"
#include "util/cppcoro/async_task.hpp"
#include "util/cppcoro/task.hpp"

namespace dfly {

class RaftPeer {
 public:
  RaftPeer(base::UringProactor* p, uint32_t id, std::string host, uint16_t port)
      : proactor_(p), id_(id), host_(std::move(host)), port_(port) {}
  ~RaftPeer();

  RaftPeer(const RaftPeer&) = delete;
  RaftPeer& operator=(const RaftPeer&) = delete;

  uint32_t id() const { return id_; }
  bool connected() const { return fd_ >= 0; }
  std::string Describe() const { return host_ + ":" + std::to_string(port_); }

  bool is_learner() const { return learner_; }
  void SetLearner(bool v) { learner_ = v; }

  uint64_t match_index() const { return match_index_; }
  void SetMatchIndex(uint64_t v) { match_index_ = v; }

  void Start();

  cppcoro::task<AppendEntriesResp> SendAppendEntries(std::string_view body,
                                                     uint64_t timeout_ms);

  cppcoro::task<RequestVoteResp> SendRequestVote(std::string_view body,
                                                 uint64_t timeout_ms);

  cppcoro::task<ReadIndexResp> SendReadIndex(uint64_t term,
                                             uint64_t timeout_ms);

  auto LockSend() { return send_mu_.scoped_lock_async(); }

 private:
  cppcoro::AsyncTask ConnectLoop();
  cppcoro::task<> ReadLoop(int fd);
  cppcoro::AsyncTask TimeoutGuard(uint64_t seq, uint64_t ms);

  void DropConnection();
  bool OnFrame(RaftMsgType type, uint64_t seq, const char* body, uint32_t len);
  void FailWaiter(uint64_t seq);

  struct Waiter {
    std::coroutine_handle<> h;
    AppendEntriesResp* out_append = nullptr;
    RequestVoteResp* out_vote = nullptr;
    ReadIndexResp* out_read = nullptr;
  };
  struct ResponseAwaiter;
  struct VoteAwaiter;
  struct ReadIndexAwaiter;

  cppcoro::task<bool> SendFramed(RaftMsgType type, uint64_t seq,
                                 std::string_view body);

  base::UringProactor* proactor_;
  uint32_t id_;
  std::string host_;
  uint16_t port_;

  int fd_ = -1;
  bool connecting_ = false;
  bool closing_ = false;
  bool learner_ = false;
  uint64_t match_index_ = 0;
  uint32_t backoff_ms_ = 50;
  static constexpr uint32_t kMaxBackoffMs = 2000;

  uint64_t next_seq_ = 1;
  std::unordered_map<uint64_t, Waiter*> waiters_;

  cppcoro::async_mutex send_mu_;

  std::string send_buf_;
  std::vector<char> recv_buf_;
  size_t recv_len_ = 0;

  struct sockaddr_in addr_ {};
};

}  // namespace dfly

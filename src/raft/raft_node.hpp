#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "io/uring_proactor.hpp"
#include "raft/raft_inbound.hpp"
#include "raft/raft_log_entry.hpp"
#include "raft/raft_peer.hpp"
#include "util/Time.hpp"
#include "util/cppcoro/async_mutex.hpp"
#include "util/cppcoro/async_task.hpp"
#include "util/cppcoro/task.hpp"

namespace dfly {

enum RaftStatus : uint32_t {
  kRaftNone = 0,
  kRaftDriving = 1u << 0,    // DriveLogs 复制驱动协程在跑
  kRaftReadDrive = 1u << 1,  // ReadIndexDrive 读确认驱动协程在跑
  kRaftReplaying = 1u << 2,  // 启动日志重放阶段
};

inline uint32_t raft_status = kRaftNone;

inline bool HasRaftStatus(RaftStatus bits) {
  return (raft_status & static_cast<uint32_t>(bits)) != 0;
}
inline void SetRaftStatus(RaftStatus bits) {
  raft_status |= static_cast<uint32_t>(bits);
}
inline void ClearRaftStatus(RaftStatus bits) {
  raft_status &= ~static_cast<uint32_t>(bits);
}
inline bool IsRaftReplaying() { return HasRaftStatus(kRaftReplaying); }

struct RaftConfig {
  uint32_t node_id = 0;

  bool is_leader = false;
  std::vector<std::string> peers;
  std::string log_path = "./raft.log";
  uint64_t rpc_timeout_ms = 1000;
  uint64_t election_timeout_ms = 300;
  uint32_t max_entries_per_rpc = 64;
  bool joining = false;
  std::string seed;
  size_t ClusterSize() const { return peers.empty() ? 1 : peers.size(); }
  size_t Quorum() const { return ClusterSize() / 2 + 1; }
};

struct RaftMember {
  uint32_t id = 0;
  std::string host;
  uint16_t port = 0;
  bool learner = true;

  std::string Describe() const { return host + ":" + std::to_string(port); }
};

enum class RaftRole : uint8_t { kFollower, kCandidate, kLeader };
const char* RoleName(RaftRole r);

std::vector<std::string> ParsePeerList(const std::string& csv);
bool SplitHostPort(const std::string& s, std::string* host, uint16_t* port);

struct LogSlot {
  uint64_t index = 0;
  uint64_t term = 0;
  uint64_t start_ms = 0;
  std::string payload;
  uint64_t file_offset = 0;
  uint64_t record_size = 0;
};

class RaftNode {
 public:
  RaftNode(base::UringProactor* main_proactor, RaftConfig cfg)
      : proactor_(main_proactor), cfg_(std::move(cfg)) {}
  ~RaftNode();

  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  bool Open();

  // 建立传输层：每个节点都监听 + 向其它 peer 拨号。Open() 之后调用。
  bool StartTransport();

  // shard 线程投过来的一批日志条目。**只能在 main 线程调用**
  // （由 Shard 侧 Post 到 main 的 TaskQueue 里执行）。
  void SubmitBatch(std::vector<RaftLogEntry>&& batch);

  // follower 侧：Log Matching 检查 → 落盘 → 重放已提交部分。
  cppcoro::task<AppendEntriesResp> HandleAppendEntries(const char* body,
                                                       uint32_t len);

  // 处理一条 RequestVote，返回是否投票给它。
  RequestVoteResp HandleRequestVote(const char* body, uint32_t len);

  bool LeaseReadAllowed() const {
    if (VoterCount() <= 1) return true;
    return util::GetSteadyTimeMs() <
           lease_deadline_ms_.load(std::memory_order_acquire);
  }
  void RequestFollowerRead(TxId txid);

  // leader 侧处理 follower 的 ReadIndex 请求（inbound 调用，main 线程，
  // 同步）：租约有效才回 success + commitIndex。
  ReadIndexResp HandleReadIndex(const char* body, uint32_t len);

  bool is_leader() const {
    return role_.load(std::memory_order_acquire) == RaftRole::kLeader;
  }
  RaftRole role() const { return role_.load(std::memory_order_acquire); }
  uint64_t term() const { return term_; }
  uint64_t last_log_index() const { return last_log_index_; }
  uint64_t commit_index() const { return commit_index_; }
  const RaftConfig& config() const { return cfg_; }

  bool ProposeAddMember(const std::string& host, uint16_t port, uint32_t id,
                        std::string* err);
  // 「RAFT ADDNODE」命令 handler 在 shard 线程提交后调用：投递到 main 上
  // 执行校验 + 把新节点登记为 learner。同步等待结果。返回 false 并把原因
  // 写入 *err 时，命令层回 -ERR。
  bool OnAddNodeCommand(const std::string& host, uint16_t port, uint32_t id,
                        std::string* err);
  std::string DescribeMembers() const;

 private:
  void ApplyAddMember(uint32_t id, const std::string& host, uint16_t port);

  size_t VoterCount() const;

  size_t Quorum() const { return VoterCount() / 2 + 1; }

  void SyncPeersToMembers();

  void MaybePromoteLearners();
  void AppendConfigEntry(const std::string& payload);
  bool IsJoiningLearner() const {
    if (!cfg_.joining) return false;
    for (const RaftMember& m : members_) {
      if (m.id == cfg_.node_id) return m.learner;
    }
    return true;  // 成员表里还没有自己 → 仍是学习者
  }

  uint64_t LastLogIndex() const { return last_log_index_; }
  uint64_t LastLogTerm() const;
  uint64_t TermAt(uint64_t index) const;
  bool HasIndex(uint64_t index) const;
  const LogSlot* SlotAt(uint64_t index) const;
  LogSlot* SlotAt(uint64_t index);

  bool TruncateFrom(uint64_t from);

  cppcoro::AsyncTask DriveLogs();
  cppcoro::task<uint64_t> AppendBatchToLog(std::vector<RaftLogEntry>& batch);
  cppcoro::task<bool> ReplicateToPeer(size_t peer_idx);
  void AdvanceCommitIndex();

  cppcoro::task<bool> AwaitCommit(uint64_t target_index);

  cppcoro::task<bool> WriteRawAndSync(const std::string& bytes,
                                      uint64_t offset);

  bool RecoverFromDisk();
  void ReplayEntry(const LogSlot& slot);
  bool ApplyConfigEntry(const std::vector<std::string_view>& args);

  void ApplyCommitted();

  cppcoro::AsyncTask HeartbeatLoop();
  cppcoro::AsyncTask ElectionLoop();
  cppcoro::task<> RunElection();
  // 一轮心跳的成功 ack 数（不含自己）达到多数派后续租。
  // sent_ms = 本轮心跳**发出**的时刻（用发送时刻而非收 ack 时刻，
  // 论证见 raft_node.cpp）。
  void RenewLease(uint64_t sent_ms, size_t acked_peers);
  // 租约时长。必须满足 心跳间隔 < 租约 < 选举超时：
  // 长于心跳间隔，两轮心跳之间租约不会断档；短于选举超时，
  // 租约期内 follower 不可能超时完成选举。取选举超时的 1/2，
  // 同时为时钟漂移留足余量（同机房漂移远小于 2 倍）。
  uint64_t LeaseMs() const { return cfg_.election_timeout_ms / 2; }
  void BecomeFollower(uint64_t new_term, const char* why);
  void BecomeLeader();
  bool StepDownIfStale(uint64_t peer_term, const char* why);
  uint64_t RandomizedElectionTimeout();

  bool PersistState();
  bool LoadState();

  // 提交失败：把未提交的后缀从日志里截掉（日志回到已提交前缀）。
  // TODO(raft-failure): 被截条目对应事务的终结方式待设计。
  void FailFrom(uint64_t from_index);

  void BroadcastReadyTxids(bool is_read, std::vector<TxId> txids);

  cppcoro::AsyncTask ReadIndexDrive();

  RaftPeer* FindPeer(uint32_t id);

  void EncodeAppendEntries(uint64_t next_index, uint32_t count,
                           std::string* out) const;

  base::UringProactor* proactor_;
  RaftConfig cfg_;

  int fd_ = -1;
  uint64_t file_offset_ = 0;

  uint64_t term_ = 0;

  std::deque<LogSlot> log_;

  std::vector<TxId> log_txids_;
  uint64_t log_start_index_ = 1;  // log_.front().index
  uint64_t last_log_index_ = 0;   // 0 = 空日志
  uint64_t commit_index_ = 0;     // 已知被多数派持久化的最大 index
  uint64_t applied_index_ = 0;    // 已应用到状态机的最大 index

  std::vector<uint64_t> next_index_;
  std::vector<uint64_t> match_index_;

  std::atomic<RaftRole> role_{RaftRole::kFollower};
  static constexpr uint32_t kNoVote = static_cast<uint32_t>(-1);
  static constexpr uint32_t kSeedPeerId = static_cast<uint32_t>(-2);
  uint32_t voted_for_ = kNoVote;
  uint32_t leader_id_ = kNoVote;
  uint64_t last_heartbeat_ms_ = 0;
  std::atomic<uint64_t> lease_deadline_ms_{0};
  std::string state_path_;
  int state_fd_ = -1;

  std::vector<RaftMember> members_;
  std::vector<std::unique_ptr<RaftPeer>> peers_;
  bool add_in_flight_ = false;
  uint32_t add_in_flight_id_ = 0;
  std::unique_ptr<RaftInbound> inbound_;
  int listen_fd_ = -1;
  bool closing_ = false;
  static constexpr uint64_t kHeartbeatMs = 50;
  static constexpr uint64_t kElectionPollMs = 30;
  static constexpr uint64_t kCommitPollMs = 5;
  static constexpr uint64_t kCommitTimeoutMs = 3000;

  std::vector<RaftLogEntry> pending_;

  std::vector<TxId> pending_reads_;

  std::string write_buf_;
  std::string wire_buf_;
  std::string hb_buf_;
  std::string vote_buf_;
  std::string state_buf_;
  std::string inbound_buf_;

  cppcoro::async_mutex inbound_mu_;
};

extern RaftNode* raft_node;

}  // namespace dfly
#include "raft/raft_node.hpp"

#include <fcntl.h>
#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <latch>

#include "command_layer/command_registry.hpp"
#include "detail/conn_context.hpp"
#include "raft/raft_inbound.hpp"
#include "redis/facade/ParseRESP.hpp"
#include "redis/facade/reply_builder.hpp"
#include "server/redis_server.hpp"
#include "sharding/shard_pool.hpp"
#include "util/Strings.hpp"
#include "util/Time.hpp"

namespace dfly {

RaftNode* raft_node = nullptr;

const char* RoleName(RaftRole r) {
  switch (r) {
    case RaftRole::kFollower:
      return "follower";
    case RaftRole::kCandidate:
      return "candidate";
    case RaftRole::kLeader:
      return "leader";
  }
  return "?";
}

std::vector<std::string> ParsePeerList(const std::string& csv) {
  std::vector<std::string> out;
  size_t start = 0;
  while (start <= csv.size()) {
    const size_t comma = csv.find(',', start);
    const size_t end = (comma == std::string::npos) ? csv.size() : comma;
    std::string item = csv.substr(start, end - start);
    size_t b = item.find_first_not_of(" \t");
    size_t e = item.find_last_not_of(" \t");
    if (b != std::string::npos) out.push_back(item.substr(b, e - b + 1));
    if (comma == std::string::npos) break;
    start = comma + 1;
  }
  return out;
}

bool SplitHostPort(const std::string& s, std::string* host, uint16_t* port) {
  const size_t colon = s.rfind(':');
  if (colon == std::string::npos || colon == 0 || colon + 1 >= s.size())
    return false;
  *host = s.substr(0, colon);
  const long p = std::strtol(s.c_str() + colon + 1, nullptr, 10);
  if (p <= 0 || p > 65535) return false;
  *port = static_cast<uint16_t>(p);
  return true;
}

RaftNode::~RaftNode() {
  closing_ = true;
  if (fd_ >= 0) {
    ::fsync(fd_);
    ::close(fd_);
    fd_ = -1;
  }
  if (state_fd_ >= 0) {
    ::close(state_fd_);
    state_fd_ = -1;
  }
  if (listen_fd_ >= 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
  }
}

bool RaftNode::HasIndex(uint64_t index) const {
  return index >= log_start_index_ && index <= last_log_index_ && !log_.empty();
}

const LogSlot* RaftNode::SlotAt(uint64_t index) const {
  if (!HasIndex(index)) return nullptr;
  return &log_[index - log_start_index_];
}

LogSlot* RaftNode::SlotAt(uint64_t index) {
  if (!HasIndex(index)) return nullptr;
  return &log_[index - log_start_index_];
}

uint64_t RaftNode::TermAt(uint64_t index) const {
  if (index == 0) return 0;
  const LogSlot* s = SlotAt(index);
  return s ? s->term : 0;
}

uint64_t RaftNode::LastLogTerm() const { return TermAt(last_log_index_); }

bool RaftNode::TruncateFrom(uint64_t from) {
  if (from > last_log_index_) return true;
  if (from <= commit_index_) {
    LOG(ERROR) << "raft: refusing to truncate committed index " << from
               << " (commit=" << commit_index_ << ")";
    return false;
  }

  const LogSlot* s = SlotAt(from);
  const uint64_t cut_offset = s ? s->file_offset : file_offset_;

  size_t keep = 0;
  for (const LogSlot& slot : log_) {
    if (slot.index >= from) break;
    ++keep;
  }
  log_.resize(keep);
  log_txids_.resize(keep);
  last_log_index_ = from - 1;

  if (::ftruncate(fd_, static_cast<off_t>(cut_offset)) < 0) {
    LOG(ERROR) << "raft: ftruncate failed: " << std::strerror(errno);
    return false;
  }
  file_offset_ = cut_offset;
  LOG(WARNING) << "raft: truncated log from index " << from << " (offset "
               << cut_offset << ")";

  return true;
}

bool RaftNode::Open() {
  fd_ = ::open(cfg_.log_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
  if (fd_ < 0) {
    LOG(ERROR) << "raft: cannot open log file " << cfg_.log_path << ": "
               << std::strerror(errno);
    return false;
  }
  state_path_ = cfg_.log_path + ".state";
  state_fd_ = ::open(state_path_.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
  if (state_fd_ < 0) {
    LOG(ERROR) << "raft: cannot open state file " << state_path_ << ": "
               << std::strerror(errno);
    return false;
  }
  if (!LoadState()) return false;
  return RecoverFromDisk();
}

bool RaftNode::PersistState() {
  state_buf_.clear();
  PutU64(&state_buf_, term_);
  PutU32(&state_buf_, voted_for_);
  PutU64(&state_buf_, commit_index_);
  PutU32(&state_buf_, RaftCrc32Raw(state_buf_));

  size_t done = 0;
  while (done < state_buf_.size()) {
    const ssize_t n = ::pwrite(state_fd_, state_buf_.data() + done,
                               state_buf_.size() - done, done);
    if (n <= 0) {
      LOG(ERROR) << "raft: state pwrite failed: " << std::strerror(errno);
      return false;
    }
    done += static_cast<size_t>(n);
  }
  if (::fdatasync(state_fd_) < 0) {
    LOG(ERROR) << "raft: state fdatasync failed: " << std::strerror(errno);
    return false;
  }
  return true;
}

bool RaftNode::LoadState() {
  char buf[24];
  const ssize_t n = ::pread(state_fd_, buf, sizeof(buf), 0);
  if (n <= 0) {
    term_ = 0;
    voted_for_ = kNoVote;
    commit_index_ = 0;
    return true;
  }
  if (n == 16) {
    const uint64_t old_term = GetU64(buf);
    const uint32_t old_voted = GetU32(buf + 8);
    const uint32_t old_crc = GetU32(buf + 12);
    if (RaftCrc32(old_term, old_voted, 0, {}) != old_crc) {
      LOG(WARNING) << "raft: legacy state file CRC mismatch, resetting";
      term_ = 0;
      voted_for_ = kNoVote;
      commit_index_ = 0;
      return true;
    }
    term_ = old_term;
    voted_for_ = old_voted;
    commit_index_ = 0;
    LOG(INFO) << "raft: loaded legacy state term=" << old_term;
    return true;
  }
  if (n < static_cast<ssize_t>(sizeof(buf))) {
    LOG(WARNING) << "raft: state file short (" << n << " bytes), resetting";
    term_ = 0;
    voted_for_ = kNoVote;
    commit_index_ = 0;
    return true;
  }
  const uint64_t term = GetU64(buf);
  const uint32_t voted = GetU32(buf + 8);
  const uint64_t commit = GetU64(buf + 12);
  const uint32_t crc = GetU32(buf + 20);
  if (RaftCrc32Raw(std::string_view(buf, 20)) != crc) {
    LOG(WARNING) << "raft: state file CRC mismatch, resetting";
    term_ = 0;
    voted_for_ = kNoVote;
    commit_index_ = 0;
    return true;
  }
  term_ = term;
  voted_for_ = voted;
  commit_index_ = commit;
  LOG(INFO) << "raft: loaded state term=" << term
            << " commit=" << commit_index_;
  return true;
}

bool RaftNode::StartTransport() {
  if (cfg_.peers.size() <= 1 && !cfg_.joining) {
    role_.store(RaftRole::kLeader, std::memory_order_release);
    LOG(INFO) << "raft: single-node mode, self-elected leader";
    return true;
  }
  if (!cfg_.joining && cfg_.node_id >= cfg_.peers.size()) {
    LOG(ERROR) << "raft: node_id " << cfg_.node_id
               << " out of range (peers=" << cfg_.peers.size() << ")";
    return false;
  }

  if (cfg_.node_id >= cfg_.peers.size()) {
    LOG(ERROR) << "raft: joining node must also list itself in raft_peers";
    return false;
  }
  std::string own_host;
  uint16_t own_port = 0;
  if (!SplitHostPort(cfg_.peers[cfg_.node_id], &own_host, &own_port)) {
    LOG(ERROR) << "raft: bad own spec '" << cfg_.peers[cfg_.node_id] << "'";
    return false;
  }
  listen_fd_ = RaftListenFd(own_port);
  if (listen_fd_ < 0) return false;
  inbound_ = std::make_unique<RaftInbound>(proactor_, this, listen_fd_);
  inbound_->Start();
  LOG(INFO) << "raft: listening on port " << own_port;

  members_.clear();
  if (cfg_.joining) {
    RaftMember self;
    self.id = cfg_.node_id;
    self.host = own_host;
    self.port = own_port;
    self.learner = true;
    members_.push_back(self);
  } else {
    for (size_t i = 0; i < cfg_.peers.size(); ++i) {
      std::string host;
      uint16_t port = 0;
      if (!SplitHostPort(cfg_.peers[i], &host, &port)) {
        LOG(ERROR) << "raft: bad peer spec '" << cfg_.peers[i] << "'";
        return false;
      }
      RaftMember m;
      m.id = static_cast<uint32_t>(i);
      m.host = host;
      m.port = port;
      m.learner = false;
      members_.push_back(m);
    }
  }

  SyncPeersToMembers();

  if (cfg_.joining && !cfg_.seed.empty()) {
    std::string shost;
    uint16_t sport = 0;
    if (!SplitHostPort(cfg_.seed, &shost, &sport)) {
      LOG(ERROR) << "raft: bad seed spec '" << cfg_.seed << "'";
      return false;
    }
    auto peer =
        std::make_unique<RaftPeer>(proactor_, kSeedPeerId, shost, sport);
    peer->Start();
    LOG(INFO) << "raft: joining, dialing seed " << peer->Describe();
    peers_.push_back(std::move(peer));
  }

  if (cfg_.is_leader && !cfg_.joining) {
    BecomeLeader();
  } else {
    role_.store(RaftRole::kFollower, std::memory_order_release);
    last_heartbeat_ms_ = util::GetSteadyTimeMs();
  }

  HeartbeatLoop();
  ElectionLoop();
  return true;
}

void RaftNode::SyncPeersToMembers() {
  if (cfg_.joining && members_.size() > 1) {
    for (auto it = peers_.begin(); it != peers_.end();) {
      if ((*it)->id() == kSeedPeerId) {
        it = peers_.erase(it);
      } else {
        ++it;
      }
    }
  }

  for (const RaftMember& m : members_) {
    if (m.id == cfg_.node_id) continue;
    RaftPeer* existing = FindPeer(m.id);
    if (existing) {
      existing->SetLearner(m.learner);
      continue;
    }
    auto peer = std::make_unique<RaftPeer>(proactor_, m.id, m.host, m.port);
    peer->SetLearner(m.learner);
    peer->Start();
    LOG(INFO) << "raft: dialing member " << peer->Describe() << " (id=" << m.id
              << (m.learner ? ", learner" : "") << ")";
    peers_.push_back(std::move(peer));
  }
  if (next_index_.size() != peers_.size()) {
    next_index_.assign(peers_.size(), last_log_index_ + 1);
    match_index_.assign(peers_.size(), 0);
  }
}

size_t RaftNode::VoterCount() const {
  size_t n = 1;
  for (const RaftMember& m : members_) {
    if (m.id == cfg_.node_id) continue;
    if (!m.learner) ++n;
  }
  return n;
}

void RaftNode::ApplyAddMember(uint32_t id, const std::string& host,
                              uint16_t port) {
  for (RaftMember& m : members_) {
    if (m.id == id) {
      m.host = host;
      m.port = port;
      return;
    }
  }
  RaftMember m;
  m.id = id;
  m.host = host;
  m.port = port;
  m.learner = true;
  members_.push_back(m);
  LOG(INFO) << "raft: member added id=" << id << " at " << m.Describe()
            << " (learner)";

  SyncPeersToMembers();
}

bool RaftNode::ProposeAddMember(const std::string& host, uint16_t port,
                                uint32_t id, std::string* err) {
  if (!is_leader()) {
    if (err) *err = "NOTLEADER";
    return false;
  }
  if (id == cfg_.node_id) {
    if (err) *err = "ERR cannot add self";
    return false;
  }
  for (const RaftMember& m : members_) {
    if (m.id == id) {
      if (err) *err = "ERR node id already present";
      return false;
    }
    if (m.host == host && m.port == port) {
      if (err) *err = "ERR address already present";
      return false;
    }
  }
  if (add_in_flight_) {
    if (err) *err = "ERR another membership change in flight";
    return false;
  }
  add_in_flight_ = true;
  add_in_flight_id_ = id;
  LOG(INFO) << "raft: proposing ADDNODE id=" << id << " " << host << ":"
            << port;
  std::string payload = EncodeRespCommandFromStrings(
      {"RAFT", "ADDNODE", host, std::to_string(port), std::to_string(id)});
  AppendConfigEntry(payload);
  return true;
}

bool RaftNode::OnAddNodeCommand(const std::string& host, uint16_t port,
                                uint32_t id, std::string* err) {
  std::latch done(1);
  bool ok = false;
  std::string reason;
  auto* main_q = &RedisServer::Instance().MainProactor()->GetTaskQueue();
  const bool posted = main_q->TryAdd([&]() {
    ok = ProposeAddMember(host, port, id, &reason);
    done.count_down();
  });
  if (!posted) {
    if (err) *err = "ERR main task queue overflow";
    return false;
  }
  done.wait();
  if (!ok && err) *err = reason;
  return ok;
}

std::string RaftNode::DescribeMembers() const {
  std::string out;
  for (const RaftMember& m : members_) {
    out += std::to_string(m.id);
    out += ' ';
    out += m.Describe();
    out += m.id == cfg_.node_id ? " self" : "";
    out += m.learner ? " learner" : " voter";
    out += '\n';
  }
  return out;
}

void RaftNode::MaybePromoteLearners() {
  if (!is_leader()) return;
  if (!add_in_flight_) return;
  RaftPeer* peer = FindPeer(add_in_flight_id_);
  if (!peer || !peer->is_learner()) return;
  if (!peer->connected()) return;
  if (peer->match_index() < last_log_index_) return;

  LOG(INFO) << "raft: learner id=" << add_in_flight_id_
            << " caught up (match=" << peer->match_index()
            << "), proposing PROMOTE";
  add_in_flight_ = false;
  const uint32_t id = add_in_flight_id_;
  std::string payload =
      EncodeRespCommandFromStrings({"RAFT", "PROMOTE", std::to_string(id)});
  AppendConfigEntry(payload);
}

bool RaftNode::ApplyConfigEntry(const std::vector<std::string_view>& args) {
  if (args.size() < 2) return false;
  const std::string cmd = util::ToUpperIfNeeded(args[0]);
  if (cmd != "RAFT") return false;
  const std::string sub = util::ToUpperIfNeeded(args[1]);
  if (sub != "ADDNODE" && sub != "PROMOTE") return false;

  if (sub == "ADDNODE") {
    if (args.size() < 5) {
      LOG(ERROR) << "raft: malformed ADDNODE entry";
      return true;
    }
    const std::string host(args[2]);
    const uint16_t port =
        static_cast<uint16_t>(std::stoi(std::string(args[3])));
    const uint32_t id = static_cast<uint32_t>(std::stoul(std::string(args[4])));
    ApplyAddMember(id, host, port);
    return true;
  }

  if (args.size() < 3) {
    LOG(ERROR) << "raft: malformed PROMOTE entry";
    return true;
  }
  const uint32_t id = static_cast<uint32_t>(std::stoul(std::string(args[2])));
  for (RaftMember& m : members_) {
    if (m.id == id) {
      if (m.learner) {
        m.learner = false;
        LOG(INFO) << "raft: member id=" << id << " is now a voter";
      }
      break;
    }
  }
  if (RaftPeer* p = FindPeer(id)) p->SetLearner(false);
  SyncPeersToMembers();
  return true;
}

uint64_t RaftNode::RandomizedElectionTimeout() {
  static uint64_t seed = 0;
  if (seed == 0) seed = util::GetCurrentTimeMs() ^ (cfg_.node_id * 2654435761u);
  seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
  const uint64_t base = cfg_.election_timeout_ms;
  return base + (seed >> 33) % base;
}

void RaftNode::BecomeFollower(uint64_t new_term, const char* why) {
  const bool term_changed = new_term > term_;
  if (role_.load(std::memory_order_relaxed) != RaftRole::kFollower)
    LOG(INFO) << "raft: " << RoleName(role_.load(std::memory_order_relaxed))
              << " -> follower (term " << term_ << " -> " << new_term
              << "): " << why;
  role_.store(RaftRole::kFollower, std::memory_order_release);
  lease_deadline_ms_.store(0, std::memory_order_release);
  if (term_changed) {
    term_ = new_term;
    voted_for_ = kNoVote;
    PersistState();
  }
  last_heartbeat_ms_ = util::GetSteadyTimeMs();
}

void RaftNode::BecomeLeader() {
  LOG(INFO) << "raft: becoming LEADER for term " << term_
            << " (last_log_index=" << last_log_index_ << ")";
  role_.store(RaftRole::kLeader, std::memory_order_release);
  leader_id_ = cfg_.node_id;
  next_index_.assign(peers_.size(), last_log_index_ + 1);
  match_index_.assign(peers_.size(), 0);
  for (auto& p : peers_) p->SetMatchIndex(0);
}

bool RaftNode::StepDownIfStale(uint64_t peer_term, const char* why) {
  if (peer_term <= term_) return false;
  BecomeFollower(peer_term, why);
  return true;
}

cppcoro::AsyncTask RaftNode::ElectionLoop() {
  uint64_t deadline_ms = RandomizedElectionTimeout();

  while (!closing_) {
    co_await proactor_->ArmPeriodicTimer(kElectionPollMs);
    if (closing_) break;
    if (peers_.empty()) continue;
    if (role_.load(std::memory_order_relaxed) == RaftRole::kLeader) continue;
    if (cfg_.joining && IsJoiningLearner()) continue;

    const uint64_t since = util::GetSteadyTimeMs() - last_heartbeat_ms_;
    if (since < deadline_ms) continue;

    LOG(INFO) << "raft: election timeout (" << since << "ms since heartbeat, "
              << "deadline " << deadline_ms << "ms)";
    co_await RunElection();
    deadline_ms = RandomizedElectionTimeout();
  }
  co_return;
}

cppcoro::task<> RaftNode::RunElection() {
  last_heartbeat_ms_ = util::GetSteadyTimeMs();

  ++term_;
  voted_for_ = cfg_.node_id;
  role_.store(RaftRole::kCandidate, std::memory_order_release);
  if (!PersistState()) {
    LOG(ERROR) << "raft: cannot persist state, aborting election";
    co_return;
  }
  const uint64_t my_term = term_;
  LOG(INFO) << "raft: starting election for term " << my_term;

  vote_buf_.clear();
  PutU64(&vote_buf_, my_term);
  PutU32(&vote_buf_, cfg_.node_id);
  PutU64(&vote_buf_, last_log_index_);
  PutU64(&vote_buf_, LastLogTerm());

  size_t votes = 1;
  const size_t need = Quorum();

  for (auto& peer : peers_) {
    if (role_.load(std::memory_order_relaxed) != RaftRole::kCandidate ||
        term_ != my_term) {
      LOG(INFO) << "raft: election for term " << my_term << " aborted";
      co_return;
    }
    if (votes >= need) break;
    if (!peer->connected()) continue;
    if (peer->is_learner()) continue;

    auto send_guard = co_await peer->LockSend();
    const RequestVoteResp resp =
        co_await peer->SendRequestVote(vote_buf_, cfg_.rpc_timeout_ms);
    if (StepDownIfStale(resp.term, "saw higher term in vote response"))
      co_return;
    if (resp.granted) {
      ++votes;
      VLOG(1) << "raft: got vote from " << peer->Describe() << " (" << votes
              << "/" << need << ")";
    }
  }

  if (role_.load(std::memory_order_relaxed) == RaftRole::kCandidate &&
      term_ == my_term && votes >= need) {
    BecomeLeader();
  } else {
    LOG(INFO) << "raft: election for term " << my_term << " failed (" << votes
              << "/" << need << " votes)";
    if (role_.load(std::memory_order_relaxed) == RaftRole::kCandidate)
      role_.store(RaftRole::kFollower, std::memory_order_release);
  }
  co_return;
}

RequestVoteResp RaftNode::HandleRequestVote(const char* body, uint32_t len) {
  RequestVoteResp resp;
  resp.granted = false;
  if (len < kRequestVoteBodySize) {
    LOG(ERROR) << "raft: short RequestVote body " << len;
    resp.term = term_;
    return resp;
  }
  const uint64_t cand_term = GetU64(body);
  const uint32_t cand_id = GetU32(body + 8);
  const uint64_t cand_last_index = GetU64(body + 12);
  const uint64_t cand_last_term = GetU64(body + 20);

  StepDownIfStale(cand_term, "saw higher term in RequestVote");

  resp.term = term_;
  if (IsJoiningLearner()) {
    LOG(INFO) << "raft: reject vote for node " << cand_id
              << " (still a joining learner)";
    return resp;
  }
  if (cand_term < term_) {
    LOG(INFO) << "raft: reject vote for node " << cand_id << " (term "
              << cand_term << " < " << term_ << ")";
    return resp;
  }
  if (voted_for_ != kNoVote && voted_for_ != cand_id) {
    LOG(INFO) << "raft: reject vote for node " << cand_id << " (already voted "
              << voted_for_ << " in term " << term_ << ")";
    return resp;
  }
  const uint64_t my_last_term = LastLogTerm();
  const bool cand_up_to_date =
      (cand_last_term > my_last_term) ||
      (cand_last_term == my_last_term && cand_last_index >= last_log_index_);
  if (!cand_up_to_date) {
    LOG(INFO) << "raft: reject vote for node " << cand_id << " (log behind: ("
              << cand_last_term << "," << cand_last_index << ") < ("
              << my_last_term << "," << last_log_index_ << "))";
    return resp;
  }

  voted_for_ = cand_id;
  if (!PersistState()) {
    LOG(ERROR) << "raft: cannot persist vote, rejecting";
    voted_for_ = kNoVote;
    return resp;
  }
  resp.granted = true;
  last_heartbeat_ms_ = util::GetSteadyTimeMs();
  LOG(INFO) << "raft: granted vote to node " << cand_id << " for term "
            << term_;
  return resp;
}

void RaftNode::SubmitBatch(std::vector<RaftLogEntry>&& batch) {
  if (batch.empty()) return;

  if (pending_.empty()) {
    pending_ = std::move(batch);
  } else {
    pending_.insert(pending_.end(), std::make_move_iterator(batch.begin()),
                    std::make_move_iterator(batch.end()));
  }

  if (HasRaftStatus(kRaftDriving)) return;
  DriveLogs();
}

void RaftNode::AppendConfigEntry(const std::string& payload) {
  if (!is_leader()) return;
  RaftLogEntry e;
  e.txid = 0;
  e.start_ms = util::GetCurrentTimeMs();
  e.payload = payload;
  std::vector<RaftLogEntry> batch;
  batch.push_back(std::move(e));
  SubmitBatch(std::move(batch));
}

cppcoro::AsyncTask RaftNode::DriveLogs() {
  SetRaftStatus(kRaftDriving);

  while (!pending_.empty()) {
    std::vector<RaftLogEntry> batch = std::move(pending_);
    pending_.clear();

    if (!is_leader()) {
      LOG(WARNING) << "raft: not leader ("
                   << RoleName(role_.load(std::memory_order_relaxed))
                   << "), dropping " << batch.size() << " entries";
      continue;
    }

    std::sort(batch.begin(), batch.end(),
              [](const RaftLogEntry& a, const RaftLogEntry& b) {
                return a.txid < b.txid;
              });

    const uint64_t my_term = term_;
    const uint64_t target = co_await AppendBatchToLog(batch);
    if (target == 0) {
      LOG(ERROR) << "raft: cannot append batch to log";
      continue;
    }

    for (size_t i = 0; i < peers_.size(); ++i) {
      if (!is_leader() || term_ != my_term) break;
      co_await ReplicateToPeer(i);
    }
    AdvanceCommitIndex();

    if (commit_index_ < target) {
      const bool ok = co_await AwaitCommit(target);
      if (!ok) {
        LOG(ERROR) << "raft: index " << target << " not committed, failing";
        FailFrom(commit_index_ + 1);
        continue;
      }
    }
  }

  ClearRaftStatus(kRaftDriving);
  co_return;
}

cppcoro::task<uint64_t> RaftNode::AppendBatchToLog(
    std::vector<RaftLogEntry>& batch) {
  write_buf_.clear();
  const uint64_t base_offset = file_offset_;
  std::vector<LogSlot> slots;
  slots.reserve(batch.size());
  std::vector<TxId> slot_txids;
  slot_txids.reserve(batch.size());

  for (RaftLogEntry& e : batch) {
    e.index = ++last_log_index_;
    e.term = term_;

    LogSlot slot;
    slot.index = e.index;
    slot.term = e.term;
    slot.start_ms = e.start_ms;
    slot.payload = e.payload;
    slot.file_offset = base_offset + write_buf_.size();
    AppendRecord(e, &write_buf_);
    slot.record_size = base_offset + write_buf_.size() - slot.file_offset;
    slots.push_back(std::move(slot));
    slot_txids.push_back(e.txid);
  }

  if (write_buf_.empty()) co_return 0;

  if (!co_await WriteRawAndSync(write_buf_, base_offset)) {
    LOG(FATAL) << "raft: leader local log write/fsync failed at offset "
               << base_offset;
  }

  const uint64_t last = slots.empty() ? 0 : slots.back().index;
  for (LogSlot& s : slots) log_.push_back(std::move(s));
  log_txids_.insert(log_txids_.end(), slot_txids.begin(), slot_txids.end());
  if (log_.size() == slots.size() && !log_.empty())
    log_start_index_ = log_.front().index;
  co_return last;
}

cppcoro::task<bool> RaftNode::ReplicateToPeer(size_t peer_idx) {
  if (peer_idx >= peers_.size()) co_return false;
  RaftPeer* peer = peers_[peer_idx].get();
  if (!peer->connected()) co_return false;
  if (!is_leader()) co_return false;

  auto send_guard = co_await peer->LockSend();
  if (!peer->connected() || !is_leader()) co_return false;

  const uint64_t my_term = term_;

  for (int attempt = 0; attempt < 8; ++attempt) {
    if (!is_leader() || term_ != my_term) co_return false;
    if (!peer->connected()) co_return false;

    uint64_t next = next_index_[peer_idx];
    if (next < log_start_index_) {
      LOG(ERROR) << "raft: peer " << peer->Describe() << " needs index " << next
                 << " but log starts at " << log_start_index_
                 << " (needs InstallSnapshot, P4b)";
      co_return false;
    }

    const uint64_t prev_index = next - 1;
    uint32_t count = 0;
    if (next <= last_log_index_)
      count = static_cast<uint32_t>(std::min<uint64_t>(
          last_log_index_ - next + 1, cfg_.max_entries_per_rpc));

    wire_buf_.clear();
    EncodeAppendEntries(next, count, &wire_buf_);

    const AppendEntriesResp resp =
        co_await peer->SendAppendEntries(wire_buf_, cfg_.rpc_timeout_ms);
    if (StepDownIfStale(resp.term, "saw higher term in AppendEntries resp"))
      co_return false;
    if (!is_leader() || term_ != my_term) co_return false;

    if (resp.success) {
      const uint64_t new_match = std::max(resp.match_index, prev_index + count);
      match_index_[peer_idx] = std::max(match_index_[peer_idx], new_match);
      next_index_[peer_idx] = match_index_[peer_idx] + 1;
      peer->SetMatchIndex(match_index_[peer_idx]);
      if (next_index_[peer_idx] <= last_log_index_) continue;
      co_return true;
    }

    uint64_t back_to = resp.conflict_index;
    if (back_to == 0 || back_to > next) back_to = next > 1 ? next - 1 : 1;
    if (back_to < 1) back_to = 1;
    if (back_to == next_index_[peer_idx]) {
      if (back_to <= 1) co_return false;
      back_to = next_index_[peer_idx] - 1;
    }
    next_index_[peer_idx] = back_to;
    VLOG(1) << "raft: peer " << peer->Describe() << " rejected, backing off to "
            << back_to;
  }
  co_return false;
}

void RaftNode::AdvanceCommitIndex() {
  if (!is_leader()) return;

  uint64_t candidate = 0;
  if (peers_.empty()) {
    candidate = last_log_index_;
  } else {
    std::vector<uint64_t> matches;
    matches.reserve(peers_.size() + 1);
    matches.push_back(last_log_index_);
    for (size_t i = 0; i < peers_.size(); ++i) {
      if (peers_[i]->is_learner()) continue;
      matches.push_back(match_index_[i]);
    }
    std::sort(matches.begin(), matches.end(), std::greater<uint64_t>());

    candidate = matches[Quorum() - 1];

    const LogSlot* s = SlotAt(candidate);
    if (s && s->term != term_) {
      VLOG(1) << "raft: not committing index " << candidate << " from old term "
              << s->term;
      return;
    }
  }
  if (candidate <= commit_index_) return;

  commit_index_ = candidate;
  if (!PersistState())
    LOG(ERROR) << "raft: failed to persist commit watermark " << candidate;
  VLOG(1) << "raft: commit_index -> " << commit_index_;

  std::vector<TxId> ready;
  for (size_t i = 0; i < log_.size(); ++i) {
    LogSlot& slot = log_[i];
    if (slot.index > commit_index_) break;
    const TxId txid = log_txids_[i];
    if (txid != 0) {
      ready.push_back(txid);
    } else if (slot.index > applied_index_) {
      ReplayEntry(slot);
    }
  }
  if (!ready.empty()) BroadcastReadyTxids(/*is_read=*/false, std::move(ready));
  applied_index_ = std::max(applied_index_, commit_index_);
}

cppcoro::task<bool> RaftNode::AwaitCommit(uint64_t target_index) {
  const uint64_t my_term = term_;
  const uint64_t deadline = util::GetCurrentTimeMs() + kCommitTimeoutMs;

  while (commit_index_ < target_index) {
    if (!is_leader() || term_ != my_term) co_return false;
    if (util::GetCurrentTimeMs() > deadline) {
      LOG(ERROR) << "raft: timed out waiting for commit of index "
                 << target_index << " (commit=" << commit_index_ << ")";
      co_return false;
    }
    co_await proactor_->ArmPeriodicTimer(kCommitPollMs);
    AdvanceCommitIndex();
  }
  co_return true;
}

void RaftNode::FailFrom(uint64_t from_index) {
  if (from_index <= commit_index_) from_index = commit_index_ + 1;
  TruncateFrom(from_index);
}

void RaftNode::BroadcastReadyTxids(bool is_read, std::vector<TxId> txids) {
  if (txids.empty()) return;
  std::sort(txids.begin(), txids.end());
  std::vector<std::pair<TxId, TxId>> ranges;
  for (TxId id : txids) {
    if (!ranges.empty() && id <= ranges.back().second + 1) {
      ranges.back().second = std::max(ranges.back().second, id);
    } else {
      ranges.emplace_back(id, id);
    }
  }
  auto shared =
      std::make_shared<std::vector<std::pair<TxId, TxId>>>(std::move(ranges));
  for (ShardId sid = 0; sid < static_cast<ShardId>(shard_pool->size()); ++sid) {
    shard_pool->Post(sid, [is_read, shared] {
      Shard::tlocal()->AddReadyRanges(is_read, *shared);
      Shard::tlocal()->DriveQueue(nullptr);
    });
  }
}

RaftPeer* RaftNode::FindPeer(uint32_t id) {
  for (auto& p : peers_) {
    if (p->id() == id) return p.get();
  }
  return nullptr;
}

void RaftNode::RequestFollowerRead(TxId txid) {
  if (txid == 0) return;

  if (LeaseReadAllowed()) {
    BroadcastReadyTxids(/*is_read=*/true, {txid});
    return;
  }

  if (role_.load(std::memory_order_relaxed) != RaftRole::kFollower ||
      leader_id_ == kNoVote) {
    LOG(WARNING) << "raft: follower read cannot be confirmed (role="
                 << RoleName(role_.load(std::memory_order_relaxed))
                 << "), leaving tx pending";
    return;
  }

  pending_reads_.push_back(txid);
  if (!HasRaftStatus(kRaftReadDrive)) {
    SetRaftStatus(kRaftReadDrive);
    ReadIndexDrive();
  }
}

cppcoro::AsyncTask RaftNode::ReadIndexDrive() {
  while (true) {
    if (pending_reads_.empty()) {
      ClearRaftStatus(kRaftReadDrive);
      co_return;
    }
    auto batch = std::move(pending_reads_);
    pending_reads_.clear();

    const uint64_t term = term_;
    const uint32_t leader = leader_id_;

    auto fail_batch = [&]() {
      LOG(WARNING) << "raft: follower read batch unconfirmed (" << batch.size()
                   << " txs), leaving pending";
    };

    RaftPeer* peer = nullptr;
    ReadIndexResp r;
    if (role_.load(std::memory_order_relaxed) == RaftRole::kFollower &&
        leader != kNoVote) {
      peer = FindPeer(leader);
    }
    if (peer) {
      auto send_guard = co_await peer->LockSend();
      r = co_await peer->SendReadIndex(term, cfg_.rpc_timeout_ms);
    }

    if (r.term > term) {
      StepDownIfStale(r.term, "higher term in ReadIndex resp");
    }

    bool ok = peer && r.success && r.term == term &&
              role_.load(std::memory_order_relaxed) == RaftRole::kFollower &&
              leader_id_ == leader;

    if (ok) {
      const uint64_t deadline = util::GetSteadyTimeMs() + cfg_.rpc_timeout_ms;
      while (applied_index_ < r.commit_index) {
        if (term_ != term ||
            role_.load(std::memory_order_relaxed) != RaftRole::kFollower ||
            leader_id_ != leader || util::GetSteadyTimeMs() >= deadline) {
          ok = false;
          break;
        }
        co_await proactor_->ArmPeriodicTimer(kCommitPollMs);
      }
    }

    if (!ok) {
      fail_batch();
      continue;
    }

    BroadcastReadyTxids(/*is_read=*/true, std::move(batch));
  }
}

ReadIndexResp RaftNode::HandleReadIndex(const char* body, uint32_t len) {
  ReadIndexResp resp;
  if (len < kReadIndexReqBodySize) {
    LOG(ERROR) << "raft: short ReadIndex req " << len;
    resp.term = term_;
    return resp;
  }

  const uint64_t asker_term = GetU64(body);
  if (asker_term > term_) {
    BecomeFollower(asker_term, "higher term in ReadIndex");
  }
  resp.term = term_;

  if (is_leader() && LeaseReadAllowed()) {
    resp.success = true;
    resp.commit_index = commit_index_;
  }
  return resp;
}

void RaftNode::EncodeAppendEntries(uint64_t next_index, uint32_t count,
                                   std::string* out) const {
  const uint64_t prev_index = next_index - 1;
  PutU64(out, term_);
  PutU32(out, cfg_.node_id);
  PutU64(out, commit_index_);
  PutU64(out, prev_index);
  PutU64(out, TermAt(prev_index));
  PutU32(out, count);

  for (uint32_t i = 0; i < count; ++i) {
    const LogSlot* s = SlotAt(next_index + i);
    if (!s) break;
    PutU64(out, s->index);
    PutU64(out, s->term);
    PutU64(out, s->start_ms);
    PutU32(out, static_cast<uint32_t>(s->payload.size()));
    out->append(s->payload);
  }
}

cppcoro::AsyncTask RaftNode::HeartbeatLoop() {
  while (!closing_) {
    co_await proactor_->ArmPeriodicTimer(kHeartbeatMs);
    if (closing_) break;
    if (!is_leader() || peers_.empty()) continue;

    const uint64_t my_term = term_;
    const uint64_t sent_ms = util::GetSteadyTimeMs();
    size_t acks = 0;
    for (size_t i = 0; i < peers_.size(); ++i) {
      if (!is_leader() || term_ != my_term) break;
      if (co_await ReplicateToPeer(i)) ++acks;
    }
    RenewLease(sent_ms, acks);
    AdvanceCommitIndex();
    MaybePromoteLearners();
  }
  co_return;
}

void RaftNode::RenewLease(uint64_t sent_ms, size_t acked_peers) {
  if (!is_leader()) return;
  if (1 + acked_peers < Quorum()) return;
  lease_deadline_ms_.store(sent_ms + LeaseMs(), std::memory_order_release);
}

cppcoro::task<AppendEntriesResp> RaftNode::HandleAppendEntries(const char* body,
                                                               uint32_t len) {
  auto inbound_guard = co_await inbound_mu_.scoped_lock_async();

  AppendEntriesResp resp;
  resp.success = false;
  resp.term = term_;

  if (len < kAppendEntriesHeaderSize) {
    LOG(ERROR) << "raft: AppendEntries body too short: " << len;
    co_return resp;
  }
  const uint64_t leader_term = GetU64(body);
  const uint32_t leader_id = GetU32(body + 8);
  const uint64_t leader_commit = GetU64(body + 12);
  const uint64_t prev_log_index = GetU64(body + 20);
  const uint64_t prev_log_term = GetU64(body + 28);
  const uint32_t count = GetU32(body + 36);

  if (leader_term < term_) {
    LOG(WARNING) << "raft: reject AppendEntries from node " << leader_id
                 << " (term " << leader_term << " < " << term_ << ")";
    resp.term = term_;
    co_return resp;
  }
  StepDownIfStale(leader_term, "saw higher term in AppendEntries");
  if (role_.load(std::memory_order_relaxed) != RaftRole::kFollower) {
    LOG(INFO) << "raft: " << RoleName(role_.load(std::memory_order_relaxed))
              << " -> follower (node " << leader_id << " is leader for term "
              << leader_term << ")";
    role_.store(RaftRole::kFollower, std::memory_order_release);
  }
  leader_id_ = leader_id;
  last_heartbeat_ms_ = util::GetSteadyTimeMs();
  resp.term = term_;

  if (prev_log_index > last_log_index_) {
    resp.conflict_index = last_log_index_ + 1;
    VLOG(1) << "raft: log too short (have " << last_log_index_ << ", need prev "
            << prev_log_index << "), hint " << resp.conflict_index;
    co_return resp;
  }
  if (prev_log_index > 0 && prev_log_index < log_start_index_) {
    resp.conflict_index = log_start_index_;
    co_return resp;
  }
  if (prev_log_index > 0) {
    const uint64_t my_prev_term = TermAt(prev_log_index);
    if (my_prev_term != prev_log_term) {
      uint64_t hint = prev_log_index;
      while (hint > log_start_index_ && hint > commit_index_ + 1 &&
             TermAt(hint - 1) == my_prev_term) {
        --hint;
      }
      resp.conflict_index = hint;
      LOG(WARNING) << "raft: log mismatch at index " << prev_log_index
                   << " (my term " << my_prev_term << " vs leader "
                   << prev_log_term << "), hint " << hint;
      co_return resp;
    }
  }

  inbound_buf_.clear();
  std::vector<LogSlot> accepted;
  size_t pos = kAppendEntriesHeaderSize;
  uint64_t write_offset = 0;
  bool need_truncate = false;
  uint64_t truncate_from = 0;

  for (uint32_t i = 0; i < count; ++i) {
    if (pos + kEntryHeaderSize > len) {
      LOG(ERROR) << "raft: AppendEntries truncated at entry " << i;
      co_return resp;
    }
    LogSlot s;
    s.index = GetU64(body + pos);
    s.term = GetU64(body + pos + 8);
    s.start_ms = GetU64(body + pos + 16);
    const uint32_t plen = GetU32(body + pos + 24);
    pos += kEntryHeaderSize;
    if (pos + plen > len) {
      LOG(ERROR) << "raft: AppendEntries payload overruns frame";
      co_return resp;
    }
    s.payload.assign(body + pos, plen);
    pos += plen;

    const LogSlot* mine = SlotAt(s.index);
    if (mine) {
      if (mine->term == s.term) continue;
      need_truncate = true;
      truncate_from = s.index;
      break;
    }
  }

  if (need_truncate) {
    if (!TruncateFrom(truncate_from)) co_return resp;
    resp.conflict_index = truncate_from;
    co_return resp;
  }

  pos = kAppendEntriesHeaderSize;
  write_offset = file_offset_;
  for (uint32_t i = 0; i < count; ++i) {
    LogSlot s;
    s.index = GetU64(body + pos);
    s.term = GetU64(body + pos + 8);
    s.start_ms = GetU64(body + pos + 16);
    const uint32_t plen = GetU32(body + pos + 24);
    pos += kEntryHeaderSize;
    s.payload.assign(body + pos, plen);
    pos += plen;

    if (s.index <= last_log_index_) continue;

    RaftLogEntry e;
    e.index = s.index;
    e.term = s.term;
    e.start_ms = s.start_ms;
    e.payload = s.payload;

    s.file_offset = write_offset + inbound_buf_.size();
    AppendRecord(e, &inbound_buf_);
    s.record_size = write_offset + inbound_buf_.size() - s.file_offset;
    accepted.push_back(std::move(s));
  }

  if (!inbound_buf_.empty()) {
    if (!co_await WriteRawAndSync(inbound_buf_, write_offset)) co_return resp;
    for (LogSlot& s : accepted) {
      last_log_index_ = s.index;
      if (log_.empty()) log_start_index_ = s.index;
      log_.push_back(std::move(s));
      log_txids_.push_back(0);
    }
  }

  resp.success = true;
  resp.match_index = last_log_index_;

  const uint64_t new_commit = std::min(leader_commit, last_log_index_);
  if (new_commit > commit_index_) {
    commit_index_ = new_commit;
    PersistState();
    ApplyCommitted();
  }

  VLOG(1) << "raft: follower accepted " << accepted.size() << " entries, "
          << "last_index=" << last_log_index_ << " commit=" << commit_index_;
  co_return resp;
}

void RaftNode::ApplyCommitted() {
  if (applied_index_ >= commit_index_) return;

  SetRaftStatus(kRaftReplaying);

  uint64_t applied = 0;
  for (uint64_t idx = applied_index_ + 1; idx <= commit_index_; ++idx) {
    const LogSlot* s = SlotAt(idx);
    if (!s) break;
    ReplayEntry(*s);
    applied_index_ = idx;
    ++applied;
  }

  ClearRaftStatus(kRaftReplaying);

  if (applied > 0)
    VLOG(1) << "raft: applied " << applied << " entries up to "
            << applied_index_;
}

cppcoro::task<bool> RaftNode::WriteRawAndSync(const std::string& bytes,
                                              uint64_t offset) {
  size_t written = 0;
  while (written < bytes.size()) {
    const int n = co_await proactor_->AsyncWriteFile(
        fd_, bytes.data() + written, bytes.size() - written, offset + written);
    if (n <= 0) {
      LOG(ERROR) << "raft: write failed at offset " << offset + written << ": "
                 << n;
      co_return false;
    }
    written += static_cast<size_t>(n);
  }
  const int rc = co_await proactor_->AsyncFsync(fd_, /*datasync=*/true);
  if (rc < 0) {
    LOG(ERROR) << "raft: fsync failed: " << std::strerror(-rc);
    co_return false;
  }
  file_offset_ = std::max(file_offset_, offset + written);
  co_return true;
}

namespace {

template <typename T>
T GetLE(const char* p) {
  T v;
  std::memcpy(&v, p, sizeof(T));
  return v;
}

}  // namespace

bool RaftNode::RecoverFromDisk() {
  const off_t size = ::lseek(fd_, 0, SEEK_END);
  if (size < 0) {
    LOG(ERROR) << "raft: lseek failed: " << std::strerror(errno);
    return false;
  }
  if (size == 0) {
    file_offset_ = 0;
    LOG(INFO) << "raft: empty log, starting fresh";
    return true;
  }

  std::string buf(static_cast<size_t>(size), '\0');
  {
    size_t done = 0;
    while (done < buf.size()) {
      const ssize_t n =
          ::pread(fd_, buf.data() + done, buf.size() - done, done);
      if (n < 0) {
        LOG(ERROR) << "raft: pread failed: " << std::strerror(errno);
        return false;
      }
      if (n == 0) break;
      done += static_cast<size_t>(n);
    }
    buf.resize(done);
  }

  size_t pos = 0;
  bool torn = false;

  while (pos < buf.size()) {
    if (buf.size() - pos < kRaftRecordHeaderSize) {
      torn = true;
      break;
    }
    const char* h = buf.data() + pos;
    const uint32_t len = GetLE<uint32_t>(h);
    const uint64_t index = GetLE<uint64_t>(h + 4);
    const uint64_t term = GetLE<uint64_t>(h + 12);
    const uint64_t start_ms = GetLE<uint64_t>(h + 20);
    const uint32_t crc = GetLE<uint32_t>(h + 28);

    if (buf.size() - pos - kRaftRecordHeaderSize < len) {
      torn = true;
      break;
    }
    std::string_view payload(h + kRaftRecordHeaderSize, len);
    if (RaftCrc32(index, term, start_ms, payload) != crc) {
      torn = true;
      break;
    }

    LogSlot s;
    s.index = index;
    s.term = term;
    s.start_ms = start_ms;
    s.payload = std::string(payload);
    s.file_offset = pos;
    s.record_size = kRaftRecordHeaderSize + len;
    if (log_.empty()) log_start_index_ = index;
    log_.push_back(std::move(s));
    log_txids_.push_back(0);

    pos += kRaftRecordHeaderSize + len;
    last_log_index_ = index;
    term_ = std::max(term_, term);
  }

  if (torn) {
    LOG(WARNING) << "raft: torn tail at offset " << pos << " (file size "
                 << buf.size() << "), truncating";
    if (::ftruncate(fd_, static_cast<off_t>(pos)) < 0) {
      LOG(ERROR) << "raft: ftruncate failed: " << std::strerror(errno);
      return false;
    }
  }
  file_offset_ = pos;

  LOG(INFO) << "raft: recovered " << log_.size() << " entries, term=" << term_
            << " last_index=" << last_log_index_ << " offset=" << pos;

  commit_index_ = std::min(commit_index_, last_log_index_);
  applied_index_ = 0;
  ApplyCommitted();

  LOG(INFO) << "raft: replay done";
  return true;
}

void RaftNode::ReplayEntry(const LogSlot& slot) {
  std::vector<std::string_view> args;
  bool need_more = false;
  size_t pos = 0;
  if (!ParseRESP{}.ParseOne(slot.payload.data(), slot.payload.size(), &pos,
                            &args, &need_more) ||
      args.empty()) {
    LOG(ERROR) << "raft: cannot parse replay payload at index " << slot.index;
    return;
  }

  if (ApplyConfigEntry(args)) return;

  const std::string upper = util::ToUpperIfNeeded(args[0]);
  const std::string_view name = upper.empty() ? args[0] : upper;
  const CommandId* ci = CIs->Find(name);
  if (!ci) {
    LOG(ERROR) << "raft: unknown command '" << args[0] << "' at index "
               << slot.index;
    return;
  }

  std::latch done(1);
  const uint64_t start_ms = slot.start_ms;
  const uint64_t index = slot.index;

  auto replay = [ci, &args, &done, start_ms, index]() {
    auto body = [](const CommandId* cid, ::dfly::CmdArgList a, std::latch* d,
                   uint64_t ms, uint64_t idx) -> cppcoro::AsyncTask {
      ReplyBuilder rb;
      rb.SetSendCallback([](std::vector<std::string>&&) {});

      util::intrusive_ptr<Transaction> tx{new Transaction(cid)};
      tx->Init(0, a);
      tx->OverrideStartTimeMs(ms);
      tx->MarkFromLeader();

      CommandContext ctx(tx, cid, &rb);
      co_await cid->Invoke(&ctx, a);
      rb.Flush();
      VLOG(2) << "raft: replayed index " << idx;
      d->count_down();
      co_return;
    };
    body(ci, ::dfly::CmdArgList(args), &done, start_ms, index);
  };

  shard_pool->Post(0, replay);
  done.wait();
}

}  // namespace dfly
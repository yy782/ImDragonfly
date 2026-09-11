#include "raft/raft_inbound.hpp"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

#include "raft/raft_node.hpp"

namespace dfly {

namespace {
constexpr size_t kRecvChunk = 64 * 1024;
}

int RaftListenFd(uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    LOG(ERROR) << "raft: socket() failed: " << std::strerror(errno);
    return -1;
  }

  // 只设 SO_REUSEADDR（允许重启时立刻复用 TIME_WAIT 的端口），
  // **绝不设 SO_REUSEPORT** —— 否则两个 raft 节点会静默共享端口，
  // 请求被内核随机分发，故障表现极难定位。
  int opt = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(port);

  if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
    LOG(ERROR) << "raft: bind port " << port
               << " failed: " << std::strerror(errno);
    ::close(fd);
    return -1;
  }
  if (::listen(fd, 16) < 0) {
    LOG(ERROR) << "raft: listen failed: " << std::strerror(errno);
    ::close(fd);
    return -1;
  }
  return fd;
}

void RaftInbound::Start() {
  if (listen_fd_ < 0) return;
  AcceptLoop();
}

cppcoro::AsyncTask RaftInbound::AcceptLoop() {
  while (true) {
    const int fd = co_await proactor_->AsyncAccept(listen_fd_);
    if (fd < 0) {
      LOG(WARNING) << "raft: accept failed: " << fd;
      continue;
    }
    int nodelay = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
    LOG(INFO) << "raft: accepted leader connection, fd=" << fd;
    SessionLoop(fd);
  }
  co_return;
}

cppcoro::AsyncTask RaftInbound::SessionLoop(int fd) {
  // 每条会话独占自己的缓冲：这个协程帧活到连接结束，
  // 缓冲随帧存活，AsyncRecv/AsyncSend 的指针始终有效。
  std::vector<char> buf(kRecvChunk);
  size_t have = 0;
  std::string out;

  while (true) {
    if (have == buf.size()) buf.resize(buf.size() * 2);

    const int n =
        co_await proactor_->AsyncRecv(fd, buf.data() + have, buf.size() - have);
    if (n <= 0) {
      LOG(INFO) << "raft: leader connection closed, fd=" << fd << " (" << n
                << ")";
      break;
    }
    have += static_cast<size_t>(n);

    size_t pos = 0;
    bool proto_ok = true;
    while (have - pos >= kFrameHeaderSize) {
      const char* p = buf.data() + pos;
      const uint32_t len = GetU32(p);
      if (len < 1 + 8 || len - 9 > kMaxFrameBody) {
        LOG(ERROR) << "raft: bad inbound frame len " << len;
        proto_ok = false;
        break;
      }
      if (have - pos < kFrameLenSize + len) break;  // 半包

      const auto type = static_cast<RaftMsgType>(GetU8(p + 4));
      const uint64_t seq = GetU64(p + 5);
      const char* body = p + kFrameHeaderSize;
      const uint32_t body_len = len - 9;
      pos += kFrameLenSize + len;

      if (type == RaftMsgType::kRequestVote) {
        // 投票判定是同步的（只读本地状态 + 一次小文件 fsync），
        // 不需要 co_await。
        const RequestVoteResp vr = node_->HandleRequestVote(body, body_len);
        out.clear();
        std::string resp_body;
        PutU64(&resp_body, vr.term);
        PutU8(&resp_body, vr.granted ? 1 : 0);
        FrameMessage(&out, RaftMsgType::kRequestVoteResp, seq, resp_body);

        size_t sent = 0;
        bool send_ok = true;
        while (sent < out.size()) {
          const int w = co_await proactor_->AsyncSend(fd, out.data() + sent,
                                                      out.size() - sent);
          if (w <= 0) {
            LOG(WARNING) << "raft: vote resp send failed: " << w;
            send_ok = false;
            break;
          }
          sent += static_cast<size_t>(w);
        }
        if (!send_ok) {
          proto_ok = false;
          break;
        }
        continue;
      }

      if (type == RaftMsgType::kReadIndex) {
        // leader 侧判定是同步的（查租约原子 + 读 commitIndex），无 co_await。
        const ReadIndexResp rr = node_->HandleReadIndex(body, body_len);
        out.clear();
        std::string resp_body;
        PutU64(&resp_body, rr.term);
        PutU8(&resp_body, rr.success ? 1 : 0);
        PutU64(&resp_body, rr.commit_index);
        FrameMessage(&out, RaftMsgType::kReadIndexResp, seq, resp_body);

        size_t sent = 0;
        bool send_ok = true;
        while (sent < out.size()) {
          const int w = co_await proactor_->AsyncSend(fd, out.data() + sent,
                                                      out.size() - sent);
          if (w <= 0) {
            LOG(WARNING) << "raft: readindex resp send failed: " << w;
            send_ok = false;
            break;
          }
          sent += static_cast<size_t>(w);
        }
        if (!send_ok) {
          proto_ok = false;
          break;
        }
        continue;
      }

      if (type != RaftMsgType::kAppendEntries) {
        LOG(WARNING) << "raft: inbound unexpected type "
                     << static_cast<int>(type);
        continue;
      }

      // 落盘 + Log Matching 检查（co_await：期间 main 继续干别的活）。
      const AppendEntriesResp ar =
          co_await node_->HandleAppendEntries(body, body_len);

      out.clear();
      std::string resp_body;
      PutU64(&resp_body, ar.term);
      PutU8(&resp_body, ar.success ? 1 : 0);
      PutU64(&resp_body, ar.conflict_index);
      PutU64(&resp_body, ar.match_index);
      FrameMessage(&out, RaftMsgType::kAppendEntriesResp, seq, resp_body);

      size_t sent = 0;
      bool send_ok = true;
      while (sent < out.size()) {
        const int w = co_await proactor_->AsyncSend(fd, out.data() + sent,
                                                    out.size() - sent);
        if (w <= 0) {
          LOG(WARNING) << "raft: inbound send failed: " << w;
          send_ok = false;
          break;
        }
        sent += static_cast<size_t>(w);
      }
      if (!send_ok) {
        proto_ok = false;
        break;
      }
    }
    if (!proto_ok) break;

    if (pos > 0) {
      std::memmove(buf.data(), buf.data() + pos, have - pos);
      have -= pos;
    }
  }

  ::close(fd);
  co_return;
}

}  // namespace dfly
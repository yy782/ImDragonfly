#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "io/uring_proactor.hpp"
#include "raft/raft_proto.hpp"
#include "util/cppcoro/async_task.hpp"

namespace dfly {

class RaftNode;

// follower 侧：接受 leader 的连接、解帧、把条目交给 RaftNode 落盘，回响应。
// **只在 main 线程使用**。
//
// follower 收到 AppendEntries 后：Log Matching 检查 → 落盘 → 按 leaderCommit
// 推进 commit_index_ 并重放已提交前缀到状态机（ApplyCommitted → ReplayEntry）。
class RaftInbound {
 public:
  RaftInbound(base::UringProactor* p, RaftNode* node, int listen_fd)
      : proactor_(p), node_(node), listen_fd_(listen_fd) {}

  // 点火 accept 循环。
  void Start();

 private:
  cppcoro::AsyncTask AcceptLoop();
  // 每条入向连接一个会话协程（leader 一般只有一条，但 leader 切换时
  // 可能短暂并存两条）。
  cppcoro::AsyncTask SessionLoop(int fd);

  base::UringProactor* proactor_;
  RaftNode* node_;
  int listen_fd_;
};

// 建一个**不带 SO_REUSEPORT** 的监听 socket。
// base::ListenFd() 设了 SO_REUSEPORT —— 那会让两个 raft 节点静默地共享
// 同一端口、请求被内核随机分发（本轮测试就被这个坑污染过一次结论）。
// raft 端口必须在被占用时明确报错。
int RaftListenFd(uint16_t port);

}  // namespace dfly
#pragma once

namespace dfly {

// raft 总开关。头文件里必须是 inline 定义，否则每个 TU 各自定义一份 →
// 多重定义链接错误。
// 注意：raft 的运行时全局标志（raft_replaying / IsRaftReplaying）定义在
// raft/raft_node.hpp —— 它们和 raft 模块强相关，放那里语义更清晰。
inline bool use_raft = false;

}  // namespace dfly
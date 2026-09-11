#pragma once

#include <cstdint>

namespace dfly {

enum class OpStatus : uint16_t {
  OK,
  KEY_NOTFOUND,
  WRONG_TYPE,
  SKIPPED,
  NO_KEY,
  OUT_OF_RANGE,
  SYNTAX_ERROR,
  INVALID_VALUE,  // 值存在但格式不正确
  RAFT_SCHED_FAIL,  // 事务调度失败（本节点非 leader，写未能进入 raft 日志）

  MULTIPLE_ERROR,  // 多个错误发生
  UNKNOWN_ERROR,   // 未知错误
};

}  // namespace dfly

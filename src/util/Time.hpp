#pragma once

#include <time.h>

namespace util {

inline uint64_t GetCurrentTimeMs() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return ts.tv_sec * 1000ULL + ts.tv_nsec / 1000000;
}

// 单调时钟（不受 NTP 校时 / 手动改系统时间影响）。
// raft 租约、超时这类"测量经过时间"的用途必须用它：用墙上时钟的话，
// 系统时间往回拨会让租约凭空延长（可能读到旧 leader），往前拨会让租约
// 无故失效。
inline uint64_t GetSteadyTimeMs() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1000ULL + ts.tv_nsec / 1000000;
}

}  // namespace util
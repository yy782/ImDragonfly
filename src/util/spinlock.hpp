#pragma once

#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <thread>

namespace util {

namespace detail {

// CPU 自旋等待提示：x86 生成 pause，ARM 生成 yield。
// 作用：(1) 暗示 CPU 处于 spin-wait，减少流水线预测惩罚；
//       (2) 降低功耗，避免 SMT 兄弟核的 turbo 频率被空烧拉高。
inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elif defined(__aarch64__)
  asm volatile("yield" ::: "memory");
#else
  asm volatile("" ::: "memory");
#endif
}

// Linux futex 系统调用封装：spin 阶段退化为睡眠时使用，避免空烧 CPU。
// futex_wait 的关键语义：内核在睡眠前原子地比较 *ftx == expected，
// 若不等则立即返回 EAGAIN，从而消除 "load 后未 sleep 前 unlock 已发生"
// 的丢唤醒窗口。
inline int futex_wait(std::atomic<uint32_t>* ftx, uint32_t expected) noexcept {
  return static_cast<int>(::syscall(SYS_futex, reinterpret_cast<uint32_t*>(ftx),
                                    FUTEX_WAIT_PRIVATE, expected, nullptr,
                                    nullptr, 0));
}

inline int futex_wake(std::atomic<uint32_t>* ftx, int n = 1) noexcept {
  return static_cast<int>(::syscall(SYS_futex, reinterpret_cast<uint32_t*>(ftx),
                                    FUTEX_WAKE_PRIVATE, n, nullptr, nullptr,
                                    0));
}

}  // namespace detail

// SpinLock: 三段式退避自旋锁，保持 Lockable 语义。
//   1. 快路径：单次 CAS，无竞争时 O(1)
//   2. spin 阶段：100 次 cpu_relax + CAS，中等竞争省 CPU
//   3. sleep 阶段：标记 kSleeper 后 futex_wait 进内核睡眠，
//      unlock 检测到 kSleeper 后 futex_wake 唤醒一个等待者。
//
// 注意：本锁非公平（FIFO），高竞争下可能饿死某些线程。
class SpinLock {
 public:
  SpinLock() = default;
  SpinLock(const SpinLock&) = delete;
  SpinLock& operator=(const SpinLock&) = delete;

  void lock() {
    uint32_t expected = 0;
    if (lockword_.compare_exchange_strong(expected, kLocked,
                                          std::memory_order_acquire)) {
      return;
    }
    slowLock();
  }

  bool try_lock() {
    uint32_t expected = 0;
    return lockword_.compare_exchange_strong(expected, kLocked,
                                             std::memory_order_acquire);
  }

  void unlock() {
    uint32_t prev = lockword_.exchange(0, std::memory_order_release);
    if (prev & kSleeper) {
      detail::futex_wake(&lockword_, 1);
    }
  }

 private:
  static constexpr uint32_t kLocked = 1;
  static constexpr uint32_t kSleeper = 2;
  static constexpr int kSpinRounds = 100;

  void slowLock() {
    for (int i = 0; i < kSpinRounds; ++i) {
      uint32_t expected = lockword_.load(std::memory_order_relaxed);
      if ((expected & kLocked) == 0) {
        if (lockword_.compare_exchange_weak(expected, kLocked,
                                            std::memory_order_acquire)) {
          return;
        }
        if ((expected & kLocked) == 0) {
          --i;
          continue;
        }
      }
      detail::cpu_relax();
    }

    while (true) {
      uint32_t expected = lockword_.load(std::memory_order_relaxed);
      if ((expected & kLocked) == 0) {
        if (lockword_.compare_exchange_weak(expected, kLocked,
                                            std::memory_order_acquire)) {
          return;
        }
        continue;
      }
      if ((expected & kSleeper) == 0) {
        if (!lockword_.compare_exchange_weak(expected, expected | kSleeper,
                                             std::memory_order_relaxed)) {
          continue;
        }
        expected |= kSleeper;
      }
      detail::futex_wait(&lockword_, expected);
    }
  }

  std::atomic<uint32_t> lockword_{0};
};

}  // namespace util

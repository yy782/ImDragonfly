#pragma once 

#include <atomic>
#include <thread>
namespace util{

class SpinLock { // TODO 告诉CPU在空转
public:
    SpinLock() : lockword_(0) {}
    
    SpinLock(const SpinLock&) = delete;
    SpinLock& operator=(const SpinLock&) = delete;
    
    void lock() {
        // 快速尝试：如果锁空闲，直接获取
        uint32_t expected = 0;
        if (lockword_.compare_exchange_strong(expected, kLocked, 
                                               std::memory_order_acquire)) { 
            return;  // 快速路径成功
        }
        // 慢速路径：锁被占用，进入自旋等待
        slowLock();
    }
    
    bool try_lock() {
        uint32_t expected = 0;
        return lockword_.compare_exchange_strong(expected, kLocked,
                                                  std::memory_order_acquire);
    }
    
    void unlock() {
        // 释放锁，确保写操作对其他线程可见
        lockword_.store(0, std::memory_order_release);
    }
    
private:
    static constexpr uint32_t kLocked = 1;
    static constexpr uint32_t kSleeper = 8;  // 有等待者标志
    
    void slowLock() {
        uint32_t expected;
        
        for (int i = 0; i < 100; ++i) {
            expected = lockword_.load(std::memory_order_relaxed);
            
            if ((expected & kLocked) == 0) {
                if (lockword_.compare_exchange_weak(expected, kLocked,
                                                    std::memory_order_acquire)) {
                    return;
                }
            }
            
            // 退避策略：自旋等待
            if (i < 50) {
                // 前50次：简单自旋
                for (volatile int j = 0; j < 10; ++j) {}
            } else {
                std::this_thread::yield();
            }
        }
        
        // 长时间等待，标记有等待者
        expected = lockword_.load(std::memory_order_relaxed);
        while (true) {
            if ((expected & kLocked) == 0) {
                if (lockword_.compare_exchange_weak(expected, kLocked,
                                                    std::memory_order_acquire)) {
                    return;
                }
            } else {
                // 锁被持有，设置 sleeper 标志
                if (lockword_.compare_exchange_weak(expected, expected | kSleeper,
                                                    std::memory_order_relaxed)) {
                    expected |= kSleeper;
                }
            }
            
            // 继续自旋
            for (volatile int j = 0; j < 100; ++j) {}
            expected = lockword_.load(std::memory_order_relaxed);
        }
    }
    
    std::atomic<uint32_t> lockword_;
};

}
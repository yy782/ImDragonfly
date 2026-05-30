#pragma once 

#include <atomic>
#include <thread>
namespace util{

class SpinLock { 
public:
    SpinLock() : lockword_(0) {}
    
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
        lockword_.store(0, std::memory_order_release);
    }
    
private:
    static constexpr uint32_t kLocked = 1;
    static constexpr uint32_t kSleeper = 8; 
    
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
            
            if (i < 50) {
                for (int j = 0; j < 10; ++j) {
                    asm volatile("" ::: "memory");  
                }
            } else {
                std::this_thread::yield();
            }
        }
        
        expected = lockword_.load(std::memory_order_relaxed);
        while (true) {
            if ((expected & kLocked) == 0) {
                if (lockword_.compare_exchange_weak(expected, kLocked,
                                                    std::memory_order_acquire)) {
                    return;
                }
            } else {
                if (lockword_.compare_exchange_weak(expected, expected | kSleeper,
                                                    std::memory_order_relaxed)) {
                    expected |= kSleeper;
                }
            }
            
            for (int j = 0; j < 100; ++j) {
                asm volatile("" ::: "memory");
            }
            expected = lockword_.load(std::memory_order_relaxed);
        }
    }
    
    std::atomic<uint32_t> lockword_;
};

}
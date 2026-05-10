// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
// synchronization

#pragma once
#include <mutex>
#include <atomic>
#include <cassert>
#include <condition_variable>  // for cv_status
#include <optional>
#include <functional>
#include <boost/intrusive_ptr.hpp>
#include "cppcoro/async_mutex.hpp"
#include "cppcoro/task.hpp"
#include "util/wait_queue.hpp"
#include "util/spinlock.hpp"
namespace util {

class EventCount {
public:
    EventCount() noexcept : val_(0) {
    }

    using cv_status = std::cv_status;

    class Key {
        friend class EventCount;
        EventCount* me_;
        uint32_t epoch_;

        explicit Key(EventCount* me, uint32_t e) noexcept : me_(me), epoch_(e) {
        }

        Key(const Key&) = delete;
    public:
        Key(Key&& o) noexcept : me_{o.me_}, epoch_{o.epoch_} {
            o.me_ = nullptr;
        };

        ~Key() {
        if (me_ != nullptr)
            me_->val_.fetch_sub(kAddWaiter, std::memory_order_relaxed);
        }

        uint32_t epoch() const {
            return epoch_;
        }
    };

    bool notify() noexcept {
        return NotifyInternal(&detail::WaitQueue::NotifyOne);
    }

    bool notifyAll() noexcept {
        return NotifyInternal(&detail::WaitQueue::NotifyAll);
    }

    template <typename Condition>
    cppcoro::task<bool> await(Condition condition){
        if (condition()) {
            std::atomic_thread_fence(std::memory_order_acquire);
            co_return false;  // fast path
        }
        bool preempt = false;
        while (true) {
            Key key = prepareWait(); 
            if (condition()) {
                std::atomic_thread_fence(std::memory_order_acquire);
                break;
            }
            preempt |= co_await wait(key.epoch());
        }
        co_return preempt;
    }


    Key prepareWait() noexcept {
        uint64_t prev = val_.fetch_add(kAddWaiter, std::memory_order_acq_rel);
        return Key(this, prev >> kEpochShift);
    }

    void finishWait() noexcept {
        // We need this barrier to ensure that notify()/notifyAll() has finished before we return.
        // This is necessary because we want to avoid the case where continue to wait for
        // another eventcount/condition_variable and have two notify functions waking up the same
        // fiber at the same time.
        lock_.lock();
        lock_.unlock();
    }


    auto wait(uint32_t epoch) noexcept{
        struct WaitAwaitable{
            EventCount* event_;
            uint32_t epoch_;



            bool SuspendWithResume = true;
            detail::Waiter waiter {};

            bool await_ready() const noexcept
            {
                return false;
            }
            bool await_suspend(
                std::coroutine_handle<> awaitingCoroutine) noexcept
              {
                    std::unique_lock lk(event_->lock_); 
                    if((event_->val_.load(std::memory_order_relaxed) >> event_->kEpochShift) == epoch_){
                            waiter.handler = awaitingCoroutine;
                            event_->wait_queue_.Link(&waiter);
                            lk.unlock();
                            SuspendWithResume = true;
                    }
                    else {
                            SuspendWithResume = false;
                            lk.unlock();
                    }
                    return SuspendWithResume;
              }


              bool await_resume() 
              {
                    if(SuspendWithResume) {
                        event_->finishWait();
                    }

                    return SuspendWithResume;
              }
        };
        return WaitAwaitable{this, epoch};
    }


private:
  friend class Key;

  EventCount(const EventCount&) = delete;
  EventCount(EventCount&&) = delete;
  EventCount& operator=(const EventCount&) = delete;
  EventCount& operator=(EventCount&&) = delete;

  // Run notify function on wait queue if any waiter is active
    bool NotifyInternal(bool (detail::WaitQueue::*f)()) noexcept{
        uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_release);
        if (prev & kWaiterMask) {
            std::unique_lock lk(lock_);
            return (wait_queue_.*f)();
        }
        return false;
    }
  std::atomic_uint64_t val_;

  SpinLock lock_;  // protects wait_queue
  detail::WaitQueue wait_queue_;

  static constexpr uint64_t kAddWaiter = 1ULL;
  static constexpr size_t kEpochShift = 32;
  static constexpr uint64_t kAddEpoch = 1ULL << kEpochShift;
  static constexpr uint64_t kWaiterMask = kAddEpoch - 1;
};


template<typename Mutex>
using LockGuard = std::lock_guard<Mutex>;


class Mutex {
private:
    cppcoro::async_mutex mtx_;
public:
    Mutex() = default;
    ~Mutex() = default;

    Mutex(Mutex const&) = delete;
    Mutex& operator=(Mutex const&) = delete;

    auto lock() {
        return mtx_.lock_async();
    }

    bool try_lock(){
        return mtx_.try_lock();
    }

    void unlock(){
        return mtx_.unlock();
    }
  
};


class Done {

public:
    enum DoneWaitDirective { 
        AND_NOTHING = 0, 
        AND_RESET = 1 
    };

    Done() : impl_(new Impl) {
    }
    ~Done() {

    }

    void Notify() {
        impl_->Notify();
    }
    auto Wait(DoneWaitDirective reset = AND_NOTHING) {
        return impl_->Wait(reset);
    }

    void Reset() {
        impl_->Reset();
    }

private:
    class Impl {
    public:
        Impl() : ready_(false) {
        }
        Impl(const Impl&) = delete;
        void operator=(const Impl&) = delete;

        friend void intrusive_ptr_add_ref(Impl* done) noexcept {
            done->use_count_.fetch_add(1, std::memory_order_relaxed);
        }

        friend void intrusive_ptr_release(Impl* impl) noexcept {
            if (1 == impl->use_count_.fetch_sub(1, std::memory_order_release)) {
                std::atomic_thread_fence(std::memory_order_acquire);
                delete impl;
            }
        }

        cppcoro::task<bool> Wait(DoneWaitDirective reset) {
            auto res = co_await ec_.await([this] { return ready_.load(std::memory_order_acquire); });
            if (reset == AND_RESET)
                ready_.store(false, std::memory_order_release);
            co_return res;
        }

        // We use EventCount to wake threads without blocking.
        void Notify() {
            ready_.store(true, std::memory_order_release);
            ec_.notify();
        }

        void Reset() {
            ready_ = false;
        }

        bool IsReady() const {
            return ready_.load(std::memory_order_acquire);
        }
        EventCount ec_;
        std::atomic<std::uint32_t> use_count_{0};
        std::atomic<bool> ready_;
    };
    using ptr_t = ::boost::intrusive_ptr<Impl>;
    ptr_t impl_;
};


// class EmbeddedBlockingCounter {
// public:
//     EmbeddedBlockingCounter(unsigned start_count = 0) : ec_{}, count_{start_count} {
//     }

//     // Returns true on success (reaching 0), false when cancelled. Acquire semantics
//     bool Wait(){
//         uint64_t cnt;
//         ec_.await(WaitCondition(&cnt));
//         return (cnt & kCancelFlag) == 0;
//     }


//     // Start with specified count. Current value must be strictly zero (not cancelled).
//     void Start(unsigned cnt) {
//         count_.store(cnt, std::memory_order_relaxed);
//     }

//     // Add to blocking counter
//     void Add(unsigned cnt = 1) {
//         count_.fetch_add(cnt, std::memory_order_relaxed);
//     }

//     // Decrement from blocking counter. Release semantics.
//     void Dec(){
//         uint64_t prev = count_.fetch_sub(1, std::memory_order_acq_rel);
//         if (prev == 1)
//             ec_.notifyAll();
//     }

//     // Cancel blocking counter, unblock wait. Release semantics.
//     void Cancel(){
//         count_.fetch_or(kCancelFlag, std::memory_order_acq_rel);
//         ec_.notifyAll();
//     }


//     // Return true if count is zero or cancelled. Has acquire semantics to be used in if checks
//     bool IsCompleted() const{
//         uint64_t v = 0;
//         bool result = WaitCondition(&v)();
//         if (result)  // acquire semantics for "if completed, then action"
//             std::atomic_thread_fence(std::memory_order_acquire);
//         return result;
//     }

// private:
//     const uint64_t kCancelFlag = (1ULL << 63);

//     // Re-usable functor for wait condition, stores result in provided pointer
//     std::function<bool()> WaitCondition(uint64_t* cnt) const {
//         return [this, cnt]() -> bool {
//             *cnt = count_.load(std::memory_order_relaxed);  // EventCount provides acquire
//             return *cnt == 0 || (*cnt & kCancelFlag);
//         };
//     }
  
//     EventCount ec_;
//     std::atomic<uint64_t> count_;
// };


// class BlockingCounter {
//  public:
//   BlockingCounter(unsigned start_count) : 
//       counter_{std::make_shared<EmbeddedBlockingCounter>(start_count)} {}

//   EmbeddedBlockingCounter* operator->() {
//       return counter_.get();
//   }

//  private:
//   std::shared_ptr<EmbeddedBlockingCounter> counter_;
// };




class ThreadEvent {
public:
    void wait() {
        std::unique_lock<std::mutex> lock(m_);
        cv_.wait(lock, [this] { 
            return flag_.load(std::memory_order_acquire) > 0; 
        });

        flag_.fetch_sub(1, std::memory_order_release);
    }

    void notify() {
   
        flag_.fetch_add(1, std::memory_order_release);
        cv_.notify_one();
    }

    void notifyAll() {

        flag_.store(INT32_MAX, std::memory_order_release);
        cv_.notify_all();
    }


    void reset() {
        flag_.store(0, std::memory_order_release);
    }

private:
    std::mutex m_;
    std::condition_variable cv_;
    std::atomic<int32_t> flag_{0};  
};



}  // namespace util

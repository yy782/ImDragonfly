// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>  // for cv_status
#include <optional>

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
            return false;  // fast path
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

            bool SuspendWithResume = false;
            bool await_ready() const noexcept
            {
                return false;
            }
            void await_suspend(
                std::coroutine_handle<> awaitingCoroutine) noexcept
              {
                  std::unique_lock lk(this_->lock_); 
                  if((event_->val_.load(std::memory_order_relaxed) >> event_->kEpochShift) == epoch_){
                      detail::Waiter waiter{awaitingCoroutine};
                      event_->wait_queue_.Link(&waiter);
                      lk.unlock();
                      SuspendWithResume = true;
                  }
                  else {
                      lk.unlock();
                      awaitingCoroutine.resume();
                  }
              }


              bool await_resume() 
              {
                  if(SuspendWithResume) {
                      event_->finishWait();
                      return true;
                  }
                  return false;
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

  ::util::SpinLock lock_;  // protects wait_queue
  detail::WaitQueue wait_queue_;

  static constexpr uint64_t kAddWaiter = 1ULL;
  static constexpr size_t kEpochShift = 32;
  static constexpr uint64_t kAddEpoch = 1ULL << kEpochShift;
  static constexpr uint64_t kWaiterMask = kAddEpoch - 1;
};



template<typename Mutex>
using LockGuard<Mutex> = std::lock_guard<Mutex>;



class Mutex {
private:
    friend class CondVar;

    friend struct LockAwaitable; 
    friend struct TryLockAwaitable;

    ::util::SpinLock wait_queue_splk_;
    detail::WaitQueue wait_queue_;
    std::coroutine_handle<> owner_{nullptr};

public:
    Mutex() = default;

    ~Mutex() {
      assert(!owner_);
    }

    Mutex(Mutex const&) = delete;
    Mutex& operator=(Mutex const&) = delete;

    cppcoro::task<void> lock() {
        struct LockAwaitable{
            Mutex* mtx_;

            bool HasSuspend = false;
            bool await_ready() const noexcept
            {
                return false;
            }            

            void await_suspend(
                std::coroutine_handle<> awaitingCoroutine) noexcept
              {

                    detail::Waiter waiter{awaitingCoroutine};
                    mtx_->wait_queue_splk_.lock();
                    if (nullptr == mtx_->owner_) {
                        mtx_->owner_ = awaitingCoroutine;
                        mtx_->wait_queue_splk_.unlock();
                        awaitingCoroutine.resume();
                    }
                    else{

                        mtx_->wait_queue_.Link(&waiter);
                        mtx_->wait_queue_splk_.unlock();
                        HasSuspend = true;                        
                    }
                  
              }
              bool await_resume() {
                return HasSuspend;
              }
        };
        while(co_await LockAwaitable{this}){}  
        co_return;

    }

    cppcoro::task<bool> try_lock(){
        struct TryLockAwaitable{
            Mutex* mtx_;
            bool GetLock = false;

            bool await_ready() const noexcept
            {
                return false;
            }            

            void await_suspend(
                    std::coroutine_handle<> awaitingCoroutine) noexcept
                  {
                      std::unique_lock lk{mtx_->wait_queue_splk_};
                      if (nullptr == mtx_->owner_) {
                          mtx_->owner_ = awaitingCoroutine;
                          GetLock = true;
                      }
                      lk.unlock();
                      awaitingCoroutine.resume();
                  }
            bool await_resume() {
              return GetLock;
            }
                       
        };
        co_return TryLockAwaitable{this};
    }

    void unlock(){
        std::unique_lock lk(wait_queue_splk_);
        owner_ = nullptr;
        wait_queue_.NotifyOne();
    }
  
};



class CondVarAny {
  detail::WaitQueue wait_queue_;

  std::cv_status PostWaitTimeout(detail::Waiter waiter, bool clean_remote,
                                 detail::FiberInterface* active);

 public:
  CondVarAny() = default;

  ~CondVarAny() {
    assert(wait_queue_.empty());
  }

  CondVarAny(CondVarAny const&) = delete;
  CondVarAny& operator=(CondVarAny const&) = delete;

  // in contrast to std::condition_variable::notify_one() isn't thread-safe and should be called
  // under the mutex
  void notify_one() noexcept {
    if (!wait_queue_.empty())
      wait_queue_.NotifyOne(detail::FiberActive());
  }

  // in contrast to std::condition_variable::notify_all() isn't thread-safe and should be called
  // under the mutex
  void notify_all() noexcept {
    if (!wait_queue_.empty())
      wait_queue_.NotifyAll(detail::FiberActive());
  }

  template <typename LockType> void wait(LockType& lt);

  template <typename LockType, typename Pred> void wait(LockType& lt, Pred pred) {
    while (!pred()) {
      wait(lt);
    }
  }

  template <typename LockType>
  std::cv_status wait_until(LockType& lt, std::chrono::steady_clock::time_point tp);

  template <typename LockType, typename Pred>
  bool wait_until(LockType& lt, std::chrono::steady_clock::time_point tp, Pred pred) {
    while (!pred()) {
      if (std::cv_status::timeout == wait_until(lt, tp)) {
        return pred();
      }
    }
    return true;
  }

  template <typename LockType>
  std::cv_status wait_for(LockType& lt, std::chrono::steady_clock::duration dur) {
    return wait_until(lt, std::chrono::steady_clock::now() + dur);
  }

  template <typename LockType, typename Pred>
  bool wait_for(LockType& lt, std::chrono::steady_clock::duration dur, Pred pred) {
    return wait_until(lt, std::chrono::steady_clock::now() + dur, pred);
  }
};



class CondVar {
 private:
  CondVarAny cnd_;

 public:
  CondVar() = default;

  CondVar(CondVar const&) = delete;
  CondVar& operator=(CondVar const&) = delete;

  // in contrast to std::condition_variable::notify_one() isn't thread-safe and should be called
  // under the mutex
  void notify_one() noexcept {
    cnd_.notify_one();
  }

  // in contrast to std::condition_variable::notify_all() isn't thread-safe and should be called
  // under the mutex
  void notify_all() noexcept {
    cnd_.notify_all();
  }

  void wait(std::unique_lock<Mutex>& lt) {
    // pre-condition
    cnd_.wait(lt);
  }

  template <typename Pred> void wait(std::unique_lock<Mutex>& lt, Pred pred) {
    cnd_.wait(lt, pred);
  }

  template <typename Clock, typename Duration>
  std::cv_status wait_until(std::unique_lock<Mutex>& lt,
                            std::chrono::time_point<Clock, Duration> const& timeout_time) {
    // pre-condition
    std::cv_status result = cnd_.wait_until(lt, timeout_time);
    // post-condition
    return result;
  }

  template <typename Clock, typename Duration, typename Pred>
  bool wait_until(std::unique_lock<Mutex>& lt,
                  std::chrono::time_point<Clock, Duration> const& timeout_time, Pred pred) {
    bool result = cnd_.wait_until(lt, timeout_time, pred);
    return result;
  }

  template <typename Rep, typename Period>
  std::cv_status wait_for(std::unique_lock<Mutex>& lt,
                          std::chrono::duration<Rep, Period> const& timeout_duration) {
    std::cv_status result = cnd_.wait_for(lt, timeout_duration);
    return result;
  }

  template <typename Rep, typename Period, typename Pred>
  bool wait_for(std::unique_lock<Mutex>& lt,
                std::chrono::duration<Rep, Period> const& timeout_duration, Pred pred) {
    bool result = cnd_.wait_for(lt, timeout_duration, pred);
    return result;
  }
};



class Done {
  class Impl;

 public:
  enum DoneWaitDirective { AND_NOTHING = 0, AND_RESET = 1 };

  Done() : impl_(new Impl) {
  }
  ~Done() {
  }

  void Notify() {
    impl_->Notify();
  }
  bool Wait(DoneWaitDirective reset = AND_NOTHING) {
    return impl_->Wait(reset);
  }

  // Returns true if Done was notified, false if timeout reached.
  bool WaitFor(const std::chrono::steady_clock::duration& duration) {
    return impl_->WaitFor(duration);
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

    bool Wait(DoneWaitDirective reset) {
      bool res = ec_.await([this] { return ready_.load(std::memory_order_acquire); });
      if (reset == AND_RESET)
        ready_.store(false, std::memory_order_release);
      return res;
    }

    // Returns true if predicate became true, false if timeout reached.
    bool WaitFor(const std::chrono::steady_clock::duration& duration) {
      auto tp = std::chrono::steady_clock::now() + duration;
      std::cv_status status =
          ec_.await_until([this] { return ready_.load(std::memory_order_acquire); }, tp);
      return status == std::cv_status::no_timeout;
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

   private:
    EventCount ec_;
    std::atomic<std::uint32_t> use_count_{0};
    std::atomic_bool ready_;
  };

  using ptr_t = ::boost::intrusive_ptr<Impl>;
  ptr_t impl_;
};

// Use `BlockingCounter` unless certain that the counters lifetime is managed properly.
// Because the decrement of Dec() can be observed before notify is called, the counter can be still
// in use even after Wait() unblocked.
class EmbeddedBlockingCounter {
  const uint64_t kCancelFlag = (1ULL << 63);

  // Re-usable functor for wait condition, stores result in provided pointer
  auto WaitCondition(uint64_t* cnt) const {
    return [this, cnt]() -> bool {
      *cnt = count_.load(std::memory_order_relaxed);  // EventCount provides acquire
      return *cnt == 0 || (*cnt & kCancelFlag);
    };
  }

 public:
  EmbeddedBlockingCounter(unsigned start_count = 0) : ec_{}, count_{start_count} {
  }

  // Returns true on success (reaching 0), false when cancelled. Acquire semantics
  bool Wait();

  // Same as Wait(), but with timeout
  bool WaitFor(const std::chrono::steady_clock::duration& duration) {
    return WaitUntil(std::chrono::steady_clock::now() + duration);
  }

  bool WaitUntil(const std::chrono::steady_clock::time_point tp);

  // Start with specified count. Current value must be strictly zero (not cancelled).
  void Start(unsigned cnt);

  // Add to blocking counter
  void Add(unsigned cnt = 1) {
    count_.fetch_add(cnt, std::memory_order_relaxed);
  }

  // Decrement from blocking counter. Release semantics.
  void Dec();

  // Cancel blocking counter, unblock wait. Release semantics.
  void Cancel();

  // Notify waiter when completed. Return null if already completed (no registration happens).
  // Caller must hold the returned key to keep registered and drop it to re-use the waiter.
  std::optional<EventCount::SubKey> OnCompletion(detail::Waiter* w);

  // Return true if count is zero or cancelled. Has acquire semantics to be used in if checks
  bool IsCompleted() const;

  uint64_t DEBUG_Count() const;

 private:
  EventCount ec_;
  std::atomic<uint64_t> count_;
};

// A barrier similar to Go's WaitGroup for tracking remote tasks.
// Internal smart pointer for easier lifetime management. Pass by value.
class BlockingCounter {
 public:
  BlockingCounter(unsigned start_count);

  EmbeddedBlockingCounter* operator->() {
    return counter_.get();
  }

 private:
  std::shared_ptr<EmbeddedBlockingCounter> counter_;
};

class SharedMutex {
 public:
  bool try_lock() ABSL_EXCLUSIVE_TRYLOCK_FUNCTION(true) {
    uint32_t expect = 0;
    return state_.compare_exchange_strong(expect, WRITER, std::memory_order_acq_rel);
  }

  void lock() ABSL_EXCLUSIVE_LOCK_FUNCTION() {
    ec_.await([this] { return try_lock(); });
  }

  bool try_lock_shared() ABSL_SHARED_TRYLOCK_FUNCTION(true) {
    uint32_t value = state_.fetch_add(READER, std::memory_order_acquire);
    if (value & WRITER) {
      state_.fetch_add(-READER, std::memory_order_release);
      return false;
    }
    return true;
  }

  void lock_shared() ABSL_SHARED_LOCK_FUNCTION() {
    ec_.await([this] { return try_lock_shared(); });
  }

  void unlock() ABSL_UNLOCK_FUNCTION() {
    state_.fetch_and(~(WRITER), std::memory_order_relaxed);
    ec_.notifyAll();
  }

  void unlock_shared() ABSL_UNLOCK_FUNCTION() {
    state_.fetch_add(-READER, std::memory_order_relaxed);
    ec_.notifyAll();
  }

 private:
  enum : int32_t { READER = 4, WRITER = 1 };
  EventCount ec_;
  std::atomic_uint32_t state_{0};
};

struct NoOpLock {
  void lock() {
  }
  void unlock() {
  }

  bool try_lock() {
    return true;
  }
};

class Barrier {
 public:
  explicit Barrier(std::size_t initial);

  Barrier(Barrier const&) = delete;
  Barrier& operator=(Barrier const&) = delete;

  bool Wait();
  void Cancel();

 private:
  std::size_t initial_;
  std::size_t current_;
  std::size_t cycle_{0};
  Mutex mtx_;
  CondVar cond_;
};



























template <typename LockType> void CondVarAny::wait(LockType& lt) {
  detail::FiberInterface* active = detail::FiberActive();

  detail::Waiter waiter(active->CreateWaiter());

  wait_queue_.Link(&waiter);
  lt.unlock();

  active->Suspend();

  // relock external again before returning
  try {
    lt.lock();
  } catch (...) {
    std::terminate();
  }
}

template <typename LockType>
std::cv_status CondVarAny::wait_until(LockType& lt, std::chrono::steady_clock::time_point tp) {
  detail::FiberInterface* active = detail::FiberActive();

  detail::Waiter waiter(active->CreateWaiter());

  // store this fiber in waiting-queue, we can do it without spinlocks because
  // lt is already locked.
  wait_queue_.Link(&waiter);

  // release the lock suspend this fiber until tp.
  lt.unlock();
  bool timed_out = active->WaitUntil(tp);

  // lock back.
  lt.lock();
  std::cv_status status = PostWaitTimeout(std::move(waiter), timed_out, active);
  assert(!waiter.IsLinked());
  return status;
}

inline bool EmbeddedBlockingCounter::Wait() {
  uint64_t cnt;
  ec_.await(WaitCondition(&cnt));
  return (cnt & kCancelFlag) == 0;
}

}  // namespace fb2

using fb2::BlockingCounter;

template <typename Pred> void Await(fb2::CondVarAny& cv, Pred&& pred) {
  fb2::NoOpLock lock;
  cv.wait(lock, std::forward<Pred>(pred));
}

}  // namespace util

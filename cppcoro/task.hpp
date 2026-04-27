#pragma once

#include "detail/awaiter_impl.hpp"
#include <atomic>
#include <exception>
#include <utility>
#include <type_traits>
#include <cstdint>
#include <cassert>
#include <coroutine>

namespace cppcoro
{
template<typename T> class task;

namespace detail
{

template<bool isSuspend = true>
class task_promise_base
{
  friend struct final_awaitable;
public:

  task_promise_base() noexcept
  {}

  auto initial_suspend() noexcept
  {
    if constexpr (isSuspend)
    {
      return std::suspend_always{};
    }
    else 
    {
      return std::suspend_never{};
    }
  }

  auto final_suspend() noexcept
  {
    if constexpr (isSuspend)
    {
      return final_awaitable{};
    }
    else 
    {
      return std::suspend_never{};
    }
  }



  void set_continuation(std::coroutine_handle<> continuation) noexcept
  {
      continuation_ = continuation;
  }

protected:

  std::coroutine_handle<> continuation_;


  struct final_awaitable
  {
    bool await_ready() const noexcept { return false; }
    template<typename PROMISE>
				std::coroutine_handle<> await_suspend(
					std::coroutine_handle<PROMISE> coro) noexcept
				{
					return coro.promise().continuation_;
				}
    void await_resume() noexcept {}
  };


};

template<typename T, bool isSuspend = true>
class task_promise final : public task_promise_base<isSuspend>
{
public:

  task_promise() noexcept {}

  ~task_promise()
  {
    switch (resultType_)
    {
      case result_type::value:
        value_.~T();
        break;
      case result_type::exception:
        exception_.~exception_ptr();
        break;
      default:
        break;
    }
  }

  task<T> get_return_object() noexcept;

  void unhandled_exception() noexcept
  {
    ::new (static_cast<void*>(std::addressof(exception_))) std::exception_ptr(
      std::current_exception());
    resultType_ = result_type::exception;
  }

  template<typename VALUE>
  requires std::is_convertible_v<VALUE&&, T>
  void return_value(VALUE&& value)
  noexcept(std::is_nothrow_constructible_v<T, VALUE&&>)
  {
    ::new (static_cast<void*>(std::addressof(value_))) T(std::forward<VALUE>(value));
    resultType_ = result_type::value;
  }

  T& result() &
  {
    if (resultType_ == result_type::exception)
    {
      std::rethrow_exception(exception_);
    }

    assert(resultType_ == result_type::value);

    return value_;
  }

  using rvalue_type = T&&;

  rvalue_type result() &&
  {
    if (resultType_ == result_type::exception)
    {
      std::rethrow_exception(exception_);
    }

    assert(resultType_ == result_type::value);

    return std::move(value_);
  }

private:

  enum class result_type { empty, value, exception };

  result_type resultType_ = result_type::empty;

  union
  {
    T value_;
    std::exception_ptr exception_;
  };

};


}

/// \brief
/// A task represents an operation that produces a result both lazily
/// and asynchronously.
///
/// When you call a coroutine that returns a task, the coroutine
/// simply captures any passed parameters and returns exeuction to the
/// caller. Execution of the coroutine body does not start until the
/// coroutine is first co_await'ed.
template<typename T = void, typename TaskPromise = task_promise<T>>
class [[nodiscard]] task
{
  using promise_type = TaskPromise;

  using value_type = T;
public:

  task() noexcept
    : coroutine_(nullptr)
  {}

  explicit task(std::coroutine_handle<promise_type> coroutine)
    : coroutine_(coroutine)
  {}

  task(task&& t) noexcept
    : coroutine_(t.coroutine_)
  {
    t.coroutine_ = nullptr;
  }

  /// Disable copy construction/assignment.
  task(const task&) = delete;
  task& operator=(const task&) = delete;

  /// Frees resources used by this task.
  ~task()
  {
    if (coroutine_)
    {
      coroutine_.destroy();
    }
  }

  task& operator=(task&& other) noexcept
  {
    if (std::addressof(other) != this)
    {
      if (coroutine_)
      {
        coroutine_.destroy();
      }

      coroutine_ = other.coroutine_;
      other.coroutine_ = nullptr;
    }

    return *this;
  }

  /// \brief
  /// Query if the task result is complete.
  ///
  /// Awaiting a task that is ready is guaranteed not to block/suspend.
  bool is_ready() const noexcept
  {
    return !coroutine_ || coroutine_.done();
  }

  auto operator co_await() const & noexcept
  {


    return awaitable{ coroutine_ };
  }

  auto operator co_await() const && noexcept
  {


    return awaitable{ coroutine_ };
  }

  /// \brief
  /// Returns an awaitable that will await completion of the task without
  /// attempting to retrieve the result.
  auto when_ready() const noexcept
  {
    struct awaitable : awaitable_base
    {
      using awaitable_base::awaitable_base; // // 继承基类的构造函数

      void await_resume() const noexcept {}
    };

    return awaitable{ coroutine_ };
  }

protected:

  struct awaitable_base
  {
    std::coroutine_handle<promise_type> coroutine_;

    awaitable_base(std::coroutine_handle<promise_type> coroutine) noexcept
      : coroutine_(coroutine)
    {}

    bool await_ready() const noexcept
    {
      return !coroutine_ || coroutine_.done();
    }


    std::coroutine_handle<> await_suspend(
				std::coroutine_handle<> awaitingCoroutine) noexcept
			{
				coroutine_.promise().set_continuation(awaitingCoroutine);
				return coroutine_;
			}


      decltype(auto) await_resume() &&
      {
        if (!this->coroutine_)
        {
          throw broken_promise{};
        }

        return std::move(this->coroutine_.promise()).result();
      }

      decltype(auto) await_resume() &
      {
        if (!this->coroutine_)
        {
          throw broken_promise{};
        }

        return this->coroutine_.promise().result();
      }


  };
  std::coroutine_handle<promise_type> coroutine_;

};




namespace detail  
{

template<>
class task_promise<void> final : public task_promise_base
{
public:

  task_promise() noexcept = default;

  task<void> get_return_object() noexcept;

  void return_void() noexcept
  {}

  void unhandled_exception() noexcept
  {
    exception_ = std::current_exception();
  }

  void result()
  {
    if (exception_)
    {
      std::rethrow_exception(exception_);
    }
  }

private:

  std::exception_ptr exception_;

};
template<typename T>
class task_promise<T&> final : public task_promise_base
{
public:

  task_promise() noexcept = default;

  task<T&> get_return_object() noexcept;

  void unhandled_exception() noexcept
  {
    exception_ = std::current_exception();
  }

  void return_value(T& value) noexcept
  {
    value_ = std::addressof(value);
  }

  T& result()
  {
    if (exception_)
    {
      std::rethrow_exception(exception_);
    }

    return *value_;
  }

private:

  T* value_ = nullptr;
  std::exception_ptr exception_;

};



template<typename T>
task<T> task_promise<T>::get_return_object() noexcept
{
  return task<T>{ std::coroutine_handle<task_promise>::from_promise(*this) };
}

inline task<void> task_promise<void>::get_return_object() noexcept
{
  return task<void>{ std::coroutine_handle<task_promise>::from_promise(*this) };
}

template<typename T>
cppcoro::task<T&> task_promise<T&>::get_return_object() noexcept
{
  return task<T&>{ std::coroutine_handle<task_promise>::from_promise(*this) };
}

template<typename AWAITABLE>
auto make_task(AWAITABLE awaitable)
-> task<remove_rvalue_reference_t<typename awaitable_traits<AWAITABLE>::await_result_t>>
{
  co_return co_await static_cast<AWAITABLE&&>(awaitable);
}
}
}


#endif //XYNET_COROUTINE_TASK_HPP


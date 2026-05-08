#pragma once
#include "awaiter_impl.hpp"
#include <atomic>
#include <exception>
#include <utility>
#include <type_traits>
#include <cstdint>
#include <cassert>
#include <coroutine>
#include "cppcoro/task.hpp"
namespace cppcoro
{

namespace detail
{

template<bool isSuspend>
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

template<typename T, bool isSuspend>
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

    task<T, task_promise<T, isSuspend>> get_return_object() noexcept{
        return task<T, task_promise<T, isSuspend>>{ std::coroutine_handle<task_promise<T, isSuspend>>::from_promise(*this) };
    }

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


template<bool isSuspend>
class task_promise<void, isSuspend> final : public task_promise_base<isSuspend>
{
public:

  task_promise() noexcept = default;

    task<void, task_promise<void, isSuspend>> get_return_object() noexcept{
    return task<void, task_promise<void, isSuspend>>{ std::coroutine_handle<task_promise<void, isSuspend>>::from_promise(*this) };
    }

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
template<typename T, bool isSuspend>
class task_promise<T&, isSuspend> final : public task_promise_base<isSuspend>
{
public:

  task_promise() noexcept = default;

    task<T&, task_promise<T&, isSuspend>> get_return_object() noexcept{
        return task<T&, task_promise<T&, isSuspend>>{ std::coroutine_handle<task_promise<T&, isSuspend>>::from_promise(*this) };
    }

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









} // namespace detail
} // namespace cppcoro
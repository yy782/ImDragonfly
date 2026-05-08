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

namespace detail
{
template<typename T, bool isSuspend>
class task_promise;
}



template<typename T, typename TaskPromise>
class [[nodiscard]] task
{

public:  
  using promise_type = TaskPromise;

  using value_type = T;


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


    return awaitable_base{ coroutine_ };
  }

  auto operator co_await() const && noexcept
  {


    return std::move(awaitable_base{ coroutine_ });
  }

  /// \brief
  /// Returns an awaitable that will await completion of the task without
  /// attempting to retrieve the result.
  auto when_ready() const noexcept
  {

    return awaitable_base {coroutine_ };
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
            throw detail::broken_promise{};
          }

          return std::move(this->coroutine_.promise()).result();
        }

        decltype(auto) await_resume() &
        {
          if (!this->coroutine_)
          {
            throw detail::broken_promise{};
          }

          return this->coroutine_.promise().result();
        }

  };

  std::coroutine_handle<promise_type> coroutine_;

};







}

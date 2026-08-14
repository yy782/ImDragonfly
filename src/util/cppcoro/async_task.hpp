

#pragma once
#include <coroutine>
#include <exception>

namespace cppcoro {
// AI总是看不懂AsyncTask,总是认为有严重问题的
template <class TaskPromise>
class AsyncTaskBase {
 public:
  using promise_type = TaskPromise;

  AsyncTaskBase() = delete;
  explicit AsyncTaskBase(std::coroutine_handle<promise_type> coroutine)
      : coroutine_(coroutine) {}
  AsyncTaskBase(AsyncTaskBase&&) = delete;
  AsyncTaskBase(const AsyncTaskBase&) = delete;
  AsyncTaskBase& operator=(const AsyncTaskBase&) = delete;
  AsyncTaskBase& operator=(AsyncTaskBase&&) = delete;
  ~AsyncTaskBase() = default;

 private:
  std::coroutine_handle<promise_type> coroutine_;
};
class AsyncPromise {
 public:
  AsyncPromise() noexcept {}
  AsyncPromise(const AsyncPromise&) = delete;
  AsyncPromise& operator=(const AsyncPromise&) = delete;
  ~AsyncPromise() = default;

  void return_void() noexcept {}
  void unhandled_exception() noexcept { std::terminate(); }
  // 项目不使用异常传播错误，使用错误码，类似谷歌，异常产生的数据不一致和资源泄漏让人很糟心
  auto initial_suspend() noexcept { return std::suspend_never{}; }
  auto final_suspend() noexcept {
    return std::suspend_never{};
  }  // 编译器自动插入：coroutine_handle::destroy(),释放协程帧

  AsyncTaskBase<AsyncPromise> get_return_object() noexcept {
    return AsyncTaskBase<AsyncPromise>{
        std::coroutine_handle<AsyncPromise>::from_promise(*this)};
  }
};

using AsyncTask = AsyncTaskBase<AsyncPromise>;

}  // namespace cppcoro
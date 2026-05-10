#pragma once
#include <exception>
#include <coroutine>

namespace cppcoro
{


template<class TaskPromise>
class AsyncTask {
public:
    using promise_type = TaskPromise;

    AsyncTask() =delete;
    explicit AsyncTask(std::coroutine_handle<promise_type> coroutine)
        : coroutine_(coroutine)
    {}
    AsyncTask(AsyncTask&&) =delete;
    AsyncTask(const AsyncTask&) = delete;
    AsyncTask& operator=(const AsyncTask&) = delete;
    AsyncTask& operator=(AsyncTask&&) =delete; 
    ~AsyncTask() = default;
  
private:
    std::coroutine_handle<promise_type> coroutine_;

};

class AsyncPromiseBase {
public:
    AsyncPromiseBase() noexcept = default;
    AsyncPromiseBase(const AsyncPromiseBase&) = delete;
    AsyncPromiseBase& operator=(const AsyncPromiseBase&) = delete;
    ~AsyncPromiseBase() = default;

    void return_void() noexcept
    {}
    void unhandled_exception() noexcept
    {
        std::terminate();
    }    
    auto initial_suspend() noexcept
    {
        return std::suspend_never{};
    }
    auto final_suspend() noexcept
    {
        return std::suspend_never{};
    }

}


class AsyncPromise : public AsyncPromiseBase {
public:
      AsyncPromise() noexcept = default;

      AsyncTask<AsyncPromise> get_return_object() noexcept{
          return AsyncTask<AsyncPromise>{ std::coroutine_handle<AsyncPromise>::from_promise(*this) };
      }

}




}    
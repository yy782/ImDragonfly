

#pragma once
#include <exception>
#include <coroutine>

namespace cppcoro
{

template<class TaskPromise>
class AsyncTaskBase {
public:
    using promise_type = TaskPromise;

    AsyncTaskBase() =delete;
    explicit AsyncTaskBase(std::coroutine_handle<promise_type> coroutine)
        : coroutine_(coroutine)
    {}
    AsyncTaskBase(AsyncTaskBase&&) =delete;
    AsyncTaskBase(const AsyncTaskBase&) = delete;
    AsyncTaskBase& operator=(const AsyncTaskBase&) = delete;
    AsyncTaskBase& operator=(AsyncTaskBase&&) =delete; 
    ~AsyncTaskBase() = default;
  
private:
    std::coroutine_handle<promise_type> coroutine_;
};
class AsyncPromise {
public:
    template<typename ...Args>
    AsyncPromise(Args&&...) noexcept {

    }
    AsyncPromise() noexcept {

    }
    AsyncPromise(const AsyncPromise&) = delete;
    AsyncPromise& operator=(const AsyncPromise&) = delete;
    ~AsyncPromise() = default;

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

    AsyncTaskBase<AsyncPromise> get_return_object() noexcept{
        return AsyncTaskBase<AsyncPromise>{ std::coroutine_handle<AsyncPromise>::from_promise(*this) };
    }    
};

using AsyncTask = AsyncTaskBase<AsyncPromise>;








}    
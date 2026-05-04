#pragma once

#include <type_traits>
#include <coroutine>
#include <concepts>
#include <coroutine>
#include <type_traits>
#include <stdexcept>
namespace cppcoro::detail{

struct void_value {};
// 检测 await_suspend 返回值是否合法
template<typename Return>
concept valid_await_suspend_return = 
    std::is_void_v<Return> ||
    std::same_as<Return, bool> ||
    requires { typename std::coroutine_handle<std::coroutine_traits<Return>>; };



template<typename T>
concept Awaiter = requires(T&& value, std::coroutine_handle<> h) {
    // await_ready 必须返回 bool
    { value.await_ready() } -> std::convertible_to<bool>;
    
    // await_suspend 返回值必须合法（void/bool/coroutine_handle）
    { value.await_suspend(h) } -> detail::valid_await_suspend_return;
    
    // await_resume 存在即可
    value.await_resume();
};

// 2. 获取 Awaiter 的重载（用概念替代 SFINAE）


template<typename T>
requires requires(T&& value) { static_cast<T&&>(value).operator co_await(); }
auto get_awaiter_impl(T&& value, int)
    noexcept(noexcept(static_cast<T&&>(value).operator co_await()))
    -> decltype(static_cast<T&&>(value).operator co_await())
{
    return static_cast<T&&>(value).operator co_await();
}


template<typename T>
requires requires(T&& value) { operator co_await(static_cast<T&&>(value)); }
auto get_awaiter_impl(T&& value, long)
    noexcept(noexcept(operator co_await(static_cast<T&&>(value))))
    -> decltype(operator co_await(static_cast<T&&>(value)))
{
    return operator co_await(static_cast<T&&>(value));
}


template<typename T>
requires Awaiter<T&&>
auto get_awaiter_impl(T&& value, ...) noexcept
    -> T&&
{
    return static_cast<T&&>(value);
}


template<typename T>
auto get_awaiter(T&& value)
    noexcept(noexcept(get_awaiter_impl(static_cast<T&&>(value), 123)))
    -> decltype(get_awaiter_impl(static_cast<T&&>(value), 123))
{
    return get_awaiter_impl(static_cast<T&&>(value), 123);
}

template<typename T, typename = void>
struct awaitable_traits
{};


template<typename T>
struct awaitable_traits<T, std::void_t<decltype(get_awaiter(std::declval<T>()))>>
{
using awaiter_t = decltype(get_awaiter(std::declval<T>()));

using await_result_t = decltype(std::declval<awaiter_t>().await_resume());
};

class broken_promise : public std::logic_error
{
public:
  broken_promise()
    : std::logic_error("broken promise")
  {}
};

template<typename T>
struct remove_rvalue_reference
{
  using type = T;
};

template<typename T>
struct remove_rvalue_reference<T&&>
{
  using type = T;
};

template<typename T>
using remove_rvalue_reference_t = typename remove_rvalue_reference<T>::type;

template<typename T, typename = std::void_t<>>
struct is_awaitable : std::false_type {};

template<typename T>
struct is_awaitable<T, std::void_t<decltype(get_awaiter(std::declval<T>()))>>
  : std::true_type
{};

template<typename T>
constexpr bool is_awaitable_v = is_awaitable<T>::value;
} // namespace cppcoro::detail

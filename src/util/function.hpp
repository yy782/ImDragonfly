// function.h
// 基于 fu2/function2.hpp 提供的非抛异常 std::function 替代品。
//
// 与 std::function 的区别：
//   * 空函数被调用时不会抛出 std::bad_function_call，而是直接 std::abort()。
//   * unique_function 为 move-only，适用于 mpmc_queue 等只需移动语义的场景。
//   * FunctionRef 为非拥有函数视图，对应 fu2::function_view。
#pragma once

#include "util/function2.hpp"

namespace util {

// 直接转发 fu2 的 function_base，便于按需定制各模板参数
// (IsOwning, IsCopyable, Capacity, IsThrowing, HasStrongExceptGuarantee)。
template <bool IsOwning, bool IsCopyable, typename Capacity, bool IsThrowing,
          bool HasStrongExceptGuarantee, typename... Signatures>
using function_base =
    fu2::function_base<IsOwning, IsCopyable, Capacity, IsThrowing,
                       HasStrongExceptGuarantee, Signatures...>;

// 拥有所有权、可拷贝、空调用时 std::abort（不抛异常）的 function。
template <typename... Signatures>
using function =
    function_base<true, true, fu2::capacity_default, false, false, Signatures...>;

// 拥有所有权、move-only、空调用时 std::abort（不抛异常）的 function。
template <typename... Signatures>
using unique_function =
    function_base<true, false, fu2::capacity_default, false, false, Signatures...>;

// 非拥有、可拷贝的函数视图（对应 fu2::function_view），空调用时 std::abort（不抛异常）。
template <typename... Signatures>
using FunctionRef =
    function_base<false, true, fu2::capacity_default, false, false, Signatures...>;

}  // namespace util


#pragma once
#include <functional>

namespace utils{
    template<class T>
    using FunctionRef<T>=std::function<T>;
}
#ifndef CPPCORO_DETAIL_UNWRAP_REFERENCE_HPP_INCLUDED
#define CPPCORO_DETAIL_UNWRAP_REFERENCE_HPP_INCLUDED

#include <functional>

namespace yy::detail
{

template<typename T>
struct unwrap_reference
{
  using type = T;
};

template<typename T>
struct unwrap_reference<std::reference_wrapper<T>>
{
  using type = T;
};

template<typename T>
using unwrap_reference_t = typename unwrap_reference<T>::type;
}


#endif
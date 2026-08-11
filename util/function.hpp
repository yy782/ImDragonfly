#pragma once 

#include "function2.hpp"

namespace util{


template <bool IsOwning, bool IsCopyable, typename Capacity, bool IsThrowing,
          bool HasStrongExceptGuarantee, typename... Signatures>
using function_base = fu2::function_base<IsOwning, IsCopyable, Capacity, IsThrowing, HasStrongExceptGuarantee, Signatures...>;

template<typename T>
using FunctionRef = fu2::function_base<false /*not owns*/, 
                    true/*copyable*/, 
                    fu2::capacity_fixed<16, 8>, 
                    false, /* non-throwing*/
                    false, /* strong exceptions guarantees*/
                    T
                    >;

}
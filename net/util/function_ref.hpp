

#include "function2.hpp"

namespace util{
template<typename T>
using FunctionRef = fu2::function_base<false /*not owns*/, 
                    true/*copyable*/, 
                    fu2::capacity_fixed<16, 8>, 
                    false, /* non-throwing*/
                    false, /* strong exceptions guarantees*/
                    T
                    >;

}
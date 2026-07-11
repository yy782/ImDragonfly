
#pragma once

namespace util{


bool isPowerOfTwo(auto n){
    return n > 0 && (n & (n - 1)) == 0;
}



}
#pragma once

#include <string>
#include <algorithm>
#include <cctype>


namespace utils{

template<tyepname StrType1, typename StrType2 = StrType1>
StrType2 StrToUpper(StrType1&& str)
{
    StrType1 result(str);
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    return result;    
}


template<typename StrType1, typename StrType2>     
bool EqualsIgnoreCaseStd(StrType1&& a, StrType2&& b) {
    // TODO 检测a,b的类型，支持char*
    if (a.size() != b.size()) return false;
    return std::equal(a.begin(), a.end(), b.begin(),
        [](char c1, char c2) {
            return std::tolower(static_cast<unsigned char>(c1)) == 
                std::tolower(static_cast<unsigned char>(c2));
        });
    }    

}

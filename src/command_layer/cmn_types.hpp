#pragma once

#include <span>
#include <string_view>

namespace cmn {

using ArgSlice = std::span<const std::string_view>; // 这两个是等价的


using CmdArgList = std::span<const std::string_view>;    

}
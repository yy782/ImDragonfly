#pragma once

#include <span>
#include <string_view>

namespace cmn {

using ArgSlice = std::span<const std::string_view>;


using CmdArgList = std::span<const std::string_view>;    

}
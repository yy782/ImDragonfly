#pragma once

#include <span>

#include <concepts>
#include <string_view>
namespace facade{
using CmdArgList = std::span<const std::string_view>;    
}
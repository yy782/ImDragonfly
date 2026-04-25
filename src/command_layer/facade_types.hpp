#pragma once

#include <absl/types/span.h>

#include <concepts>
#include <string_view>
namespace facade{
using CmdArgList = absl::Span<const std::string_view>;    
}
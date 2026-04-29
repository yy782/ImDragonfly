#pragma once

#include <span>
#include <string_view>

namespace cmn {

using ArgSlice = std::Span<const std::string_view>;

}
#pragma once

#include <absl/types/span.h>
#include <string_view>

namespace cmn {

using ArgSlice = absl::Span<const std::string_view>;

}
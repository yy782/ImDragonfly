
#pragma once

#include <span>
#include <utility> 
#include <cstdint>
#include <string_view>
namespace dfly {

using DbIndex = uint16_t;
using ShardId = uint16_t;

using SlotId = std::uint16_t;

using ArgSlice = std::span<const std::string_view>; // from arg_range.h
using IndexSlice = std::pair<uint32_t, uint32_t>;



constexpr DbIndex kInvalidDbId = DbIndex(-1);
constexpr ShardId kInvalidSid = ShardId(-1);









class EngineShard;
class Transaction;
class DbSlice;
class ConnectionContext;
class CommandContext;
class Namespace;
class CommandRegistry;
class Interpreter;
}  // namespace dfly
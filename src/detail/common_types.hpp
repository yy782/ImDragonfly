
#pragma once


namespace dfly {

using DbIndex = uint16_t;
using ShardId = uint16_t;

using SlotId = std::uint16_t;
class EngineShard;
class Transaction;
class DbSlice;
class ConnectionContext;
class CommandContext;
class Namespace;
class CommandRegistry;
class Interpreter;
}  // namespace dfly
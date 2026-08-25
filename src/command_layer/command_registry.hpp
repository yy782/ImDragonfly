#pragma once

#include <cstddef>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>

#include "command_layer/command_id.hpp"

namespace dfly {

struct CommandSpec {
  const char* name;
  uint32_t mask;
  int8_t first_key;
  int8_t last_key;
  CommandId::Handler handler;
  int8_t step{1};
};

// 编译期校验：命令目录内命令名不得重复。
constexpr bool CheckUniqueNames(std::span<const CommandSpec> specs) {
  for (size_t i = 0; i < specs.size(); ++i)
    for (size_t j = i + 1; j < specs.size(); ++j)
      if (std::string_view(specs[i].name) == std::string_view(specs[j].name))
        return false;
  return true;
}

struct TransparentHash {
  using is_transparent = void;
  size_t operator()(std::string_view sv) const noexcept {
    return std::hash<std::string_view>{}(sv);
  }
};

class CommandRegistry {
 public:
  CommandRegistry();

  void Register(std::span<const CommandSpec> specs);

  const CommandId* Find(std::string_view cmd) const {
    auto it = cmd_map_.find(cmd);
    return it == cmd_map_.end() ? nullptr : &it->second;
  }

  CommandId* Find(std::string_view cmd) {
    auto it = cmd_map_.find(cmd);
    return it == cmd_map_.end() ? nullptr : &it->second;
  }

  size_t size() const { return cmd_map_.size(); }

 private:
  using CmdMap = std::unordered_map<std::string, CommandId, TransparentHash,
                                    std::equal_to<>>;
  CmdMap cmd_map_;
};

void RegisterStringFamily(CommandRegistry* registry);
void RegisterGeneric(CommandRegistry* registry);
void RegisterHashFamily(CommandRegistry* registry);
void RegisterSetFamily(CommandRegistry* registry);
void RegisterZSetFamily(CommandRegistry* registry);
void RegisterListFamily(CommandRegistry* registry);

}  // namespace dfly

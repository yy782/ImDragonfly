// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "command_layer/cmn_types.hpp"
#include "command_layer/command_id.hpp"
#include "function.hpp"

namespace dfly {

// 透明 hash：同时支持 std::string 与 std::string_view，并开启 unordered_map
// 的异构查找，使 Find(std::string_view) 无需再构造临时 std::string。
// std::hash<std::string_view> 与 std::hash<std::string> 对相同字符序列
// 产生一致哈希（C++17 起标准保证），因此查找结果与原实现完全等价。
struct TransparentHash {
  using is_transparent = void;
  size_t operator()(std::string_view sv) const noexcept {
    return std::hash<std::string_view>{}(sv);
  }
};

class CommandRegistry {
 public:
  CommandRegistry();

  CommandRegistry& operator<<(CommandId cmd);

  const CommandId* Find(std::string_view cmd) const {
    auto it = cmd_map_.find(cmd);
    return it == cmd_map_.end() ? nullptr : &it->second;
  }

  CommandId* Find(std::string_view cmd) {
    auto it = cmd_map_.find(cmd);
    return it == cmd_map_.end() ? nullptr : &it->second;
  }

  void StartFamily();

  using FamiliesVec = std::vector<std::vector<std::string>>;
  FamiliesVec GetFamilies();

 private:
  using CmdMap = std::unordered_map<std::string, CommandId, TransparentHash,
                                    std::equal_to<>>;
  CmdMap cmd_map_;

  FamiliesVec family_of_commands_;
  size_t bit_index_;
};

}  // namespace dfly

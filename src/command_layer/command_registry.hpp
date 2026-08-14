// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <optional>
#include <unordered_map>

#include "command_layer/cmn_types.hpp"
#include "command_layer/command_id.hpp"
#include "function.hpp"

namespace dfly {

class CommandRegistry {
 public:
  CommandRegistry();

  CommandRegistry& operator<<(CommandId cmd);

  const CommandId* Find(std::string_view cmd) const {
    auto it = cmd_map_.find(std::string(cmd));
    return it == cmd_map_.end() ? nullptr : &it->second;
  }

  CommandId* Find(std::string_view cmd) {
    auto it = cmd_map_.find(std::string(cmd));
    return it == cmd_map_.end() ? nullptr : &it->second;
  }

  void StartFamily();

  using FamiliesVec = std::vector<std::vector<std::string>>;
  FamiliesVec GetFamilies();

 private:
  std::unordered_map<std::string, CommandId> cmd_map_;

  FamiliesVec family_of_commands_;
  size_t bit_index_;
};

}  // namespace dfly

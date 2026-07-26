// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "command_registry.hpp"

#include "Strings.hpp"

namespace dfly {

using namespace facade;



CommandRegistry::CommandRegistry() {}

CommandRegistry& CommandRegistry::operator<<(CommandId cmd) {
  std::string k = std::string(cmd.name());
  cmd.SetFamily(family_of_commands_.size() - 1);
  cmd_map_.emplace(k, std::move(cmd));
  return *this;
}

void CommandRegistry::StartFamily() {
  family_of_commands_.emplace_back();
  bit_index_ = 0;
}

CommandRegistry::FamiliesVec CommandRegistry::GetFamilies() {
  return std::move(family_of_commands_);
}

}  // namespace dfly

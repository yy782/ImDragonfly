// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "command_registry.hpp"


#include "util/Strings.hpp"

namespace dfly {

using namespace facade;


CommandId::CommandId(const char* name, size_t keys_start, size_t keys_nums, size_t keys_offset, uint32_t opt_mask)
    : facade::CommandId(name, keys_start, keys_nums, keys_offset, opt_mask) {
}

CommandId::~CommandId() {
}

CommandId CommandId::Clone(const std::string_view name) const {
    CommandId cloned =
        CommandId{name.data(), keys_start_, keys_nums_, keys_offset_};
    cloned.handler_ = handler_;
    return cloned;
}


CommandRegistry::CommandRegistry() {
}



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

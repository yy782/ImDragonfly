// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "command_registry.hpp"

#include "base/bits.h"
#include "base/flags.h"
#include "base/logging.h"
#include "base/stl_util.h"
#include "utils/Strings.hpp"

namespace dfly {

using namespace facade;


CommandId::CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key,
                     int8_t last_key)
    : facade::CommandId(name, mask, arity, first_key, last_key) {
}

CommandId::~CommandId() {
}

CommandId CommandId::Clone(const std::string_view name) const {
    CommandId cloned =
        CommandId{name.data(), opt_mask_, arity_, first_key_, last_key_};
    cloned.handler_ = handler_;
    return cloned;
}


CommandRegistry::CommandRegistry() {
}



CommandRegistry& CommandRegistry::operator<<(CommandId cmd) {
    string k = string(cmd.name());
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

std::pair<const CommandId*, ParsedArgs> CommandRegistry::FindExtended(ParsedArgs args) const {
    std::string cmd = utils::StrToUpper<std::string>(args.Front());// 提取并转大写
    const CommandId* res = Find(cmd);
    if (!res)
      return {nullptr, {}};
    auto tail_args = args.Tail();  
    return {res, tail_args};
}



}  // namespace dfly

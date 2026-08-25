#pragma once

#include "command_layer/command_registry.hpp"
#include "detail/common_types.hpp"
#include "detail/conn_context.hpp"

namespace dfly {

using ::dfly::CmdArgList;

namespace cmd {
struct CoroTask;
}

class SetFamily {
 public:
  static cmd::CoroTask SAdd(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask SRem(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask SMembers(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask SCard(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask SIsMember(CommandContext* cmd_cntx, CmdArgList args);
};

}  // namespace dfly

#pragma once

#include "command_layer/command_registry.hpp"
#include "detail/common_types.hpp"
#include "detail/conn_context.hpp"

namespace dfly {

using ::dfly::CmdArgList;

namespace cmd {
struct CoroTask;
}

class HashFamily {
 public:
  static cmd::CoroTask HSet(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask HGet(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask HDel(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask HExists(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask HLen(CommandContext* cmd_cntx, CmdArgList args);
};

}  // namespace dfly

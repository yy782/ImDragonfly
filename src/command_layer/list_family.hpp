#pragma once

#include "command_layer/command_registry.hpp"
#include "detail/common_types.hpp"
#include "detail/conn_context.hpp"

namespace dfly {

using ::dfly::CmdArgList;

namespace cmd {
struct CoroTask;
}

class ListFamily {
 public:
  static cmd::CoroTask LPush(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask RPush(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask LPop(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask RPop(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask LLen(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask LIndex(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask LRange(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask LSet(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask LRem(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask LInsert(CommandContext* cmd_cntx, CmdArgList args);
};

}  // namespace dfly

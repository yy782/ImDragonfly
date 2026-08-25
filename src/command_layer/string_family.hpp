#pragma once

#include "command_layer/command_registry.hpp"
#include "detail/common_types.hpp"
#include "detail/conn_context.hpp"

namespace dfly {

using ::dfly::CmdArgList;

namespace cmd {
struct CoroTask;
}

class StringFamily {
 public:
  static cmd::CoroTask Set(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask Get(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask MGet(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask MSet(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask Append(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask Strlen(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask Incr(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask IncrBy(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask Decr(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask DecrBy(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask Setnx(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask Getset(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask Getrange(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask Setrange(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask Getdel(CommandContext* cmd_cntx, CmdArgList args);
};

}  // namespace dfly

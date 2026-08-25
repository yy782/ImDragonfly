#pragma once

#include "command_layer/command_registry.hpp"
#include "detail/common_types.hpp"
#include "detail/conn_context.hpp"

namespace dfly {

using ::dfly::CmdArgList;

namespace cmd {
struct CoroTask;
}

class ZSetFamily {
 public:
  static cmd::CoroTask ZAdd(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask ZCard(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask ZScore(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask ZRem(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask ZRank(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask ZRevRank(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask ZRange(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask ZRevRange(CommandContext* cmd_cntx, CmdArgList args);
};

}  // namespace dfly

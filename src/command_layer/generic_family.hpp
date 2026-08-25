#pragma once

#include "detail/common_types.hpp"

namespace dfly {

using ::dfly::CmdArgList;

namespace cmd {
struct CoroTask;
}

class GenericFamily {
 public:
  static void Register(CommandRegistry* registry);

 public:
  static cmd::CoroTask Delex(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask Ping(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask Exists(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask Expire(CommandContext* cmd_cntx, CmdArgList args);

  static cmd::CoroTask ExpireTime(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask Ttl(CommandContext* cmd_cntx, CmdArgList args);

  static cmd::CoroTask Client_Info(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask ShutDown(CommandContext* cmd_cntx, CmdArgList args);

  // DEBUG DB 遍历全部分片把实际存在的 key 打到日志。
  static cmd::CoroTask Debug(CommandContext* cmd_cntx, CmdArgList args);
};

}  // namespace dfly

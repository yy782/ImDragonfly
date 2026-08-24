#pragma once

#include "command_layer/cmn_types.hpp"
#include "command_layer/command_registry.hpp"
#include "detail/conn_context.hpp"

namespace dfly {

using ::cmn::CmdArgList;

namespace cmd {
struct CoroTask;
}

class SetFamily {  // Set 命令家族，处理 Redis Set 命令
 private:
  static cmd::CoroTask SAdd(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 SADD 命令，向集合添加一个或多个成员
  static cmd::CoroTask SRem(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 SREM 命令，从集合移除一个或多个成员
  static cmd::CoroTask SMembers(CommandContext* cmd_cntx,
                                CmdArgList args);  // 处理 SMEMBERS 命令，返回集合中的所有成员
  static cmd::CoroTask SCard(CommandContext* cmd_cntx,
                             CmdArgList args);  // 处理 SCARD 命令，返回集合的大小
  static cmd::CoroTask SIsMember(CommandContext* cmd_cntx,
                                 CmdArgList args);  // 处理 SISMEMBER 命令，检查成员是否在集合中
};

}  // namespace dfly

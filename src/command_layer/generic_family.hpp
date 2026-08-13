// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "command_layer/cmn_types.hpp"
#include "detail/tx_base.hpp"

namespace dfly {

using cmn::CmdArgList;

namespace cmd {
struct CoroTask;
}

class GenericFamily {  // 通用命令家族，处理 Redis 通用命令
 public:
  static void Register(
      CommandRegistry* registry);  // 注册所有通用命令到命令注册表

 private:
  static cmd::CoroTask Delex(CommandContext* cmd_cntx,
                             CmdArgList args);  // 处理 DELEX 命令，用于删除键
  static cmd::CoroTask Ping(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 PING 命令，用于测试连接
  static cmd::CoroTask Exists(
      CommandContext* cmd_cntx,
      CmdArgList args);  // 处理 EXISTS 命令，检查键是否存在
  static cmd::CoroTask Expire(
      CommandContext* cmd_cntx,
      CmdArgList args);  // 处理 EXPIRE 命令，设置键的过期时间（秒）

  static cmd::CoroTask ExpireTime(
      CommandContext* cmd_cntx,
      CmdArgList args);  // 处理 EXPIRETIME 命令，获取键的绝对过期时间（秒）
  static cmd::CoroTask Ttl(
      CommandContext* cmd_cntx,
      CmdArgList args);  // 处理 TTL 命令，获取键的剩余生存时间（秒）

  static cmd::CoroTask Client_Info(CommandContext* cmd_cntx, CmdArgList args);
  static cmd::CoroTask ShutDown(CommandContext* cmd_cntx, CmdArgList args);
};

}  // namespace dfly

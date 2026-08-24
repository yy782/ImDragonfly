#pragma once

#include "command_layer/cmn_types.hpp"
#include "command_layer/command_registry.hpp"
#include "detail/conn_context.hpp"

namespace dfly {

using ::cmn::CmdArgList;

namespace cmd {
struct CoroTask;
}

class HashFamily {  // Hash 命令家族，处理 Redis Hash 命令
 private:
  static cmd::CoroTask HSet(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 HSET 命令，设置哈希表中的字段值
  static cmd::CoroTask HGet(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 HGET 命令，获取哈希表中字段的值
  static cmd::CoroTask HDel(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 HDEL 命令，删除哈希表中的一个或多个字段
  static cmd::CoroTask HExists(CommandContext* cmd_cntx,
                               CmdArgList args);  // 处理 HEXISTS 命令，检查字段是否存在于哈希表
  static cmd::CoroTask HLen(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 HLEN 命令，返回哈希表中字段的数量
};

}  // namespace dfly

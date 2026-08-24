#pragma once

#include "command_layer/cmn_types.hpp"
#include "command_layer/command_registry.hpp"
#include "detail/conn_context.hpp"

namespace dfly {

using ::cmn::CmdArgList;

namespace cmd {
struct CoroTask;
}

class ZSetFamily {  // ZSet 命令家族，处理 Redis 有序集合命令
 private:
  static cmd::CoroTask ZAdd(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 ZADD 命令，向有序集合添加成员或更新其分数
  static cmd::CoroTask ZCard(CommandContext* cmd_cntx,
                             CmdArgList args);  // 处理 ZCARD 命令，返回有序集合的大小
  static cmd::CoroTask ZScore(CommandContext* cmd_cntx,
                              CmdArgList args);  // 处理 ZSCORE 命令，返回成员的分数
  static cmd::CoroTask ZRem(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 ZREM 命令，从有序集合移除一个或多个成员
  static cmd::CoroTask ZRank(CommandContext* cmd_cntx,
                             CmdArgList args);  // 处理 ZRANK 命令，返回成员的排名（升序）
  static cmd::CoroTask ZRevRank(CommandContext* cmd_cntx,
                                CmdArgList args);  // 处理 ZREVRANK 命令，返回成员的排名（降序）
  static cmd::CoroTask ZRange(CommandContext* cmd_cntx,
                              CmdArgList args);  // 处理 ZRANGE 命令，按排名范围返回成员（升序）
  static cmd::CoroTask ZRevRange(CommandContext* cmd_cntx,
                                 CmdArgList args);  // 处理 ZREVRANGE 命令，按排名范围返回成员（降序）
};

}  // namespace dfly

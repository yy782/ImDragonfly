#pragma once

#include "command_layer/cmn_types.hpp"
#include "command_layer/command_registry.hpp"
#include "detail/conn_context.hpp"

namespace dfly {

using ::cmn::CmdArgList;

namespace cmd {
struct CoroTask;
}

class StringFamily {  // String 命令家族，处理 Redis String 命令
 private:
  static cmd::CoroTask Set(CommandContext* cmd_cntx,
                           CmdArgList args);  // 处理 SET 命令，设置键值并支持 EX/PX/NX/XX 选项
  static cmd::CoroTask Get(CommandContext* cmd_cntx,
                           CmdArgList args);  // 处理 GET 命令，获取键的值
  static cmd::CoroTask MGet(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 MGET 命令，批量获取多个键的值
  static cmd::CoroTask MSet(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 MSET 命令，批量设置多组键值
  static cmd::CoroTask Append(CommandContext* cmd_cntx,
                              CmdArgList args);  // 处理 APPEND 命令，追加值并返回新长度
  static cmd::CoroTask Strlen(CommandContext* cmd_cntx,
                              CmdArgList args);  // 处理 STRLEN 命令，返回值的长度
  static cmd::CoroTask Incr(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 INCR 命令，键值加 1
  static cmd::CoroTask IncrBy(CommandContext* cmd_cntx,
                              CmdArgList args);  // 处理 INCRBY 命令，键值增加指定步长
  static cmd::CoroTask Decr(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 DECR 命令，键值减 1
  static cmd::CoroTask DecrBy(CommandContext* cmd_cntx,
                              CmdArgList args);  // 处理 DECRBY 命令，键值减少指定步长
  static cmd::CoroTask Setnx(CommandContext* cmd_cntx,
                             CmdArgList args);  // 处理 SETNX 命令，仅当键不存在时设置
  static cmd::CoroTask Getset(CommandContext* cmd_cntx,
                              CmdArgList args);  // 处理 GETSET 命令，设置新值并返回旧值
  static cmd::CoroTask Getrange(CommandContext* cmd_cntx,
                                CmdArgList args);  // 处理 GETRANGE 命令，返回子串
  static cmd::CoroTask Setrange(CommandContext* cmd_cntx,
                                CmdArgList args);  // 处理 SETRANGE 命令，从偏移处覆盖值
  static cmd::CoroTask Getdel(CommandContext* cmd_cntx,
                              CmdArgList args);  // 处理 GETDEL 命令，返回旧值并删除键
};

}  // namespace dfly

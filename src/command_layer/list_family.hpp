#pragma once

#include "command_layer/cmn_types.hpp"
#include "command_layer/command_registry.hpp"
#include "detail/conn_context.hpp"

namespace dfly {

using ::cmn::CmdArgList;

namespace cmd {
struct CoroTask;
}

class ListFamily {  // List 命令家族，处理 Redis List 命令
 private:
  static cmd::CoroTask LPush(CommandContext* cmd_cntx,
                             CmdArgList args);  // 处理 LPUSH 命令，向列表头部插入元素
  static cmd::CoroTask RPush(CommandContext* cmd_cntx,
                             CmdArgList args);  // 处理 RPUSH 命令，向列表尾部插入元素
  static cmd::CoroTask LPop(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 LPOP 命令，弹出列表头部元素
  static cmd::CoroTask RPop(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 RPOP 命令，弹出列表尾部元素
  static cmd::CoroTask LLen(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 LLEN 命令，返回列表长度
  static cmd::CoroTask LIndex(CommandContext* cmd_cntx,
                              CmdArgList args);  // 处理 LINDEX 命令，返回指定下标的元素
  static cmd::CoroTask LRange(CommandContext* cmd_cntx,
                              CmdArgList args);  // 处理 LRANGE 命令，按范围返回列表元素
  static cmd::CoroTask LSet(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 LSET 命令，设置指定下标的元素值
  static cmd::CoroTask LRem(CommandContext* cmd_cntx,
                            CmdArgList args);  // 处理 LREM 命令，移除指定数量的匹配元素
  static cmd::CoroTask LInsert(CommandContext* cmd_cntx,
                               CmdArgList args);  // 处理 LINSERT 命令，在指定元素前后插入新元素
};

}  // namespace dfly

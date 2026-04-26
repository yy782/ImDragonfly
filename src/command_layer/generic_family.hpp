// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>
#include "detail/tx_base.h"


#include "facade_types.hpp"


ABSL_DECLARE_FLAG(uint32_t, dbnum);

namespace dfly {

using facade::CmdArgList;

class GenericFamily { // 通用命令家族，处理 Redis 通用命令
 public:
  static void Register(CommandRegistry* registry); // 注册所有通用命令到命令注册表

  // // Accessed by Service::Exec and Service::Watch as an utility.
  // static OpResult<uint32_t> OpExists(const OpArgs& op_args, const ShardArgs& keys); // 检查多个键是否存在
  // static OpResult<uint32_t> OpDel(const OpArgs& op_args, const ShardArgs& keys, bool async); // 删除多个键

 private:
  static void Delex(CmdArgList args, CommandContext* cmd_cntx); // 处理 DELEX 命令，用于删除键
  static void Ping(CmdArgList args, CommandContext* cmd_cntx); // 处理 PING 命令，用于测试连接
  static void Exists(CmdArgList args, CommandContext* cmd_cntx); // 处理 EXISTS 命令，检查键是否存在
  static void Expire(CmdArgList args, CommandContext* cmd_cntx); // 处理 EXPIRE 命令，设置键的过期时间（秒）
  static void Persist(CmdArgList args, CommandContext* cmd_cntx); // 处理 PERSIST 命令，移除键的过期时间
  static void Keys(CmdArgList args, CommandContext* cmd_cntx); // 处理 KEYS 命令，根据模式查找键

  static void ExpireTime(CmdArgList args, CommandContext* cmd_cntx); // 处理 EXPIRETIME 命令，获取键的过期时间（秒）
  static void Ttl(CmdArgList args, CommandContext* cmd_cntx); // 处理 TTL 命令，获取键的剩余生存时间（秒）



  static void Select(CmdArgList args, CommandContext* cmd_cntx); // 处理 SELECT 命令，选择数据库
  static void FieldExpire(CmdArgList args, CommandContext* cmd_cntx); // 处理 FIELDEXPIRE 命令，设置字段的过期时间
};

}  // namespace dfly

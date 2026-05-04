// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once


#include "detail/tx_base.hpp"
#include "command_layer/cmn_types.hpp"


namespace dfly {

using cmn::CmdArgList;

class GenericFamily { // 通用命令家族，处理 Redis 通用命令
public:
    static void Register(CommandRegistry* registry); // 注册所有通用命令到命令注册表


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

};

}  // namespace dfly

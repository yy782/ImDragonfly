// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "generic_family.hpp"


#include <optional>

#include "cmd_support.hpp"
#include "sharding/db_slice.hpp"
#include "sharding/op_status.hpp"
#include "network/redis_server.hpp"
#include <atomic>

namespace dfly {
using namespace dfly::cmd;
facade::OpResult<uint32_t> OpDel(const OpArgs& op_args, const ShardArgs& keys) {

    auto& db_slice = op_args.GetDbSlice();
    uint32_t res = 0;
    for (std::string_view key : keys) {
        auto it = db_slice.FindMutable(op_args.db_cntx_, key).it_;  // post_updater will run immediately
        if (!IsValid(it.GetInnerIt())) {
            continue;
        }
        db_slice.Del(op_args.db_cntx_, it, nullptr);
        ++res;
    }

    return res;
}

CoroTask CmdDel(CmdArgList args, CommandContext* cmd_cntx) {

    (void)args;

    std::atomic<uint32_t> result = 0;
    auto cb = [&](Transaction* tx, EngineShard* es) -> facade::OpResult<void> {
        auto shard_args = tx->GetShardArgs(es->shard_id());
        auto op_args = tx->GetOpArgs(es);
        auto res = OpDel(op_args, shard_args);
        result.fetch_add(res.value_or(0), std::memory_order_relaxed);
        return {OpStatus::OK};
    };

    facade::OpResult<void> res = co_await cmd::SingleHopT(cb);
    uint32_t del_cnt = result.load(std::memory_order_relaxed);

    auto conn = cmd_cntx->conn_cntx()->owner_;
    if ( res.status() == OpStatus::OK)
        conn->Send(del_cnt);
    else 
        conn->SendERROR();
    co_return;
}

void GenericFamily::Delex(CmdArgList args, CommandContext* cmd_cntx) {
    CmdDel(args, cmd_cntx);
    return;
}


void GenericFamily::Ping(CmdArgList args, CommandContext* cmd_cntx) {
    auto conn = cmd_cntx->conn_cntx()->owner_;
    if (args.size() > 1) {
        return conn->SendERROR();
    }
    std::string msg = "PONG";
    
    conn->SendStatus(msg);
}



CoroTask CmdExists(CmdArgList args, CommandContext* cmd_cntx) {

    (void)args;

    auto Op = [](const OpArgs& op_args, const ShardArgs& keys) -> facade::OpResult<uint32_t> {
        auto& db_slice = op_args.GetDbSlice();
        uint32_t res = 0;

        for (std::string_view key : keys) {
          auto find_res = db_slice.FindReadOnly(op_args.db_cntx_, key);
          res += IsValid(find_res.GetInnerIt());
        }
        return {res};    
    };

    std::atomic<uint32_t> result{0};

    auto cb = [&result, &Op](Transaction* t, EngineShard* shard) -> facade::OpResult<void> {
      ShardArgs shard_args = t->GetShardArgs(shard->shard_id());
      auto res = Op(t->GetOpArgs(shard), shard_args);
      result.fetch_add(res.value_or(0), std::memory_order_relaxed);
      return {OpStatus::OK};
    };

    facade::OpResult<void> res = co_await cmd::SingleHopT(cb);

    auto conn = cmd_cntx->conn_cntx()->owner_;
    if(res.status() == OpStatus::OK)
    {
        
        conn->Send(result.load());  // FIXME
    }
    else 
    {
        conn->SendERROR();
    }

    co_return;

}

void GenericFamily::Exists(CmdArgList args, CommandContext* cmd_cntx) {
    CmdExists(args, cmd_cntx);
}



CoroTask CmdExpire(std::string_view key, int64_t sec, CommandContext* cmd_cntx) {
    

    auto cb = [&](Transaction* t, EngineShard* shard) -> facade::OpResult<void> {
        auto op_args = t->GetOpArgs(shard);
        auto& db_slice = op_args.GetDbSlice();
        auto find_res = db_slice.FindMutable(op_args.db_cntx_, key);
        if (!IsValid(find_res.it_.GetInnerIt())) {
          return {OpStatus::KEY_NOTFOUND};
        }

        return db_slice.UpdateExpire(op_args.db_cntx_, find_res.it_, sec);     
    };
    auto res = co_await cmd::SingleHopT(cb);

    auto conn = cmd_cntx->conn_cntx()->owner_;
    if(res == OpStatus::OK)
    {
        
        conn->SendStatus("OK");
    }
    else 
    {
        conn->SendERROR();
    }

    co_return;
}

void GenericFamily::Expire(CmdArgList args, CommandContext* cmd_cntx) {
    std::string_view key = args[0];
    std::string_view sec = args[1];
    int64_t int_arg = std::atoi(sec.data());
    CmdExpire(key, int_arg, cmd_cntx);
}



// void GenericFamily::Keys(CmdArgList args, CommandContext* cmd_cntx) {
//     // TODO
// }


CoroTask CmdExpireTime(std::string_view key, CommandContext* cmd_cntx) {

    auto cb = [&](Transaction* t, EngineShard* shard) -> facade::OpResult<int64_t> {
      auto& db_slice = t->GetDbSlice(shard->shard_id());
      auto it = db_slice.FindReadOnly(t->GetDbContext(), key);
      if (!IsValid(it.GetInnerIt()))
        return {OpStatus::KEY_NOTFOUND};

      if (!it.GetInnerIt()->first.HasExpire())
        return {OpStatus::SKIPPED};

      int64_t ttl_ms = it.GetInnerIt()->first.GetExpireTime();

      return {ttl_ms};
    };

    facade::OpResult<int64_t> res = co_await cmd::SingleHopT(cb);

    auto conn = cmd_cntx->conn_cntx()->owner_;  
    if(res.status() == OpStatus::OK)
    {
        conn->Send(res.value());
    }
    else 
    {
        conn->SendERROR();
    }
    co_return;    
}


void GenericFamily::ExpireTime(CmdArgList args, CommandContext* cmd_cntx) {
    CmdExpireTime(args[0], cmd_cntx);
}


CoroTask CmdTtl(std::string_view key, CommandContext* cmd_cntx) {

    auto cb = [&](Transaction* t, EngineShard* shard) -> facade::OpResult<int64_t> { 

        auto& db_slice = t->GetDbSlice(shard->shard_id());
        auto it = db_slice.FindReadOnly(t->GetDbContext(), key);
        if (!IsValid(it.GetInnerIt()))
            return {OpStatus::KEY_NOTFOUND};

        if (!it.GetInnerIt()->first.HasExpire())
            return {OpStatus::SKIPPED};

        auto ttlTime = it.GetInnerIt()->first.GetExpireTime();
        return ttlTime - t->GetDbContext().time_now_ms_;
    };
  
    facade::OpResult<int64_t> res = co_await cmd::SingleHopT(cb);

    auto conn = cmd_cntx->conn_cntx()->owner_;

    if(res.status() == OpStatus::OK)
    {
        
        conn->Send(res.value());
    }
    else 
    {
        conn->SendERROR();
    }
    co_return;      
}

void GenericFamily::Ttl(CmdArgList args, CommandContext* cmd_cntx) {
    CmdTtl(args[0], cmd_cntx);
}



// void GenericFamily::Select(CmdArgList args, CommandContext* cmd_cntx) {
//   // TODO
// }

using CI = CommandId;
void GenericFamily::Register(CommandRegistry* registry) {

  registry->StartFamily();
  *registry
      << CI{"DEL",-2, 1, -1}.SetHandler(&GenericFamily::Delex)
      << CI{"PING", -1, 0, 0}.SetHandler(&GenericFamily::Ping)
      << CI{"EXISTS", -2, 1, -1}.SetHandler(&GenericFamily::Exists)
      << CI{"EXPIRE", -3, 1, 1}.SetHandler(&GenericFamily::Expire)
      << CI{"TTL", 2, 1, 1}.SetHandler(&GenericFamily::Ttl)
      ;
}

void RegisterGeneric(CommandRegistry* registry) {
  GenericFamily::Register(registry);
}

}  // namespace dfly

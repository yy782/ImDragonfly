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
#include <glog/logging.h>

namespace dfly {
using namespace dfly::cmd;
facade::OpResult<uint32_t> OpDel(Transaction* tx, DbSlice& db_slice) {
    uint32_t res = 0;
    auto& slice = tx->GetSlice(db_slice.shard_id());
    for (const auto& [key, keyId] : slice) {
        auto it = db_slice.FindMutable(tx->GetDbContext(), key).it;  
        if (!IsValid(it.GetInnerIt())) {
            continue;
        }
        db_slice.Del(tx->GetDbContext(), it, nullptr);
        ++res;
    }

    return res;
}

CoroTask CmdDel(CommandContext* cmd_cntx, CmdArgList args) {

    (void)args;

    std::atomic<uint32_t> result = 0;
    auto cb = [&](Transaction* tx, EngineShard* es) -> facade::OpResult<void> {
        DbSlice& dbslice = tx->GetDbSlice(es->shard_id());
        auto res = OpDel(tx, dbslice);
        result.fetch_add(res.value_or(0), std::memory_order_relaxed);
        return {OpStatus::OK};
    };

    facade::OpResult<void> res = co_await cmd::SingleHopT(cb);
    uint32_t del_cnt = result.load(std::memory_order_relaxed);

    auto conn = cmd_cntx->conn_cntx()->owner();
    if ( res.status() == OpStatus::OK)
        conn->Send(del_cnt);
    else 
        conn->SendERROR();
    co_return;
}

void GenericFamily::Delex(CommandContext* cmd_cntx, CmdArgList args) {
    assert(!args.empty());
    CmdDel(cmd_cntx, args);
    return;
}


void GenericFamily::Ping(CommandContext* cmd_cntx, CmdArgList args) {
    auto conn = cmd_cntx->conn_cntx()->owner(); 
    if (args.size() > 1) {
        return conn->SendERROR();
    }
    std::string msg = "PONG";
    
    conn->SendStatus(msg);
}



CoroTask CmdExists(CommandContext* cmd_cntx, CmdArgList args) {

    (void)args;

    auto Op = [](Transaction* tx, DbSlice& db_slice) -> facade::OpResult<uint32_t> {
        auto& slice = tx->GetSlice(db_slice.shard_id());
        uint32_t res = 0;
        for (const auto& [key, keyId] : slice) {
          auto find_res = db_slice.FindReadOnly(tx->GetDbContext(), key);
          res += IsValid(find_res.GetInnerIt());
        }
        return {res};    
    };

    std::atomic<uint32_t> result{0};

    auto cb = [&result, &Op](Transaction* t, EngineShard* es) -> facade::OpResult<void> {
      auto res = Op(t, t->GetDbSlice(es->shard_id()));
      result.fetch_add(res.value_or(0), std::memory_order_relaxed);
      return {OpStatus::OK};
    };

    facade::OpResult<void> res = co_await cmd::SingleHopT(cb);

    auto conn = cmd_cntx->conn_cntx()->owner(); 
    if(res.status() == OpStatus::OK)
    {
        
        conn->Send(result.load());  // FIXME
    }
    else 
    {
        conn->SendInteger(0);
    }

    co_return;

}

void GenericFamily::Exists(CommandContext* cmd_cntx, CmdArgList args) {
    CmdExists(cmd_cntx, args);
}



CoroTask CmdExpire(CommandContext* cmd_cntx, std::string_view key, int64_t sec) {
    

    auto cb = [&](Transaction* t, EngineShard* es) -> facade::OpResult<void> {
        auto& db_slice = t->GetDbSlice(es->shard_id());
        auto find_res = db_slice.FindMutable(t->GetDbContext(), key);
        if (!IsValid(find_res.it.GetInnerIt())) {
          return {OpStatus::KEY_NOTFOUND};
        }
        auto ttlTime = t->GetDbContext().GetTimeNowMs()/1000 + sec; // 精度丢失
        return db_slice.UpdateExpire(t->GetDbContext(), find_res.it, ttlTime);     
    };
    auto res = co_await cmd::SingleHopT(cb);

    auto conn = cmd_cntx->conn_cntx()->owner(); 
    if(res.status() == OpStatus::OK)    
    {
        
        conn->SendInteger(1);
    }
    else 
    {
        conn->SendInteger(0);
    }

    co_return;
}

void GenericFamily::Expire(CommandContext* cmd_cntx, CmdArgList args) {
    std::string_view key = args[1];
    std::string_view sec = args[2];
    int64_t int_arg = std::atoi(sec.data());
    CmdExpire(cmd_cntx, key, int_arg);
}



// void GenericFamily::Keys(CmdArgList args, CommandContext* cmd_cntx) {
//     // TODO
// }


CoroTask CmdExpireTime(CommandContext* cmd_cntx, std::string_view key) {

    auto cb = [&](Transaction* t, EngineShard* es) -> facade::OpResult<int64_t> {
      auto& db_slice = t->GetDbSlice(es->shard_id());
      auto it = db_slice.FindReadOnly(t->GetDbContext(), key);
      if (!IsValid(it.GetInnerIt()))
        return {OpStatus::KEY_NOTFOUND};

      if (!it.GetInnerIt()->first.HasExpire())
        return {OpStatus::SKIPPED};

      int64_t ttl_ms = it.GetInnerIt()->first.GetExpireTime();

      return {ttl_ms};
    };

    facade::OpResult<int64_t> res = co_await cmd::SingleHopT(cb);

    auto conn = cmd_cntx->conn_cntx()->owner();  
    if(res.status() == OpStatus::OK)
    {
        conn->Send(res.value());
    }
    else 
    {
        if (res.status() == OpStatus::KEY_NOTFOUND) {
            conn->SendInteger(-2);
        }else if (res.status() == OpStatus::SKIPPED) {
            conn->SendInteger(-1);
        }else {
            conn->SendERROR();
        }
    }
    co_return;    
}


void GenericFamily::ExpireTime(CommandContext* cmd_cntx, CmdArgList args) {
    CmdExpireTime(cmd_cntx, args[1]);
}


CoroTask CmdTtl(CommandContext* cmd_cntx, std::string_view key) {

    auto cb = [&](Transaction* t, EngineShard* es) -> facade::OpResult<int64_t> { 

        auto& db_slice = t->GetDbSlice(es->shard_id());
        auto it = db_slice.FindReadOnly(t->GetDbContext(), key);
        if (!IsValid(it.GetInnerIt()))
            return {OpStatus::KEY_NOTFOUND};

        if (!it.GetInnerIt()->first.HasExpire())
            return {OpStatus::SKIPPED};

        auto ttlTime = it.GetInnerIt()->first.GetExpireTime() - t->GetDbContext().GetTimeNowMs() / 1000;
        assert(ttlTime > 0);
        return ttlTime;
    };
  
    facade::OpResult<int64_t> res = co_await cmd::SingleHopT(cb);

    auto conn = cmd_cntx->conn_cntx()->owner(); 

    if(res.status() == OpStatus::OK)
    {
        
        conn->Send(res.value());
    }
    else 
    {
        if (res.status() == OpStatus::KEY_NOTFOUND) {
            conn->SendInteger(-2);
        }else if (res.status() == OpStatus::SKIPPED) {
            conn->SendInteger(-1);
        }else {
            conn->SendERROR();
        }
    }
    co_return;      
}

void GenericFamily::Ttl(CommandContext* cmd_cntx, CmdArgList args) {
    CmdTtl(cmd_cntx, args[1]);
}

void GenericFamily::Client_Info(CommandContext* cmd_cntx, CmdArgList args) {

    auto conn = cmd_cntx->conn_cntx()->owner(); 

    conn->SendStatus("OK");
    
}


void GenericFamily::ShutDown(CommandContext*, CmdArgList) {
    ser->Stop();
}


// void GenericFamily::Select(CmdArgList args, CommandContext* cmd_cntx) {
//   // TODO
// }

using CI = CommandId;
void GenericFamily::Register(CommandRegistry* registry) {

  registry->StartFamily();
  *registry
      << CI{"DEL", /*keys_start*/ 1, /*keys_nums*/ kInvalidKeysNum, /*keys_offset*/ 1}.SetHandler(&GenericFamily::Delex)
      << CI{"PING", kInvalidKeysStart, 0, kInvalidKeysOffset, CO::READABLE}.SetHandler(&GenericFamily::Ping)
      << CI{"EXISTS", 1, kInvalidKeysNum, 1, CO::READABLE}.SetHandler(&GenericFamily::Exists)
      << CI{"EXPIRE", 1, 1, kInvalidKeysOffset}.SetHandler(&GenericFamily::Expire)
      << CI{"EXPIRETIME", 1, 1, kInvalidKeysOffset, CO::READABLE}.SetHandler(&GenericFamily::ExpireTime)
      << CI{"TTL", 1, 1, kInvalidKeysOffset, CO::READABLE}.SetHandler(&GenericFamily::Ttl)
      << CI{"CLIENT", kInvalidKeysStart, 0, kInvalidKeysOffset, CO::READABLE}.SetHandler(&GenericFamily::Client_Info)
      << CI{"HELLO", kInvalidKeysStart, 0, kInvalidKeysOffset, CO::READABLE}.SetHandler(&GenericFamily::Client_Info)
      << CI{"SHUTDOWN", kInvalidKeysStart, 0, kInvalidKeysOffset}.SetHandler(&GenericFamily::ShutDown)
      ;
}

void RegisterGeneric(CommandRegistry* registry) {
  GenericFamily::Register(registry);
}

}  // namespace dfly

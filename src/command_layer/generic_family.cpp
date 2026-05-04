// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "generic_family.hpp"


#include <optional>

#include "cmd_support.hpp"

#include <atomic>

namespace dfly {

facade::OpResult<uint32_t> OpDel(const OpArgs& op_args, const ShardArgs& keys) {

    auto& db_slice = op_args.GetDbSlice();
    uint32_t res = 0;
    for (std::string_view key : keys) {
        auto it = db_slice.FindMutable(op_args.db_cntx, key).it;  // post_updater will run immediately
        if (!IsValid(it))
        continue;

        db_slice.Del(op_args.db_cntx, it, nullptr);
        ++res;
    }

    return res;
}

CoroTask CmdDel(CmdArgList args, CommandContext* cmd_cntx) {
    std::atomic<uint32_t> result = 0;
    auto cb = [&](Transaction* tx, EngineShard* es) {
        auto args = tx->GetShardArgs(es->shard_id());
        auto op_args = tx->GetOpArgs(es);
        auto res = OpDel(op_args, args, async_unlink);
        result.fetch_add(res.value_or(0), memory_order_relaxed);
        return OpStatus::OK;
    };

    co_await cmd::SingleHopT(cb);
    uint32_t del_cnt = result.load(memory_order_relaxed);

    auto* conn = cmd_cntx->conn_cntx()->owner_;
    conn->Send(del_cnt);
    
    co_return std::nullopt;
}

void GenericFamily::Delex(CmdArgList args, CommandContext* cmd_cntx) {
    CmdDel(args, cmd_cntx);
    return;
}


void GenericFamily::Ping(CmdArgList args, CommandContext* cmd_cntx) {
    if (args.size() > 1) {
        return cmd_cntx->SendError();
    }
    std::string_view msg = args[0];
    auto* conn = cmd_cntx->conn_cntx()->owner_;
    conn->send(msg);
}



CoroTask CmdExists(CmdArgList args, CommandContext* cmd_cntx) {

    auto Op = [](const OpArgs& op_args, const ShardArgs& keys) -> facade::OpResult<uint32_t> {
        auto& db_slice = op_args.GetDbSlice();
        uint32_t res = 0;

        for (string_view key : keys) {
          auto find_res = db_slice.FindReadOnly(op_args.db_cntx, key);
          res += IsValid(find_res);
        }
        return res;    
    };

    std::atomic<uint32_t> result{0};

    auto cb = [&result, &Op](Transaction* t, EngineShard* shard) -> facade::OpResukt<void> {
      ShardArgs args = t->GetShardArgs(shard->shard_id());
      auto res = Op(t->GetOpArgs(shard), args);
      result.fetch_add(res.value_or(0), memory_order_relaxed);
      return {OpStatus::OK};
    };

    auto res = co_await cmd::SingleHopT(cb);

    auto* conn = cmd_cntx->conn_cntx()->owner_;
    if(res.status() == OpStatus::OK)
    {
        
        conn->Send(result.load());  // FIXME
    }
    else 
    {
        conn->SendERROR();
    }

    co_return std::nullopt;

}

void GenericFamily::Exists(CmdArgList args, CommandContext* cmd_cntx) {
    OpExists(args, cmd_cntx);
}



CoroTask CmdExpire(const OpArgs& op_args, string_view key, const ExpireParams& params) {


    auto cb = [&](Transaction* t, EngineShard* shard) -> facade::OpResult<void> {
        auto& db_slice = op_args.GetDbSlice();
        auto find_res = db_slice.FindMutable(op_args.db_cntx, key);
        if (!IsValid(find_res.it_)) {
          return {OpStatus::KEY_NOTFOUND};
        }

        return db_slice.UpdateExpire(op_args.db_cntx, find_res.it, params);     
    };
    auto res = co_await cmd::SingleHopT(cb);

    auto* conn = cmd_cntx->conn_cntx()->owner_;
    if(res.status() == OpStatus::OK)
    {
        
        conn->SendOK();
    }
    else 
    {
        conn->SendERROR();
    }

    co_return std::nullopt;
}

void GenericFamily::Expire(CmdArgList args, CommandContext* cmd_cntx) {
    std::string_view key = args[0];
    std::string_view sec = args[1];
    int64_t int_arg = std::atoi(sec);
    OpExpire(t->GetOpArgs(shard), key, int_arg);
}



void GenericFamily::Keys(CmdArgList args, CommandContext* cmd_cntx) {
    // TODO
}


CoroTask CmdExpireTime(std::string_view key, CommandContext* cmd_cntx) {

    auto cb = [&](Transaction* t, EngineShard* shard) -> facade::OpResult<int64_t> {
      auto& db_slice = t->GetDbSlice(shard->shard_id());
      auto it = db_slice.FindReadOnly(t->GetDbContext(), key);
      if (!IsValid(it))
        return {OpStatus::KEY_NOTFOUND};

      if (!it->first.HasExpire())
        return {OpStatus::SKIPPED};

      int64_t ttl_ms = it->first.GetExpireTime();

      return {ttl_ms};
    };

    auto res = co_await cmd::SingleHopT(cb);

    auto* conn = cmd_cntx->conn_cntx()->owner_;
    if(res.status() == OpStatus::OK)
    {
        
        conn->Send(res.value());
    }
    else 
    {
        conn->SendError();
    }
    co_return std::nullopt;    
}


void GenericFamily::ExpireTime(CmdArgList args, CommandContext* cmd_cntx) {
    CmdExpireTime(args[0], cmd_cntx);
}


CoroTask CmdTtl(std::string_view key, CommandContext* cmd_cntx) {

    auto cb = [&](Transaction* t, EngineShard* shard) -> facade::OpResult<int64_t> { 

        auto& db_slice = t->GetDbSlice(shard->shard_id());
        auto it = db_slice.FindReadOnly(t->GetDbContext(), key);
        if (!IsValid(it))
            return {OpStatus::KEY_NOTFOUND};

        if (!it->first.HasExpire())
            return {OpStatus::SKIPPED};

        auto ttlTime = it->first.GetExpireTime();
        return ttlTime - t->GetDbContext().time_now_ms_;
    };
  
    auto res = co_await cmd::SingleHopT(cb);

    auto* conn = cmd_cntx->conn_cntx()->owner_;


    if(res.status() == OpStatus::OK)
    {
        
        conn->Send(res.value());
    }
    else 
    {
        conn->SendError();
    }
    co_return std::nullopt;      
}

void GenericFamily::Ttl(CmdArgList args, CommandContext* cmd_cntx) {
    CmdTtl(args[0], cmd_cntx);
}



void GenericFamily::Select(CmdArgList args, CommandContext* cmd_cntx) {
  // TODO
}


void GenericFamily::Register(CommandRegistry* registry) {
  constexpr auto kSelectOpts = CO::LOADING | CO::FAST;
  registry->StartFamily();
  *registry
      << CI{"DEL",-2, 1, -1}.SetAsyncHandler(CmdDel)
      << CI{"PING", -1, 0, 0}.SetHandler(&GenericFamily::Ping)
      << CI{"EXISTS", -2, 1, -1}.SetHandler(&GenericFamily::Exists)
      << CI{"EXPIRE", -3, 1, 1}.SetHandler(&GenericFamily::Expire)
      << CI{"KEYS", 2, 0, 0}.SetHandler(&GenericFamily::Keys)
      << CI{"TTL", CO::READONLY | CO::FAST, 2, 1, 1, acl::kTTL}.SetHandler(&GenericFamily::Ttl)
      ;
}

}  // namespace dfly

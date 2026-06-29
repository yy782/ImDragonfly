// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//


#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <variant>


#include "cmd_arg_parser.hpp"
#include "sharding/db_slice.hpp"
#include "sharding/engine_shard.hpp"
#include "command_registry.hpp"
#include "sharding/op_status.hpp"
#include "detail/conn_context.hpp"

#include "transaction_layer/transaction.hpp"
#include "cmd_support.hpp"
#include "network/redis_server.hpp"
namespace dfly {

namespace {

using CI = CommandId;

constexpr uint32_t kMaxStrLen = 1 << 28;
 
using StringResult = std::string;

StringResult ReadString(DbIndex dbid, std::string_view key, const PrimeValue& pv, EngineShard* es) {
    (void)dbid;
    (void)key;
    (void)es;
    // 分层存储扩展

    return StringResult{pv.ToString()};
}


using ::cmd::CmdArgParser;
using cmd::CoroTask;
using ::cmd::CmdArgParser;
using Slice = Transaction::Slice;
// Helper for performing SET operations with various options

class SetCmd { // SET 命令处理器
public:
    explicit SetCmd(const Slice& slice)
        : slice_(slice) {
    }

    enum SetFlags {
        SET_ALWAYS = 0,
        SET_KEEP_EXPIRE = 1 << 2,     /* KEEPTTL: Set and keep the ttl */
        SET_EXPIRE_AFTER_MS = 1 << 4, /* EX,PX,EXAT,PXAT: Expire after ms. */
    };

    struct SetParams {
        uint16_t flags_ = SET_ALWAYS;
        uint64_t expire_after_ms_ = 0;  // Relative value based on now. 0 means no expiration.

        constexpr bool IsConditionalSet() const {
            return false;
        }
    };

    facade::OpResult<void> Set(const SetParams& params, std::string_view key, std::string_view value);

private:
   facade::OpResult<void> SetExisting(const SetParams& params, std::string_view value,
                        DbSlice::ItAndUpdater* it_upd);

    void AddNew(const SetParams& params, const DbSlice::Iterator& it, std::string_view key,
                std::string_view value);

    Slice slice_;
};



facade::OpResult<void> SetCmd::Set(const SetParams& params, std::string_view key, std::string_view value) {



    DbSlice& db_slice = slice_.GetDbSlice();
    auto op_res = db_slice.AddOrFind(slice_.GetDbContext(), key, std::nullopt);


    if (!op_res->is_new) {
        return SetExisting(params, value, &(*op_res));
    } else {
        AddNew(params, op_res->it, key, value);
        return OpStatus::OK;
    }
}

facade::OpResult<void> SetCmd::SetExisting(const SetParams& params, std::string_view value,
                             DbSlice::ItAndUpdater* it_upd) {

  
    PrimeValue& prime_value = it_upd->it->second;


    auto& db_slice = slice_.GetDbSlice();
    uint64_t at_ms =
        params.expire_after_ms_ ? params.expire_after_ms_ + slice_.GetDbContext().GetTimeNowMs() : 0;

    if (!(params.flags_ & SET_KEEP_EXPIRE)) {
        if (at_ms) {
            db_slice.AddExpire(slice_.GetDbContext().GetDbIndex(), it_upd->it, at_ms);
        } else {
            db_slice.RemoveExpire(slice_.GetDbContext().GetDbIndex(), it_upd->it);
        }
    }
    prime_value.SetString(value);
    return OpStatus::OK;
}

void SetCmd::AddNew(const SetParams& params, const DbSlice::Iterator& it, std::string_view key,
                    std::string_view value) {

    (void)key;                    

  auto& db_slice = slice_.GetDbSlice();
  it->second = PrimeValue{value};

  if (params.expire_after_ms_) {
      db_slice.AddExpire(slice_.GetDbContext().GetDbIndex(), it,
                        params.expire_after_ms_ + slice_.GetDbContext().GetTimeNowMs());
  }
}




struct NegativeExpire {};  // Returned if relative expiry was in the past
struct ErrorReply{};
std::variant<SetCmd::SetParams, ErrorReply, NegativeExpire> ParseSetParams(
    CmdArgParser parser, const CommandContext* cmd_cntx) {
    SetCmd::SetParams sparams;
    (void)cmd_cntx;
    while (parser.HasNext()) {
        if (parser.Check("EX")) { // not same
            if (parser.HasError())
                return ErrorReply{};

            sparams.flags_ |= SetCmd::SET_EXPIRE_AFTER_MS;
        }
    }
    return sparams;
}

CoroTask CmdMSet(CommandContext* cmd_cntx, CmdArgList args) {
    auto cb = [&args](Transaction* tx, EngineShard* es) -> OpResult<void> {
        auto& slice = tx->GetSlice(es->shard_id());
        for (const auto& [key, keyId] : slice) {
            auto& value = args[keyId + 1];
            auto it_res = tx->GetDbSlice(es->shard_id()).AddOrUpdate(tx->GetDbContext(), key, PrimeValue{value}, 0);
            if (it_res.status() !=  facade::OpStatus::OK) {
                // TODO 
            }            
        }            
        return {};       
    };
    co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();
    conn->SendStatus("OK");
    co_return;
}


CoroTask CmdSet(CommandContext* cmd_cntx, CmdArgList args) {
    args = args.subspan(1); // Skip command name

    CmdArgParser parser{args};

    auto [key, value] = parser.Next<std::string_view, std::string_view>();
    auto params_result = ParseSetParams(parser, cmd_cntx); // 解析 SET 命令的选项（如 EX 、 PX 、 NX 等）


    auto& sparams = std::get<SetCmd::SetParams>(params_result); // 获取解析后的 SetParams 结构体

    auto cb = [key, 
           value, 
           sparams](Transaction* t, EngineShard* shard)-> OpResult<void> {
        return SetCmd(t->GetSlice(shard->shard_id())).Set(sparams, key, value);
    };

    auto result = co_await cmd::SingleHopT(cb);


    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        conn->SendStatus("OK"); 
    } else {
        conn->SendERROR();
    }

    co_return;
}



CoroTask CmdMGet(CommandContext* cmd_cntx, CmdArgList /*args*/) {
 
    std::vector<std::string> vec(cmd_cntx->tx()->GetKeyNum());
    auto cb = [&vec](Transaction* tx, EngineShard* es) -> OpResult<void> {
        auto& slice = tx->GetSlice(es->shard_id());
        for (auto& [key, keyId] : slice) {
            auto it_res = tx->GetDbSlice(es->shard_id()).FindReadOnly(tx->GetDbContext(), key);
            if (it_res.GetInnerIt().owner() == nullptr) { // 没找到
                vec[keyId - 1] = ""; // args第一个参数是MGET,与vec不同，要减一
            }else {
                vec[keyId - 1] = ReadString(tx->GetDbIndex(), key, it_res.GetInnerIt()->second, es);
            }             
        }
        return {};        
    };
    co_await cmd::SingleHopT(cb); 
    auto conn = cmd_cntx->conn_cntx()->owner();
    conn->SendVec(std::move(vec));        


    co_return;


}
CoroTask CmdGet(CommandContext* cmd_cntx, CmdArgList args) {


    auto cb = [key = args[1]](Transaction* tx, EngineShard* es) -> OpResult<StringResult> {
        auto it_res = tx->GetDbSlice(es->shard_id()).FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) { // 没找到
            return OpStatus::KEY_NOTFOUND;
        }


        return {ReadString(tx->GetDbIndex(), key, it_res.GetInnerIt()->second, es)};
    };
    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner(); 
    if (result.status() == OpStatus::OK) {   
        conn->Send(result.value());
    } else {
        conn->Send(std::string());
    }    
    co_return;
}


}  // namespace



void Set(CommandContext* cmd_cntx, CmdArgList args) {
    CmdSet(cmd_cntx, args);
}

void Get(CommandContext* cmd_cntx, CmdArgList args) {
    CmdGet(cmd_cntx, args);
}
void MSET(CommandContext* cmd_cntx, CmdArgList args) {
    CmdMSet(cmd_cntx, args);
}

void MGET(CommandContext* cmd_cntx, CmdArgList args) {
    CmdMGet(cmd_cntx, args);
}


void RegisterStringFamily(CommandRegistry* registry) {

    registry->StartFamily();
    *registry
        << CI{"SET", /*keys_start*/ 1, /*keys_nums*/ 1, /*keys_offset*/ kInvalidKeysOffset}.SetHandler(Set)
        << CI{"GET", 1, 1, kInvalidKeysOffset}.SetHandler(Get)
        << CI{"MGET", 1, kInvalidKeysNum, 1}.SetHandler(MGET)
        << CI{"MSET", 1, kInvalidKeysNum, 2}.SetHandler(MSET)
        ;
}

}  // namespace dfly

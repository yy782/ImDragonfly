// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//


#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <variant>

#include "base/flags.h"
#include "base/logging.h"
#include "base/stl_util.h"
#include "util/fibers/future.h"

#include "db_slice.hpp"
#include "engine_shard.hpp"
#include "command_registry.hpp"
#include "op_status.hpp"
#include "conn_context2.hpp"
#include "transaction.hpp"



namespace dfly {

namespace {

using CI = CommandId;

constexpr uint32_t kMaxStrLen = 1 << 28;
 
using StringResult = std::string;

StringResult ReadString(DbIndex dbid, std::string_view key, const PrimeValue& pv, EngineShard* es) {
    return StringResult{pv.ToString()};
}




// Helper for performing SET operations with various options
class SetCmd { // SET 命令处理器
public:
    explicit SetCmd(OpArgs op_args)
        : op_args_(op_args) {
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

    const OpArgs op_args_;

};



facade::OpResult<void> SetCmd::Set(const SetParams& params, string_view key, string_view value) {
    auto& db_slice = op_args_.GetDbSlice();
    auto op_res = db_slice.AddOrFind(op_args_.db_cntx, key, std::nullopt);


    if (!op_res->is_new) {
        return SetExisting(params, value, &(*op_res));
    } else {
        AddNew(params, op_res->it, key, value);
        return OpStatus::OK;
    }
}

facade::OpResult<void> SetCmd::SetExisting(const SetParams& params, string_view value,
                             DbSlice::ItAndUpdater* it_upd) {

    PrimeKey& key = it_upd->it->first;
    PrimeValue& prime_value = it_upd->it->second;
    EngineShard* shard = op_args_.shard_;

    auto& db_slice = op_args_.GetDbSlice();
    uint64_t at_ms =
        params.expire_after_ms ? params.expire_after_ms + op_args_.db_cntx_.time_now_ms : 0;

    if (!(params.flags & SET_KEEP_EXPIRE)) {
        if (at_ms) {
            db_slice.AddExpire(op_args_.db_cntx.db_index, it_upd->it, at_ms);
        } else {
            db_slice.RemoveExpire(op_args_.db_cntx.db_index, it_upd->it);
        }
    }
    prime_value.SetString(value);
    return OpStatus::OK;
}

void SetCmd::AddNew(const SetParams& params, const DbSlice::Iterator& it, std::string_view key,
                    std::string_view value) {
  auto& db_slice = op_args_.GetDbSlice();
  it->second = PrimeValue{value};

  if (params.expire_after_ms) {
      db_slice.AddExpire(op_args_.db_cntx.db_index, it,
                        params.expire_after_ms + op_args_.db_cntx.time_now_ms);
  }
}




struct NegativeExpire {};  // Returned if relative expiry was in the past
struct ErrorReply{};
std::variant<SetCmd::SetParams, ErrorReply, NegativeExpire> ParseSetParams(
    CmdArgParser parser, const CommandContext* cmd_cntx) {
    SetCmd::SetParams sparams;

    while (parser.HasNext()) {
        if (parser.check("EX")) { // not same
            if (parser.HasError())
                return ErrorReply{};

            sparams.flags |= SetCmd::SET_EXPIRE_AFTER_MS;
        }
    }
    return sparams;
}

using CoroTask = cppcoro::task<void, cmd::Coro>;


CoroTask CmdSet(CmdArgList args, CommandContext* cmd_cntx) {
    facade::CmdArgParser parser{args};

    auto [key, value] = parser.Next<string_view, string_view>();
    auto params_result = ParseSetParams(parser, cmd_cntx); // 解析 SET 命令的选项（如 EX 、 PX 、 NX 等）


    auto& sparams = std::get<SetCmd::SetParams>(params_result); // 获取解析后的 SetParams 结构体

    auto cb = [&](Transaction* t, EngineShard* shard)-> OpResult<void> {
        return SetCmd(t->GetOpArgs(shard)).Set(sparams, key, value);
    };

    auto result = co_await cmd::SingleHopT(cb);


    auto* conn = cmd_cntx->conn_cntx()->owner_;

    if (result.status() == OpStatus::OK) {
        conn->SendOk();
    } else {
        conn->SendERROR();
    }

    co_return std::nullopt;
}

CoroTask CmdGet(CmdArgList args, CommandContext* cmd_cntx) {
    auto cb = [key = args[0]](Transaction* tx, EngineShard* es) -> OpResult<StringResult> {
        auto it_res = tx->GetDbSlice(es->shard_id()).FindReadOnly(tx->GetDbContext(), key, OBJ_STRING);
        if (!it_res.ok())
            return it_res.status();

        return ReadString(tx->GetDbIndex(), key, (*it_res)->second, es);
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto* conn = cmd_cntx->conn_cntx()->owner_;
    if (result.status() == OpStatus::OK) {
        conn->Send(result.value());
    } else {
        conn->SendERROR();
    }    
    co_return std::nullopt;
}


}  // namespace


void RegisterStringFamily(CommandRegistry* registry) {
  constexpr uint32_t kMSetMask = CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL;

  registry->StartFamily();
  *registry
      << CI{"SET", -3, 1, 1}.SetAsyncHandler(
             CmdSet)
      << CI{"GET", 2, 1, 1}.SetAsyncHandler(CmdGet)
      ;
}

}  // namespace dfly

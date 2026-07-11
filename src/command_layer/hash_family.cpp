#include "cmd_arg_parser.hpp"
#include "sharding/db_slice.hpp"
#include "sharding/engine_shard.hpp"
#include "command_registry.hpp"
#include "sharding/op_status.hpp"
#include "detail/conn_context.hpp"
#include "transaction_layer/transaction.hpp"
#include "cmd_support.hpp"
#include "network/redis_server.hpp"
#include "redis/redis_aux.hpp"
#include "sharding/DashTable/compact_obj.hpp"

namespace dfly {

namespace {

using CI = CommandId;
using cmd::CoroTask;
using Slice = Transaction::Slice;

HashObject* GetOrCreateHash(Transaction* tx, EngineShard* es, std::string_view key) {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto op_res = db_slice.AddOrFind(tx->GetDbContext(), key, OBJ_HASH);

    if (!op_res) {
        return nullptr;
    }

    PrimeValue& prime_value = op_res->it->second;

    if (op_res->is_new) {
        prime_value = CompactValue::MakeHash();
    } else if (prime_value.ObjType() != OBJ_HASH) {
        return nullptr;
    }

    return prime_value.GetHash();
}

// HSET 命令：设置哈希表中的字段值
CoroTask CmdHSet(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    auto field = args[2];
    auto value = args[3];

    auto cb = [key, field, value](Transaction* tx, EngineShard* es) -> OpResult<int> {
        HashObject* hash = GetOrCreateHash(tx, es, key);
        if (!hash) {
            return OpStatus::WRONG_TYPE;
        }

        bool existed = hash->Exists(std::string(field));
        hash->Set(std::string(field), std::string(value));
        return existed ? 0 : 1;
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        conn->SendInteger(static_cast<int64_t>(result.value()));
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

// HGET 命令：获取哈希表中字段的值
CoroTask CmdHGet(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    auto field = args[2];

    auto cb = [key, field](Transaction* tx, EngineShard* es) -> OpResult<std::string> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return OpStatus::KEY_NOTFOUND;
        }

        const PrimeValue& prime_value = it_res.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_HASH) {
            return OpStatus::WRONG_TYPE;
        }

        const HashObject* hash = prime_value.GetHash();
        std::string val = hash->Get(std::string(field));
        if (val.empty()) {
            return OpStatus::KEY_NOTFOUND;
        }
        return val;
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        conn->SendString(result.value());
    } else {
        conn->SendNULL();
    }

    co_return;
}

// HDEL 命令：删除哈希表中的一个或多个字段
CoroTask CmdHDel(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    auto fields = args.subspan(2);

    auto cb = [key, fields](Transaction* tx, EngineShard* es) -> OpResult<size_t> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindMutable(tx->GetDbContext(), key);

        if (it_res.it.GetInnerIt().owner() == nullptr) {
            return 0ULL;
        }

        PrimeValue& prime_value = it_res.it.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_HASH) {
            return OpStatus::WRONG_TYPE;
        }

        HashObject* hash = prime_value.GetHash();
        size_t deleted = 0;
        for (const auto& field : fields) {
            deleted += hash->Del(std::string(field));
        }
        return deleted;
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        conn->SendInteger(static_cast<int64_t>(result.value()));
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

// HEXISTS 命令：检查哈希表中是否存在指定字段
CoroTask CmdHExists(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    auto field = args[2];

    auto cb = [key, field](Transaction* tx, EngineShard* es) -> OpResult<int> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return 0;
        }

        const PrimeValue& prime_value = it_res.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_HASH) {
            return OpStatus::WRONG_TYPE;
        }

        const HashObject* hash = prime_value.GetHash();
        return hash->Exists(std::string(field)) ? 1 : 0;
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        conn->SendInteger(static_cast<int64_t>(result.value()));
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

// HLEN 命令：返回哈希表中字段的数量
CoroTask CmdHLen(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];

    auto cb = [key](Transaction* tx, EngineShard* es) -> OpResult<size_t> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return 0ULL;
        }

        const PrimeValue& prime_value = it_res.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_HASH) {
            return OpStatus::WRONG_TYPE;
        }

        const HashObject* hash = prime_value.GetHash();
        return hash->Length();
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        conn->SendInteger(static_cast<int64_t>(result.value()));
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

void HSet(CommandContext* cmd_cntx, CmdArgList args) {
    CmdHSet(cmd_cntx, args);
}

void HGet(CommandContext* cmd_cntx, CmdArgList args) {
    CmdHGet(cmd_cntx, args);
}

void HDel(CommandContext* cmd_cntx, CmdArgList args) {
    CmdHDel(cmd_cntx, args);
}

void HExists(CommandContext* cmd_cntx, CmdArgList args) {
    CmdHExists(cmd_cntx, args);
}

void HLen(CommandContext* cmd_cntx, CmdArgList args) {
    CmdHLen(cmd_cntx, args);
}

}  // namespace

void RegisterHashFamily(CommandRegistry* registry) {
    registry->StartFamily();
    *registry
        << CI{"HSET", 1, 1, kInvalidKeysOffset}.SetHandler(HSet)
        << CI{"HGET", 1, 1, kInvalidKeysOffset, CO::READABLE}.SetHandler(HGet)
        << CI{"HDEL", 1, 1, kInvalidKeysOffset}.SetHandler(HDel)
        << CI{"HEXISTS", 1, 1, kInvalidKeysOffset, CO::READABLE}.SetHandler(HExists)
        << CI{"HLEN", 1, 1, kInvalidKeysOffset, CO::READABLE}.SetHandler(HLen)
        ;
}

}  // namespace dfly
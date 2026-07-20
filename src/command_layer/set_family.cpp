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

SetObject* GetOrCreateSet(Transaction* tx, EngineShard* es, std::string_view key) {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto op_res = db_slice.AddOrFind(tx->GetDbContext(), key, OBJ_SET);

    if (!op_res) {
        return nullptr;
    }

    PrimeValue& prime_value = op_res->it->second;

    if (op_res->is_new) {
        prime_value = CompactValue::MakeSet();
    } else if (prime_value.ObjType() != OBJ_SET) {
        return nullptr;
    }

    return prime_value.GetSet();
}

// SADD 命令：向集合添加一个或多个成员
CoroTask CmdSAdd(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    auto members = args.subspan(2);

    auto cb = [key, members](Transaction* tx, EngineShard* es) -> OpResult<size_t> {
        SetObject* set = GetOrCreateSet(tx, es, key);
        if (!set) {
            return OpStatus::WRONG_TYPE;
        }

        size_t added = 0;
        for (const auto& member : members) {
            added += set->Add(std::string(member));
        }
        return added;
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto* t = cmd_cntx->tx();

    if (result.status() == OpStatus::OK) {
        t->CollectedResult(BuildInteger(static_cast<int64_t>(result.value())));
    } else {
        t->CollectedResult(BuildError("WRONGTYPE Operation against a key holding the wrong kind of value"));
    }

    co_return;
}

// SREM 命令：从集合中移除一个或多个成员
CoroTask CmdSRem(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    auto members = args.subspan(2);

    auto cb = [key, members](Transaction* tx, EngineShard* es) -> OpResult<size_t> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindMutable(tx->GetDbContext(), key);

        if (it_res.it.GetInnerIt().owner() == nullptr) {
            return 0ULL;
        }

        PrimeValue& prime_value = it_res.it.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_SET) {
            return OpStatus::WRONG_TYPE;
        }

        SetObject* set = prime_value.GetSet();
        size_t removed = 0;
        for (const auto& member : members) {
            removed += set->Remove(std::string(member));
        }
        return removed;
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto* t = cmd_cntx->tx();

    if (result.status() == OpStatus::OK) {
        t->CollectedResult(BuildInteger(static_cast<int64_t>(result.value())));
    } else {
        t->CollectedResult(BuildError("WRONGTYPE Operation against a key holding the wrong kind of value"));
    }

    co_return;
}

// SMEMBERS 命令：返回集合中的所有成员
CoroTask CmdSMembers(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    std::vector<std::string> members;

    auto cb = [key, &members](Transaction* tx, EngineShard* es) -> OpResult<void> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return {};
        }

        const PrimeValue& prime_value = it_res.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_SET) {
            return OpStatus::WRONG_TYPE;
        }

        const SetObject* set = prime_value.GetSet();
        for (const auto& member : set->Data()) {
            members.push_back(member);
        }
        return {};
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto* t = cmd_cntx->tx();

    if (result.status() == OpStatus::OK) {
        t->CollectedResult(BuildArray(std::move(members)));
    } else {
        t->CollectedResult(BuildError("WRONGTYPE Operation against a key holding the wrong kind of value"));
    }

    co_return;
}

// SCARD 命令：返回集合的大小
CoroTask CmdSCard(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];

    auto cb = [key](Transaction* tx, EngineShard* es) -> OpResult<size_t> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return 0ULL;
        }

        const PrimeValue& prime_value = it_res.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_SET) {
            return OpStatus::WRONG_TYPE;
        }

        const SetObject* set = prime_value.GetSet();
        return set->Length();
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto* t = cmd_cntx->tx();

    if (result.status() == OpStatus::OK) {
        t->CollectedResult(BuildInteger(static_cast<int64_t>(result.value())));
    } else {
        t->CollectedResult(BuildError("WRONGTYPE Operation against a key holding the wrong kind of value"));
    }

    co_return;
}

// SISMEMBER 命令：检查成员是否在集合中
CoroTask CmdSIsMember(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    auto member = args[2];

    auto cb = [key, member](Transaction* tx, EngineShard* es) -> OpResult<int> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return 0;
        }

        const PrimeValue& prime_value = it_res.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_SET) {
            return OpStatus::WRONG_TYPE;
        }

        const SetObject* set = prime_value.GetSet();
        return set->Contains(std::string(member)) ? 1 : 0;
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto* t = cmd_cntx->tx();

    if (result.status() == OpStatus::OK) {
        t->CollectedResult(BuildInteger(static_cast<int64_t>(result.value())));
    } else {
        t->CollectedResult(BuildError("WRONGTYPE Operation against a key holding the wrong kind of value"));
    }

    co_return;
}

void SAdd(CommandContext* cmd_cntx, CmdArgList args) {
    CmdSAdd(cmd_cntx, args);
}

void SRem(CommandContext* cmd_cntx, CmdArgList args) {
    CmdSRem(cmd_cntx, args);
}

void SMembers(CommandContext* cmd_cntx, CmdArgList args) {
    CmdSMembers(cmd_cntx, args);
}

void SCard(CommandContext* cmd_cntx, CmdArgList args) {
    CmdSCard(cmd_cntx, args);
}

void SIsMember(CommandContext* cmd_cntx, CmdArgList args) {
    CmdSIsMember(cmd_cntx, args);
}

}  // namespace

void RegisterSetFamily(CommandRegistry* registry) {
    registry->StartFamily();
    *registry
        << CI{"SADD", 1, 1, kInvalidKeysOffset}.SetHandler(SAdd)
        << CI{"SREM", 1, 1, kInvalidKeysOffset}.SetHandler(SRem)
        << CI{"SMEMBERS", 1, 1, kInvalidKeysOffset, CO::READABLE}.SetHandler(SMembers)
        << CI{"SCARD", 1, 1, kInvalidKeysOffset, CO::READABLE}.SetHandler(SCard)
        << CI{"SISMEMBER", 1, 1, kInvalidKeysOffset, CO::READABLE}.SetHandler(SIsMember)
        ;
}

}  // namespace dfly
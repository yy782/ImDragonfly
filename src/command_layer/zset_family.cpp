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

using CI = CommandId;
using cmd::CoroTask;
using Slice = Transaction::Slice;

namespace {

ZSetObject* GetOrCreateZSet(Transaction* tx, EngineShard* es, std::string_view key) {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto op_res = db_slice.AddOrFind(tx->GetDbContext(), key, OBJ_ZSET);

    if (!op_res) {
        return nullptr;
    }

    PrimeValue& prime_value = op_res->it->second;

    if (op_res->is_new) {
        prime_value = CompactValue::MakeZSet();
    } else if (prime_value.ObjType() != OBJ_ZSET) {
        return nullptr;
    }

    return prime_value.GetZSet();
}

CoroTask CmdZAdd(CommandContext* cmd_cntx, CmdArgList args) {
    if (args.size() < 4) {
        auto conn = cmd_cntx->conn_cntx()->owner();
        conn->SendERROR("wrong number of arguments for 'zadd' command");
        co_return;
    }

    auto key = args[1];
    auto cb = [key, args](Transaction* tx, EngineShard* es) -> OpResult<size_t> {
        ZSetObject* zset = GetOrCreateZSet(tx, es, key);
        if (!zset) {
            return OpStatus::WRONG_TYPE;
        }

        size_t added = 0;
        for (size_t i = 2; i + 1 < args.size(); i += 2) {
            double score = std::stod(std::string(args[i]));
            std::string_view member = args[i + 1];
            added += zset->Add(std::string(member), score);
        }

        return added;
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        conn->Send(static_cast<int64_t>(result.value()));
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

CoroTask CmdZCard(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];

    auto cb = [key](Transaction* tx, EngineShard* es) -> OpResult<size_t> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return 0ULL;
        }

        const PrimeValue& prime_value = it_res.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_ZSET) {
            return OpStatus::WRONG_TYPE;
        }

        const ZSetObject* zset = prime_value.GetZSet();
        return zset->Length();
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        conn->Send(static_cast<int64_t>(result.value()));
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

CoroTask CmdZScore(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    std::string_view member = args[2];

    auto cb = [key, member](Transaction* tx, EngineShard* es) -> OpResult<std::string> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return OpStatus::KEY_NOTFOUND;
        }

        const PrimeValue& prime_value = it_res.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_ZSET) {
            return OpStatus::WRONG_TYPE;
        }

        const ZSetObject* zset = prime_value.GetZSet();
        auto score = zset->Score(std::string(member));
        if (!score.has_value()) {
            return OpStatus::KEY_NOTFOUND;
        }
        return std::to_string(score.value());
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        conn->Send(result.value());
    } else if (result.status() == OpStatus::KEY_NOTFOUND) {
        conn->SendStatus("(nil)");
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

CoroTask CmdZRem(CommandContext* cmd_cntx, CmdArgList args) {
    if (args.size() < 3) {
        auto conn = cmd_cntx->conn_cntx()->owner();
        conn->SendERROR("wrong number of arguments for 'zrem' command");
        co_return;
    }

    auto key = args[1];

    auto cb = [key, args](Transaction* tx, EngineShard* es) -> OpResult<size_t> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return 0ULL;
        }

        PrimeValue& prime_value = const_cast<PrimeValue&>(it_res.GetInnerIt()->second);
        if (prime_value.ObjType() != OBJ_ZSET) {
            return OpStatus::WRONG_TYPE;
        }

        ZSetObject* zset = prime_value.GetZSet();
        size_t removed = 0;
        for (size_t i = 2; i < args.size(); ++i) {
            removed += zset->Remove(std::string(args[i]));
        }

        return removed;
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        conn->Send(static_cast<int64_t>(result.value()));
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

CoroTask CmdZRank(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    std::string_view member = args[2];

    auto cb = [key, member](Transaction* tx, EngineShard* es) -> OpResult<int64_t> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return -1;
        }

        const PrimeValue& prime_value = it_res.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_ZSET) {
            return OpStatus::WRONG_TYPE;
        }

        const ZSetObject* zset = prime_value.GetZSet();
        return zset->Rank(std::string(member));
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        if (result.value() >= 0) {
            conn->Send(result.value());
        } else {
            conn->SendStatus("(nil)");
        }
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

CoroTask CmdZRevRank(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    std::string_view member = args[2];

    auto cb = [key, member](Transaction* tx, EngineShard* es) -> OpResult<int64_t> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return -1;
        }

        const PrimeValue& prime_value = it_res.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_ZSET) {
            return OpStatus::WRONG_TYPE;
        }

        const ZSetObject* zset = prime_value.GetZSet();
        return zset->RevRank(std::string(member));
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        if (result.value() >= 0) {
            conn->Send(result.value());
        } else {
            conn->SendStatus("(nil)");
        }
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

CoroTask CmdZRange(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    int64_t start = std::stoll(std::string(args[2]));
    int64_t end = std::stoll(std::string(args[3]));
    bool with_scores = false;

    if (args.size() >= 5 && args[4] == "WITHSCORES") {
        with_scores = true;
    }

    auto cb = [key, start, end](Transaction* tx, EngineShard* es) -> OpResult<std::vector<std::pair<std::string, double>>> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return std::vector<std::pair<std::string, double>>{};
        }

        const PrimeValue& prime_value = it_res.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_ZSET) {
            return OpStatus::WRONG_TYPE;
        }

        const ZSetObject* zset = prime_value.GetZSet();
        return zset->Range(start, end);
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        std::vector<std::string> resp;
        for (const auto& pair : result.value()) {
            resp.push_back(pair.first);
            if (with_scores) {
                resp.push_back(std::to_string(pair.second));
            }
        }
        conn->SendVec(resp);
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

CoroTask CmdZRevRange(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    int64_t start = std::stoll(std::string(args[2]));
    int64_t end = std::stoll(std::string(args[3]));
    bool with_scores = false;

    if (args.size() >= 5 && args[4] == "WITHSCORES") {
        with_scores = true;
    }

    auto cb = [key, start, end](Transaction* tx, EngineShard* es) -> OpResult<std::vector<std::pair<std::string, double>>> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return std::vector<std::pair<std::string, double>>{};
        }

        const PrimeValue& prime_value = it_res.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_ZSET) {
            return OpStatus::WRONG_TYPE;
        }

        const ZSetObject* zset = prime_value.GetZSet();
        return zset->RevRange(start, end);
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        std::vector<std::string> resp;
        for (const auto& pair : result.value()) {
            resp.push_back(pair.first);
            if (with_scores) {
                resp.push_back(std::to_string(pair.second));
            }
        }
        conn->SendVec(resp);
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

void ZAdd(CommandContext* cmd_cntx, CmdArgList args) {
    CmdZAdd(cmd_cntx, args);
}

void ZCard(CommandContext* cmd_cntx, CmdArgList args) {
    CmdZCard(cmd_cntx, args);
}

void ZScore(CommandContext* cmd_cntx, CmdArgList args) {
    CmdZScore(cmd_cntx, args);
}

void ZRem(CommandContext* cmd_cntx, CmdArgList args) {
    CmdZRem(cmd_cntx, args);
}

void ZRank(CommandContext* cmd_cntx, CmdArgList args) {
    CmdZRank(cmd_cntx, args);
}

void ZRevRank(CommandContext* cmd_cntx, CmdArgList args) {
    CmdZRevRank(cmd_cntx, args);
}

void ZRange(CommandContext* cmd_cntx, CmdArgList args) {
    CmdZRange(cmd_cntx, args);
}

void ZRevRange(CommandContext* cmd_cntx, CmdArgList args) {
    CmdZRevRange(cmd_cntx, args);
}

}  // namespace

void RegisterZSetFamily(CommandRegistry* registry) {
    registry->StartFamily();
    *registry
        << CI{"ZADD", 1, 1, kInvalidKeysOffset}.SetHandler(ZAdd)
        << CI{"ZCARD", 1, 1, kInvalidKeysOffset, CO::READABLE}.SetHandler(ZCard)
        << CI{"ZSCORE", 1, 1, kInvalidKeysOffset, CO::READABLE}.SetHandler(ZScore)
        << CI{"ZREM", 1, 1, kInvalidKeysOffset}.SetHandler(ZRem)
        << CI{"ZRANK", 1, 1, kInvalidKeysOffset, CO::READABLE}.SetHandler(ZRank)
        << CI{"ZREVRANK", 1, 1, kInvalidKeysOffset, CO::READABLE}.SetHandler(ZRevRank)
        << CI{"ZRANGE", 1, 1, kInvalidKeysOffset, CO::READABLE}.SetHandler(ZRange)
        << CI{"ZREVRANGE", 1, 1, kInvalidKeysOffset, CO::READABLE}.SetHandler(ZRevRange)
        ;
}

}  // namespace dfly
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

// 辅助函数：确保键存在且是列表类型
ListObject* GetOrCreateList(Transaction* tx, EngineShard* es, std::string_view key) {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto op_res = db_slice.AddOrFind(tx->GetDbContext(), key, OBJ_LIST);

    if (!op_res) {
        return nullptr;
    }

    PrimeValue& prime_value = op_res->it->second;

    if (op_res->is_new) {
        prime_value = CompactValue::MakeList();
    } else if (prime_value.ObjType() != OBJ_LIST) {
        return nullptr;
    }

    return prime_value.GetList();
}

// LPUSH 命令：将一个或多个值插入到列表头部
CoroTask CmdLPush(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    auto values = args.subspan(2);

    auto cb = [key, values](Transaction* tx, EngineShard* es) -> OpResult<size_t> {
        ListObject* list = GetOrCreateList(tx, es, key);
        if (!list) {
            return OpStatus::WRONG_TYPE;
        }

        for (const auto& val : values) {
            list->PushFront(std::string(val));
        }
        return list->Length();
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

// RPUSH 命令：将一个或多个值插入到列表尾部
CoroTask CmdRPush(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    auto values = args.subspan(2);

    auto cb = [key, values](Transaction* tx, EngineShard* es) -> OpResult<size_t> {
        ListObject* list = GetOrCreateList(tx, es, key);
        if (!list) {
            return OpStatus::WRONG_TYPE;
        }

        for (const auto& val : values) {
            list->PushBack(std::string(val));
        }
        return list->Length();
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

// LPOP 命令：移除并返回列表的第一个元素
CoroTask CmdLPop(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];

    auto cb = [key](Transaction* tx, EngineShard* es) -> OpResult<std::string> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindMutable(tx->GetDbContext(), key);

        if (it_res.it.GetInnerIt().owner() == nullptr) {
            return OpStatus::KEY_NOTFOUND;
        }

        PrimeValue& prime_value = it_res.it.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_LIST) {
            return OpStatus::WRONG_TYPE;
        }

        ListObject* list = prime_value.GetList();
        if (list->Empty()) {
            return std::string("");
        }

        return list->PopFront();
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        if (result.value().empty()) {
            conn->SendERROR();
        } else {
            conn->Send(result.value());
        }
    } else if (result.status() == OpStatus::KEY_NOTFOUND) {
        conn->SendERROR();
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

// RPOP 命令：移除并返回列表的最后一个元素
CoroTask CmdRPop(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];

    auto cb = [key](Transaction* tx, EngineShard* es) -> OpResult<std::string> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindMutable(tx->GetDbContext(), key);

        if (it_res.it.GetInnerIt().owner() == nullptr) {
            return OpStatus::KEY_NOTFOUND;
        }

        PrimeValue& prime_value = it_res.it.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_LIST) {
            return OpStatus::WRONG_TYPE;
        }

        ListObject* list = prime_value.GetList();
        if (list->Empty()) {
            return std::string("");
        }

        return list->PopBack();
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        if (result.value().empty()) {
            conn->SendERROR();
        } else {
            conn->Send(result.value());
        }
    } else if (result.status() == OpStatus::KEY_NOTFOUND) {
        conn->SendERROR();
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

// LLEN 命令：返回列表的长度
CoroTask CmdLLen(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];

    auto cb = [key](Transaction* tx, EngineShard* es) -> OpResult<size_t> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return 0ULL;
        }

        const PrimeValue& prime_value = it_res.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_LIST) {
            return OpStatus::WRONG_TYPE;
        }

        const ListObject* list = prime_value.GetList();
        return list->Length();
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

// LINDEX 命令：获取列表中指定索引的元素
CoroTask CmdLIndex(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    int64_t index = std::stoll(std::string(args[2]));

    auto cb = [key, index](Transaction* tx, EngineShard* es) -> OpResult<std::string> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return OpStatus::KEY_NOTFOUND;
        }

        const PrimeValue& prime_value = it_res.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_LIST) {
            return OpStatus::WRONG_TYPE;
        }

        const ListObject* list = prime_value.GetList();
        std::string val = list->GetElement(index);
        if (val.empty()) {
            return OpStatus::KEY_NOTFOUND;
        }
        return val;
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        conn->Send(result.value());
    } else {
        conn->SendNULL();
    }

    co_return;
}

// LSET 命令：设置列表中指定索引的元素
CoroTask CmdLSet(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    int64_t index = std::stoll(std::string(args[2]));
    auto value = args[3];

    auto cb = [key, index, value](Transaction* tx, EngineShard* es) -> OpResult<void> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindMutable(tx->GetDbContext(), key);

        if (it_res.it.GetInnerIt().owner() == nullptr) {
            return OpStatus::NO_KEY;
        }

        PrimeValue& prime_value = it_res.it.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_LIST) {
            return OpStatus::WRONG_TYPE;
        }

        ListObject* list = prime_value.GetList();
        if (!list->SetElement(index, std::string(value))) {
            return OpStatus::OUT_OF_RANGE;
        }
        return {};
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        conn->SendStatus("OK");
    } else if (result.status() == OpStatus::NO_KEY) {
        conn->SendERROR("no such key");
    } else if (result.status() == OpStatus::OUT_OF_RANGE) {
        conn->SendERROR("index out of range");
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

// LRANGE 命令：获取列表指定范围的元素
CoroTask CmdLRange(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    int64_t start = std::stoll(std::string(args[2]));
    int64_t end = std::stoll(std::string(args[3]));
    std::vector<std::string> result_values;

    auto cb = [key, start, end, &result_values](Transaction* tx, EngineShard* es) -> OpResult<void> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);

        if (it_res.GetInnerIt().owner() == nullptr) {
            return {};
        }

        const PrimeValue& prime_value = it_res.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_LIST) {
            return OpStatus::WRONG_TYPE;
        }

        const ListObject* list = prime_value.GetList();
        result_values = list->GetRange(start, end);
        return {};
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        conn->SendVec(std::move(result_values));
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

// LREM 命令：从列表中删除元素
CoroTask CmdLRem(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    int64_t count = std::stoll(std::string(args[2]));
    auto value = args[3];

    auto cb = [key, count, value](Transaction* tx, EngineShard* es) -> OpResult<size_t> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindMutable(tx->GetDbContext(), key);

        if (it_res.it.GetInnerIt().owner() == nullptr) {
            return 0ULL;
        }

        PrimeValue& prime_value = it_res.it.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_LIST) {
            return OpStatus::WRONG_TYPE;
        }

        ListObject* list = prime_value.GetList();
        return list->Remove(count, std::string(value));
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

// LINSERT 命令：在列表中插入元素
CoroTask CmdLInsert(CommandContext* cmd_cntx, CmdArgList args) {
    auto key = args[1];
    std::string pos(args[2]);
    auto pivot = args[3];
    auto value = args[4];

    auto cb = [key, pos, pivot, value](Transaction* tx, EngineShard* es) -> OpResult<int> {
        auto& db_slice = tx->GetDbSlice(es->shard_id());
        auto it_res = db_slice.FindMutable(tx->GetDbContext(), key);

        if (it_res.it.GetInnerIt().owner() == nullptr) {
            return 0;
        }

        PrimeValue& prime_value = it_res.it.GetInnerIt()->second;
        if (prime_value.ObjType() != OBJ_LIST) {
            return OpStatus::WRONG_TYPE;
        }

        ListObject* list = prime_value.GetList();
        bool inserted = false;
        if (pos == "BEFORE") {
            inserted = list->InsertBefore(std::string(pivot), std::string(value));
        } else if (pos == "AFTER") {
            inserted = list->InsertAfter(std::string(pivot), std::string(value));
        } else {
            return OpStatus::SYNTAX_ERROR;
        }

        return inserted ? static_cast<int>(list->Length()) : -1;
    };

    auto result = co_await cmd::SingleHopT(cb);
    auto conn = cmd_cntx->conn_cntx()->owner();

    if (result.status() == OpStatus::OK) {
        if (result.value() == -1) {
            conn->SendERROR("no such pivot");
        } else {
            conn->Send(static_cast<int64_t>(result.value()));
        }
    } else if (result.status() == OpStatus::SYNTAX_ERROR) {
        conn->SendERROR("syntax error");
    } else {
        conn->SendERROR("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    co_return;
}

// 命令处理函数包装器
void LPush(CommandContext* cmd_cntx, CmdArgList args) {
    CmdLPush(cmd_cntx, args);
}

void RPush(CommandContext* cmd_cntx, CmdArgList args) {
    CmdRPush(cmd_cntx, args);
}

void LPop(CommandContext* cmd_cntx, CmdArgList args) {
    CmdLPop(cmd_cntx, args);
}

void RPop(CommandContext* cmd_cntx, CmdArgList args) {
    CmdRPop(cmd_cntx, args);
}

void LLen(CommandContext* cmd_cntx, CmdArgList args) {
    CmdLLen(cmd_cntx, args);
}

void LIndex(CommandContext* cmd_cntx, CmdArgList args) {
    CmdLIndex(cmd_cntx, args);
}

void LSet(CommandContext* cmd_cntx, CmdArgList args) {
    CmdLSet(cmd_cntx, args);
}

void LRange(CommandContext* cmd_cntx, CmdArgList args) {
    CmdLRange(cmd_cntx, args);
}

void LRem(CommandContext* cmd_cntx, CmdArgList args) {
    CmdLRem(cmd_cntx, args);
}

void LInsert(CommandContext* cmd_cntx, CmdArgList args) {
    CmdLInsert(cmd_cntx, args);
}

}  // namespace

void RegisterListFamily(CommandRegistry* registry) {
    registry->StartFamily();
    *registry
        << CI{"LPUSH", 1, 1, kInvalidKeysOffset}.SetHandler(LPush)
        << CI{"RPUSH", 1, 1, kInvalidKeysOffset}.SetHandler(RPush)
        << CI{"LPOP", 1, 1, kInvalidKeysOffset}.SetHandler(LPop)
        << CI{"RPOP", 1, 1, kInvalidKeysOffset}.SetHandler(RPop)
        << CI{"LLEN", 1, 1, kInvalidKeysOffset}.SetHandler(LLen)
        << CI{"LINDEX", 1, 1, kInvalidKeysOffset}.SetHandler(LIndex)
        << CI{"LSET", 1, 1, kInvalidKeysOffset}.SetHandler(LSet)
        << CI{"LRANGE", 1, 1, kInvalidKeysOffset}.SetHandler(LRange)
        << CI{"LREM", 1, 1, kInvalidKeysOffset}.SetHandler(LRem)
        << CI{"LINSERT", 1, 1, kInvalidKeysOffset}.SetHandler(LInsert)
        ;
}

}  // namespace dfly

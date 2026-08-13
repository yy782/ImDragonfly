#include "cmd_arg_parser.hpp"
#include "cmd_support.hpp"
#include "command_registry.hpp"
#include "detail/conn_context.hpp"
#include "redis/redis_aux.hpp"
#include "sharding/DashTable/compact_obj.hpp"
#include "sharding/db_slice.hpp"
#include "sharding/engine_shard.hpp"
#include "sharding/op_status.hpp"
#include "transaction_layer/transaction.hpp"

namespace dfly {

namespace {

using CI = CommandId;
using cmd::CoroTask;
using Slice = Transaction::Slice;

// 辅助函数：确保键存在且是列表类型
ListObject* GetOrCreateList(Transaction* tx, EngineShard* es,
                            std::string_view key) {
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

  auto cb = [key, values](Transaction* tx,
                          EngineShard* es) -> OpResult<size_t> {
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
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

// RPUSH 命令：将一个或多个值插入到列表尾部
CoroTask CmdRPush(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto values = args.subspan(2);

  auto cb = [key, values](Transaction* tx,
                          EngineShard* es) -> OpResult<size_t> {
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
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
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
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    if (result.value().empty()) {
      rb->BuildError("ERR");
    } else {
      rb->BuildBulkString(result.value());
    }
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    rb->BuildError("ERR");
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
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
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    if (result.value().empty()) {
      rb->BuildError("ERR");
    } else {
      rb->BuildBulkString(result.value());
    }
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    rb->BuildError("ERR");
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
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
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

// LINDEX 命令：获取列表中指定索引的元素
CoroTask CmdLIndex(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  int64_t index = std::stoll(std::string(args[2]));

  auto cb = [key, index](Transaction* tx,
                         EngineShard* es) -> OpResult<std::string> {
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
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    rb->BuildBulkString(result.value());
  } else {
    rb->BuildBulkString(std::string());
  }

  co_return;
}

// LSET 命令：设置列表中指定索引的元素
CoroTask CmdLSet(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  int64_t index = std::stoll(std::string(args[2]));
  auto value = args[3];

  auto cb = [key, index, value](Transaction* tx,
                                EngineShard* es) -> OpResult<void> {
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
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    rb->BuildSimpleString("OK");
  } else if (result.status() == OpStatus::NO_KEY) {
    rb->BuildError("no such key");
  } else if (result.status() == OpStatus::OUT_OF_RANGE) {
    rb->BuildError("index out of range");
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

// LRANGE 命令：获取列表指定范围的元素
CoroTask CmdLRange(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  int64_t start = std::stoll(std::string(args[2]));
  int64_t end = std::stoll(std::string(args[3]));
  std::vector<std::string> result_values;

  auto cb = [key, start, end, &result_values](
                Transaction* tx, EngineShard* es) -> OpResult<void> {
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
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    rb->BuildArray(std::move(result_values));
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

// LREM 命令：从列表中删除元素
CoroTask CmdLRem(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  int64_t count = std::stoll(std::string(args[2]));
  auto value = args[3];

  auto cb = [key, count, value](Transaction* tx,
                                EngineShard* es) -> OpResult<size_t> {
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
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

// LINSERT 命令：在列表中插入元素
CoroTask CmdLInsert(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  std::string pos(args[2]);
  auto pivot = args[3];
  auto value = args[4];

  auto cb = [key, pos, pivot, value](Transaction* tx,
                                     EngineShard* es) -> OpResult<int> {
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
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    if (result.value() == -1) {
      rb->BuildError("no such pivot");
    } else {
      rb->BuildInteger(static_cast<int64_t>(result.value()));
    }
  } else if (result.status() == OpStatus::SYNTAX_ERROR) {
    rb->BuildError("syntax error");
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

}  // namespace

void RegisterListFamily(CommandRegistry* registry) {
  registry->StartFamily();
  *registry << CI{"LPUSH", CO::JOURNALED, 1, 1}.SetHandler(CmdLPush)
            << CI{"RPUSH", CO::JOURNALED, 1, 1}.SetHandler(CmdRPush)
            << CI{"LPOP", CO::JOURNALED, 1, 1}.SetHandler(CmdLPop)
            << CI{"RPOP", CO::JOURNALED, 1, 1}.SetHandler(CmdRPop)
            << CI{"LLEN", CO::READONLY, 1, 1}.SetHandler(CmdLLen)
            << CI{"LINDEX", CO::READONLY, 1, 1}.SetHandler(CmdLIndex)
            << CI{"LSET", CO::JOURNALED, 1, 1}.SetHandler(CmdLSet)
            << CI{"LRANGE", CO::READONLY, 1, 1}.SetHandler(CmdLRange)
            << CI{"LREM", CO::JOURNALED, 1, 1}.SetHandler(CmdLRem)
            << CI{"LINSERT", CO::JOURNALED, 1, 1}.SetHandler(CmdLInsert);
}

}  // namespace dfly

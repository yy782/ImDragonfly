#include "list_family.hpp"

#include "cmd_support.hpp"
#include "command_registry.hpp"
#include "detail/conn_context.hpp"
#include "detail/op_status.hpp"
#include "redis/redis_aux.hpp"
#include "sharding/DashTable/compact_obj.hpp"
#include "sharding/shard.hpp"
#include "transaction_layer/transaction.hpp"
#include "util/arg_parse.hpp"

namespace dfly {

using cmd::CoroTask;

namespace {

using Slice = Transaction::Slice;

using util::ParseInt;

ListObject* GetOrCreateList(Transaction* tx, Shard* shard,
                            std::string_view key) {
  auto& storage = shard->GetShardStorage();
  const DbContext cntx = tx->GetDbContext();
  ListObject* list = nullptr;
  storage.Mutate(cntx, key, [&](PrimeValue* pv) {
    if (pv->IsEmpty()) {
      *pv = CompactValue::MakeList();
    } else if (pv->ObjType() != OBJ_LIST) {
      return;
    }
    list = pv->GetList();
  });
  return list;
}

}  // namespace

CoroTask ListFamily::LPush(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto values = args.subspan(2);

  auto cb = [key, values](Transaction* tx, Shard* shard) -> OpResult<size_t> {
    ListObject* list = GetOrCreateList(tx, shard, key);
    if (!list) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    for (const auto& value : values) {
      list->PushFront(std::string(value));
    }
    return list->Length();
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

CoroTask ListFamily::RPush(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto values = args.subspan(2);

  auto cb = [key, values](Transaction* tx, Shard* shard) -> OpResult<size_t> {
    ListObject* list = GetOrCreateList(tx, shard, key);
    if (!list) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    for (const auto& value : values) {
      list->PushBack(std::string(value));
    }
    return list->Length();
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

CoroTask ListFamily::LPop(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];

  auto cb = [key](Transaction* tx, Shard* shard) -> OpResult<std::string> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto f = storage.Find(cntx, key);
    if (!f) {
      if (f.error() != OpStatus::KEY_NOTFOUND)
        return util::make_unexpected(f.error());
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }
    if (f.value()->second.ObjType() != OBJ_LIST) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    std::string popped;
    auto up = storage.Mutate(cntx, key, [&](PrimeValue* pv) {
      ListObject* list = pv->GetList();
      if (list->Empty()) {
        return;
      }
      popped = list->PopFront();
    });
    if (!up) return util::make_unexpected(up.error());
    return popped;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    if (result.value().empty()) {
      rb->BuildNullBulkString();
    } else {
      rb->BuildBulkString(result.value());
    }
  } else if (result.error() == OpStatus::KEY_NOTFOUND) {
    rb->BuildNullBulkString();
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

CoroTask ListFamily::RPop(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];

  auto cb = [key](Transaction* tx, Shard* shard) -> OpResult<std::string> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto f = storage.Find(cntx, key);
    if (!f) {
      if (f.error() != OpStatus::KEY_NOTFOUND)
        return util::make_unexpected(f.error());
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }
    if (f.value()->second.ObjType() != OBJ_LIST) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    std::string popped;
    auto up = storage.Mutate(cntx, key, [&](PrimeValue* pv) {
      ListObject* list = pv->GetList();
      if (list->Empty()) {
        return;
      }
      popped = list->PopBack();
    });
    if (!up) return util::make_unexpected(up.error());
    return popped;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    if (result.value().empty()) {
      rb->BuildNullBulkString();
    } else {
      rb->BuildBulkString(result.value());
    }
  } else if (result.error() == OpStatus::KEY_NOTFOUND) {
    rb->BuildNullBulkString();
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

CoroTask ListFamily::LLen(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];

  auto cb = [key](Transaction* tx, Shard* shard) -> OpResult<size_t> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return 0ULL;
    }

    const PrimeValue* pv = &res.value()->second;
    if (pv->ObjType() != OBJ_LIST) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    const ListObject* list = pv->GetList();
    return list->Length();
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

CoroTask ListFamily::LIndex(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  int64_t index = 0;
  if (!ParseInt(args[2], index)) {
    cmd_cntx->rb()->BuildError("ERR value is not an integer or out of range");
    co_return;
  }

  auto cb = [key, index](Transaction* tx,
                         Shard* shard) mutable -> OpResult<std::string> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }

    const PrimeValue* pv = &res.value()->second;
    if (pv->ObjType() != OBJ_LIST) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    const ListObject* list = pv->GetList();
    int64_t len = static_cast<int64_t>(list->Length());
    if (index < 0) {
      index = len + index;
    }
    if (index < 0 || index >= len) {
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }
    return list->GetElement(index);
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    rb->BuildBulkString(result.value());
  } else if (result.error() == OpStatus::KEY_NOTFOUND) {
    rb->BuildNullBulkString();
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

CoroTask ListFamily::LRange(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  int64_t start = 0, stop = 0;
  if (!ParseInt(args[2], start) || !ParseInt(args[3], stop)) {
    cmd_cntx->rb()->BuildError("ERR value is not an integer or out of range");
    co_return;
  }

  auto cb = [key, start, stop](
                Transaction* tx,
                Shard* shard) -> OpResult<std::vector<std::string>> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return {};
    }

    const PrimeValue* pv = &res.value()->second;
    if (pv->ObjType() != OBJ_LIST) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    const ListObject* list = pv->GetList();
    return list->GetRange(start, stop);
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    rb->BuildArray(std::move(result.value()));
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

CoroTask ListFamily::LSet(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  int64_t index = 0;
  auto value = args[3];
  if (!ParseInt(args[2], index)) {
    cmd_cntx->rb()->BuildError("ERR value is not an integer or out of range");
    co_return;
  }

  auto cb = [key, index, value](Transaction* tx,
                                Shard* shard) mutable -> OpResult<void> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto f = storage.Find(cntx, key);
    if (!f) {
      if (f.error() != OpStatus::KEY_NOTFOUND)
        return util::make_unexpected(f.error());
      return util::make_unexpected(OpStatus::NO_KEY);
    }
    if (f.value()->second.ObjType() != OBJ_LIST) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    bool ok = false;
    auto up = storage.Mutate(cntx, key, [&](PrimeValue* pv) {
      ListObject* list = pv->GetList();
      int64_t len = static_cast<int64_t>(list->Length());
      if (index < 0) {
        index = len + index;
      }
      if (index < 0 || index >= len) {
        return;
      }
      ok = list->SetElement(index, std::string(value));
    });
    if (!up) return util::make_unexpected(up.error());
    if (!ok) {
      return util::make_unexpected(OpStatus::OUT_OF_RANGE);
    }
    return {};
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    rb->BuildSimpleString("OK");
  } else if (result.error() == OpStatus::NO_KEY) {
    rb->BuildError("ERR no such key");
  } else if (result.error() == OpStatus::OUT_OF_RANGE) {
    rb->BuildError("ERR index out of range");
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

CoroTask ListFamily::LRem(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  int64_t count = 0;
  auto value = args[3];
  if (!ParseInt(args[2], count)) {
    cmd_cntx->rb()->BuildError("ERR value is not an integer or out of range");
    co_return;
  }

  auto cb = [key, count, value](Transaction* tx,
                                Shard* shard) -> OpResult<size_t> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto f = storage.Find(cntx, key);
    if (!f) {
      if (f.error() != OpStatus::KEY_NOTFOUND)
        return util::make_unexpected(f.error());
      return 0ULL;
    }
    if (f.value()->second.ObjType() != OBJ_LIST) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    size_t removed = 0;
    auto up = storage.Mutate(cntx, key, [&](PrimeValue* pv) {
      ListObject* list = pv->GetList();
      removed = list->Remove(count, std::string(value));
    });
    if (!up) return util::make_unexpected(up.error());
    return removed;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

CoroTask ListFamily::LInsert(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto pos = args[2];
  auto pivot = args[3];
  auto value = args[4];

  auto cb = [key, pos, pivot, value](Transaction* tx,
                                     Shard* shard) -> OpResult<int> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto f = storage.Find(cntx, key);
    if (!f) {
      if (f.error() != OpStatus::KEY_NOTFOUND)
        return util::make_unexpected(f.error());
      return 0;
    }
    if (f.value()->second.ObjType() != OBJ_LIST) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    OpStatus err = OpStatus::OK;
    int len = -1;
    auto up = storage.Mutate(cntx, key, [&](PrimeValue* pv) {
      ListObject* list = pv->GetList();
      if (pos == "BEFORE") {
        if (list->InsertBefore(std::string(pivot), std::string(value))) {
          len = static_cast<int>(list->Length());
        }
      } else if (pos == "AFTER") {
        if (list->InsertAfter(std::string(pivot), std::string(value))) {
          len = static_cast<int>(list->Length());
        }
      } else {
        err = OpStatus::SYNTAX_ERROR;
      }
    });
    if (err != OpStatus::OK) return util::make_unexpected(err);
    if (!up) return util::make_unexpected(up.error());
    return len;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else if (result.error() == OpStatus::SYNTAX_ERROR) {
    rb->BuildError("ERR syntax error");
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

namespace {
constexpr CommandSpec kCommands[] = {
    {"LPUSH", CO::JOURNALED, 1, 1, &ListFamily::LPush},
    {"RPUSH", CO::JOURNALED, 1, 1, &ListFamily::RPush},
    {"LPOP", CO::JOURNALED, 1, 1, &ListFamily::LPop},
    {"RPOP", CO::JOURNALED, 1, 1, &ListFamily::RPop},
    {"LLEN", CO::READONLY, 1, 1, &ListFamily::LLen},
    {"LINDEX", CO::READONLY, 1, 1, &ListFamily::LIndex},
    {"LRANGE", CO::READONLY, 1, 1, &ListFamily::LRange},
    {"LSET", CO::JOURNALED, 1, 1, &ListFamily::LSet},
    {"LREM", CO::JOURNALED, 1, 1, &ListFamily::LRem},
    {"LINSERT", CO::JOURNALED, 1, 1, &ListFamily::LInsert},
};
static_assert(CheckUniqueNames(kCommands), "list family: duplicate names");

}  // namespace

void RegisterListFamily(CommandRegistry* registry) {
  registry->Register(kCommands);
}

}  // namespace dfly

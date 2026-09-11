#include "hash_family.hpp"

#include "cmd_support.hpp"
#include "command_registry.hpp"
#include "detail/conn_context.hpp"
#include "detail/op_status.hpp"
#include "redis/redis_aux.hpp"
#include "sharding/DashTable/compact_obj.hpp"
#include "sharding/shard.hpp"
#include "transaction_layer/transaction.hpp"

namespace dfly {

using cmd::CoroTask;

namespace {

using Slice = Transaction::Slice;

HashObject* GetOrCreateHash(Transaction* tx, Shard* shard,
                            std::string_view key) {
  auto& storage = shard->GetShardStorage();
  const DbContext cntx = tx->GetDbContext();
  HashObject* hash = nullptr;
  storage.Mutate(cntx, key, [&](PrimeValue* pv) {
    if (pv->IsEmpty()) {
      *pv = CompactValue::MakeHash();
    } else if (pv->ObjType() != OBJ_HASH) {
      return;
    }
    hash = pv->GetHash();
  });
  return hash;
}

}  // namespace

CoroTask HashFamily::HSet(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto field = args[2];
  auto value = args[3];

  auto cb = [key, field, value](Transaction* tx, Shard* shard,
                                OpStatus sched) -> OpResult<int> {
    if (sched != OpStatus::OK) return util::make_unexpected(sched);
    HashObject* hash = GetOrCreateHash(tx, shard, key);
    if (!hash) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    bool existed = hash->Exists(std::string(field));
    hash->Set(std::string(field), std::string(value));
    return existed ? 0 : 1;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else if (result.error() == OpStatus::RAFT_SCHED_FAIL) {
    rb->BuildError("not leader");
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

CoroTask HashFamily::HGet(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto field = args[2];

  auto cb = [key, field](Transaction* tx, Shard* shard,
                         OpStatus /*sched*/) -> OpResult<std::string> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }

    const PrimeValue* pv = &res.value()->second;
    if (pv->ObjType() != OBJ_HASH) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    const HashObject* hash = pv->GetHash();
    std::string val = hash->Get(std::string(field));
    if (val.empty()) {
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }
    return val;
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

CoroTask HashFamily::HDel(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto fields = args.subspan(2);

  auto cb = [key, fields](Transaction* tx, Shard* shard,
                          OpStatus sched) -> OpResult<size_t> {
    if (sched != OpStatus::OK) return util::make_unexpected(sched);
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto f = storage.Find(cntx, key);
    if (!f) {
      if (f.error() != OpStatus::KEY_NOTFOUND)
        return util::make_unexpected(f.error());
      return 0ULL;
    }
    if (f.value()->second.ObjType() != OBJ_HASH) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    size_t deleted = 0;
    auto up = storage.Mutate(cntx, key, [&](PrimeValue* pv) {
      HashObject* hash = pv->GetHash();
      for (const auto& field : fields) {
        deleted += hash->Del(std::string(field));
      }
    });
    if (!up) return util::make_unexpected(up.error());
    return deleted;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else if (result.error() == OpStatus::RAFT_SCHED_FAIL) {
    rb->BuildError("not leader");
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

CoroTask HashFamily::HExists(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto field = args[2];

  auto cb = [key, field](Transaction* tx, Shard* shard,
                         OpStatus /*sched*/) -> OpResult<int> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return 0;
    }

    const PrimeValue* pv = &res.value()->second;
    if (pv->ObjType() != OBJ_HASH) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    const HashObject* hash = pv->GetHash();
    return hash->Exists(std::string(field)) ? 1 : 0;
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

CoroTask HashFamily::HLen(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];

  auto cb = [key](Transaction* tx, Shard* shard,
                  OpStatus /*sched*/) -> OpResult<size_t> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return 0ULL;
    }

    const PrimeValue* pv = &res.value()->second;
    if (pv->ObjType() != OBJ_HASH) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    const HashObject* hash = pv->GetHash();
    return hash->Length();
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

namespace {
constexpr CommandSpec kCommands[] = {
    {"HSET", CO::JOURNALED, 1, 1, &HashFamily::HSet},
    {"HGET", CO::READONLY, 1, 1, &HashFamily::HGet},
    {"HDEL", CO::JOURNALED, 1, 1, &HashFamily::HDel},
    {"HEXISTS", CO::READONLY, 1, 1, &HashFamily::HExists},
    {"HLEN", CO::READONLY, 1, 1, &HashFamily::HLen},
};
static_assert(CheckUniqueNames(kCommands), "hash family: duplicate names");

}  // namespace

void RegisterHashFamily(CommandRegistry* registry) {
  registry->Register(kCommands);
}

}  // namespace dfly

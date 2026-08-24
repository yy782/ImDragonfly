#include "hash_family.hpp"

#include "cmd_arg_parser.hpp"
#include "cmd_support.hpp"
#include "command_registry.hpp"
#include "detail/conn_context.hpp"
#include "redis/redis_aux.hpp"
#include "sharding/DashTable/compact_obj.hpp"
#include "sharding/shard.hpp"
#include "sharding/op_status.hpp"
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
      return;  // 类型不符 → hash 保持 nullptr → WRONG_TYPE
    }
    hash = pv->GetHash();
  });
  return hash;
}

// HSET 命令：设置哈希表中的字段值
CoroTask HashFamily::HSet(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto field = args[2];
  auto value = args[3];

  auto cb = [key, field, value](Transaction* tx,
                                Shard* shard) -> OpResult<int> {
    HashObject* hash = GetOrCreateHash(tx, shard, key);
    if (!hash) {
      return OpStatus::WRONG_TYPE;
    }

    bool existed = hash->Exists(std::string(field));
    hash->Set(std::string(field), std::string(value));
    return existed ? 0 : 1;
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

// HGET 命令：获取哈希表中字段的值
CoroTask HashFamily::HGet(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto field = args[2];

  auto cb = [key, field](Transaction* tx,
                         Shard* shard) -> OpResult<std::string> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return OpStatus::KEY_NOTFOUND;
    }

    const PrimeValue* pv = res->value;
    if (pv->ObjType() != OBJ_HASH) {
      return OpStatus::WRONG_TYPE;
    }

    const HashObject* hash = pv->GetHash();
    std::string val = hash->Get(std::string(field));
    if (val.empty()) {
      return OpStatus::KEY_NOTFOUND;
    }
    return val;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    rb->BuildBulkString(result.value());
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    rb->BuildNullBulkString();
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

// HDEL 命令：删除哈希表中的一个或多个字段
CoroTask HashFamily::HDel(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto fields = args.subspan(2);

  auto cb = [key, fields](Transaction* tx, Shard* shard) -> OpResult<size_t> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto f = storage.Find(cntx, key);
    if (!f) {
      if (f.error() != OpStatus::KEY_NOTFOUND)
        return util::make_unexpected(f.error());
      return 0ULL;  // 不存在 → 0
    }
    if (f->obj_type() != OBJ_HASH) {
      return OpStatus::WRONG_TYPE;
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

  if (result.status() == OpStatus::OK) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

// HEXISTS 命令：检查哈希表中是否存在指定字段
CoroTask HashFamily::HExists(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto field = args[2];

  auto cb = [key, field](Transaction* tx, Shard* shard) -> OpResult<int> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return 0;  // 不存在 → 0
    }

    const PrimeValue* pv = res->value;
    if (pv->ObjType() != OBJ_HASH) {
      return OpStatus::WRONG_TYPE;
    }

    const HashObject* hash = pv->GetHash();
    return hash->Exists(std::string(field)) ? 1 : 0;
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

// HLEN 命令：返回哈希表中字段的数量
CoroTask HashFamily::HLen(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];

  auto cb = [key](Transaction* tx, Shard* shard) -> OpResult<size_t> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return 0ULL;  // 不存在 → 0
    }

    const PrimeValue* pv = res->value;
    if (pv->ObjType() != OBJ_HASH) {
      return OpStatus::WRONG_TYPE;
    }

    const HashObject* hash = pv->GetHash();
    return hash->Length();
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

// 命令目录：表驱动注册，constexpr 声明 + 编译期查重。
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

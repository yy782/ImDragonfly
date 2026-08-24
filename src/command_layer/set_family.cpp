#include "set_family.hpp"

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

SetObject* GetOrCreateSet(Transaction* tx, Shard* shard,
                          std::string_view key) {
  auto& storage = shard->GetShardStorage();
  const DbContext cntx = tx->GetDbContext();
  SetObject* set = nullptr;
  storage.Mutate(cntx, key, [&](PrimeValue* pv) {
    if (pv->IsEmpty()) {
      *pv = CompactValue::MakeSet();
    } else if (pv->ObjType() != OBJ_SET) {
      return;  // 类型不符 → set 保持 nullptr → WRONG_TYPE
    }
    set = pv->GetSet();
  });
  return set;
}

// SADD 命令：向集合添加一个或多个成员
CoroTask SetFamily::SAdd(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto members = args.subspan(2);

  auto cb = [key, members](Transaction* tx, Shard* shard) -> OpResult<size_t> {
    SetObject* set = GetOrCreateSet(tx, shard, key);
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
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

// SREM 命令：从集合中移除一个或多个成员
CoroTask SetFamily::SRem(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto members = args.subspan(2);

  auto cb = [key, members](Transaction* tx, Shard* shard) -> OpResult<size_t> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto f = storage.Find(cntx, key);
    if (!f) {
      if (f.error() != OpStatus::KEY_NOTFOUND)
        return util::make_unexpected(f.error());
      return 0ULL;  // 不存在 → 0
    }
    if (f->obj_type() != OBJ_SET) {
      return OpStatus::WRONG_TYPE;
    }

    size_t removed = 0;
    auto up = storage.Mutate(cntx, key, [&](PrimeValue* pv) {
      SetObject* set = pv->GetSet();
      for (const auto& member : members) {
        removed += set->Remove(std::string(member));
      }
    });
    if (!up) return util::make_unexpected(up.error());
    return removed;
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

// SMEMBERS 命令：返回集合中的所有成员
CoroTask SetFamily::SMembers(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  std::vector<std::string> members;

  auto cb = [key, &members](Transaction* tx, Shard* shard) -> OpResult<void> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return {};  // 不存在 → 空数组
    }

    const PrimeValue* pv = res->value;
    if (pv->ObjType() != OBJ_SET) {
      return OpStatus::WRONG_TYPE;
    }

    const SetObject* set = pv->GetSet();
    for (const auto& member : set->Data()) {
      members.push_back(member);
    }
    return {};
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    rb->BuildArray(std::move(members));
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

// SCARD 命令：返回集合的大小
CoroTask SetFamily::SCard(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];

  auto cb = [key](Transaction* tx, Shard* shard) -> OpResult<size_t> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return 0ULL;  // 不存在 → 0
    }

    const PrimeValue* pv = res->value;
    if (pv->ObjType() != OBJ_SET) {
      return OpStatus::WRONG_TYPE;
    }

    const SetObject* set = pv->GetSet();
    return set->Length();
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

// SISMEMBER 命令：检查成员是否在集合中
CoroTask SetFamily::SIsMember(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto member = args[2];

  auto cb = [key, member](Transaction* tx, Shard* shard) -> OpResult<int> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return 0;  // 不存在 → 0
    }

    const PrimeValue* pv = res->value;
    if (pv->ObjType() != OBJ_SET) {
      return OpStatus::WRONG_TYPE;
    }

    const SetObject* set = pv->GetSet();
    return set->Contains(std::string(member)) ? 1 : 0;
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
    {"SADD", CO::JOURNALED, 1, 1, &SetFamily::SAdd},
    {"SREM", CO::JOURNALED, 1, 1, &SetFamily::SRem},
    {"SMEMBERS", CO::READONLY, 1, 1, &SetFamily::SMembers},
    {"SCARD", CO::READONLY, 1, 1, &SetFamily::SCard},
    {"SISMEMBER", CO::READONLY, 1, 1, &SetFamily::SIsMember},
};
static_assert(CheckUniqueNames(kCommands), "set family: duplicate names");

}  // namespace

void RegisterSetFamily(CommandRegistry* registry) {
  registry->Register(kCommands);
}

}  // namespace dfly

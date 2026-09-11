#include "zset_family.hpp"

#include <vector>

#include "cmd_support.hpp"
#include "command_registry.hpp"
#include "detail/conn_context.hpp"
#include "detail/op_status.hpp"
#include "redis/redis_aux.hpp"
#include "sharding/DashTable/compact_obj.hpp"
#include "sharding/shard.hpp"
#include "transaction_layer/transaction.hpp"
#include "util/Strings.hpp"
#include "util/arg_parse.hpp"

namespace dfly {

using cmd::CoroTask;

namespace {
using Slice = Transaction::Slice;

using util::ParseDouble;
using util::ParseInt;

ZSetObject* GetOrCreateZSet(Transaction* tx, Shard* shard,
                            std::string_view key) {
  auto& storage = shard->GetShardStorage();
  const DbContext cntx = tx->GetDbContext();
  ZSetObject* zset = nullptr;
  storage.Mutate(cntx, key, [&](PrimeValue* pv) {
    if (pv->IsEmpty()) {
      *pv = CompactValue::MakeZSet();
    } else if (pv->ObjType() != OBJ_ZSET) {
      return;
    }
    zset = pv->GetZSet();
  });
  return zset;
}

}  // namespace

CoroTask ZSetFamily::ZAdd(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];

  std::vector<std::pair<double, std::string>> pairs;
  pairs.reserve((args.size() - 2) / 2);
  for (size_t i = 2; i + 1 < args.size(); i += 2) {
    double score = 0.0;
    if (!ParseDouble(args[i], score)) {
      cmd_cntx->rb()->BuildError("ERR value is not a valid float");
      co_return;
    }
    pairs.emplace_back(score, std::string(args[i + 1]));
  }

  auto cb = [key, pairs](Transaction* tx, Shard* shard,
                         OpStatus sched) -> OpResult<size_t> {
    if (sched != OpStatus::OK) return util::make_unexpected(sched);
    ZSetObject* zset = GetOrCreateZSet(tx, shard, key);
    if (!zset) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    size_t added = 0;
    for (const auto& [score, member] : pairs) {
      added += zset->Add(member, score);
    }
    return added;
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

CoroTask ZSetFamily::ZCard(CommandContext* cmd_cntx, CmdArgList args) {
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
    if (pv->ObjType() != OBJ_ZSET) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    const ZSetObject* zset = pv->GetZSet();
    return zset->Length();
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

CoroTask ZSetFamily::ZScore(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto member = args[2];

  auto cb = [key, member](Transaction* tx, Shard* shard,
                          OpStatus /*sched*/) -> OpResult<double> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }

    const PrimeValue* pv = &res.value()->second;
    if (pv->ObjType() != OBJ_ZSET) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    const ZSetObject* zset = pv->GetZSet();
    auto score = zset->Score(std::string(member));
    if (!score) {
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }
    return *score;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    rb->BuildDouble(result.value());
  } else if (result.error() == OpStatus::KEY_NOTFOUND) {
    rb->BuildNullBulkString();
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

CoroTask ZSetFamily::ZRem(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto members = args.subspan(2);

  auto cb = [key, members](Transaction* tx, Shard* shard,
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
    if (f.value()->second.ObjType() != OBJ_ZSET) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    size_t removed = 0;
    auto up = storage.Mutate(cntx, key, [&](PrimeValue* pv) {
      ZSetObject* zset = pv->GetZSet();
      for (const auto& member : members) {
        if (zset->Remove(std::string(member))) {
          ++removed;
        }
      }
    });
    if (!up) return util::make_unexpected(up.error());
    return removed;
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

CoroTask ZSetFamily::ZRank(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto member = args[2];

  auto cb = [key, member](Transaction* tx, Shard* shard,
                          OpStatus /*sched*/) -> OpResult<size_t> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }

    const PrimeValue* pv = &res.value()->second;
    if (pv->ObjType() != OBJ_ZSET) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    const ZSetObject* zset = pv->GetZSet();
    int64_t rank = zset->Rank(std::string(member));
    if (rank < 0) {
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }
    return rank;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else if (result.error() == OpStatus::KEY_NOTFOUND) {
    rb->BuildNullBulkString();
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

CoroTask ZSetFamily::ZRevRank(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto member = args[2];

  auto cb = [key, member](Transaction* tx, Shard* shard,
                          OpStatus /*sched*/) -> OpResult<size_t> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }

    const PrimeValue* pv = &res.value()->second;
    if (pv->ObjType() != OBJ_ZSET) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    const ZSetObject* zset = pv->GetZSet();
    int64_t rank = zset->RevRank(std::string(member));
    if (rank < 0) {
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }
    return rank;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else if (result.error() == OpStatus::KEY_NOTFOUND) {
    rb->BuildNullBulkString();
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

CoroTask ZSetFamily::ZRange(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  int64_t start = 0, stop = 0;
  bool with_scores =
      args.size() == 5 &&
      util::EqualsIgnoreCaseStd(args[4], std::string_view("WITHSCORES"));

  if (!ParseInt(args[2], start) || !ParseInt(args[3], stop)) {
    cmd_cntx->rb()->BuildError("ERR value is not an integer or out of range");
    co_return;
  }

  auto cb = [key, start, stop](Transaction* tx, Shard* shard,
                               OpStatus /*sched*/)
      -> OpResult<std::vector<std::pair<std::string, double>>> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return {};
    }

    const PrimeValue* pv = &res.value()->second;
    if (pv->ObjType() != OBJ_ZSET) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    const ZSetObject* zset = pv->GetZSet();

    return zset->Range(start, stop);
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    rb->StartArray(result.value().size());
    for (const auto& [member, score] : result.value()) {
      rb->BuildBulkString(member);
      if (with_scores) {
        rb->BuildDouble(score);
      }
    }
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

CoroTask ZSetFamily::ZRevRange(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  int64_t start = 0, stop = 0;
  bool with_scores =
      args.size() == 5 &&
      util::EqualsIgnoreCaseStd(args[4], std::string_view("WITHSCORES"));

  if (!ParseInt(args[2], start) || !ParseInt(args[3], stop)) {
    cmd_cntx->rb()->BuildError("ERR value is not an integer or out of range");
    co_return;
  }

  auto cb = [key, start, stop](Transaction* tx, Shard* shard,
                               OpStatus /*sched*/)
      -> OpResult<std::vector<std::pair<std::string, double>>> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return {};
    }

    const PrimeValue* pv = &res.value()->second;
    if (pv->ObjType() != OBJ_ZSET) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    const ZSetObject* zset = pv->GetZSet();

    return zset->RevRange(start, stop);
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.has_value()) {
    rb->StartArray(result.value().size());
    for (const auto& [member, score] : result.value()) {
      rb->BuildBulkString(member);
      if (with_scores) {
        rb->BuildDouble(score);
      }
    }
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

namespace {
constexpr CommandSpec kCommands[] = {
    {"ZADD", CO::JOURNALED, 1, 1, &ZSetFamily::ZAdd},
    {"ZCARD", CO::READONLY, 1, 1, &ZSetFamily::ZCard},
    {"ZSCORE", CO::READONLY, 1, 1, &ZSetFamily::ZScore},
    {"ZREM", CO::JOURNALED, 1, 1, &ZSetFamily::ZRem},
    {"ZRANK", CO::READONLY, 1, 1, &ZSetFamily::ZRank},
    {"ZREVRANK", CO::READONLY, 1, 1, &ZSetFamily::ZRevRank},
    {"ZRANGE", CO::READONLY, 1, 1, &ZSetFamily::ZRange},
    {"ZREVRANGE", CO::READONLY, 1, 1, &ZSetFamily::ZRevRange},
};
static_assert(CheckUniqueNames(kCommands), "zset family: duplicate names");

}  // namespace

void RegisterZSetFamily(CommandRegistry* registry) {
  registry->Register(kCommands);
}

}  // namespace dfly

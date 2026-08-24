#include "zset_family.hpp"

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

ZSetObject* GetOrCreateZSet(Transaction* tx, Shard* shard,
                            std::string_view key) {
  auto& storage = shard->GetShardStorage();
  const DbContext cntx = tx->GetDbContext();
  ZSetObject* zset = nullptr;
  storage.Mutate(cntx, key, [&](PrimeValue* pv) {
    if (pv->IsEmpty()) {
      *pv = CompactValue::MakeZSet();
    } else if (pv->ObjType() != OBJ_ZSET) {
      return;  // 类型不符 → zset 保持 nullptr → WRONG_TYPE
    }
    zset = pv->GetZSet();
  });
  return zset;
}

// ZADD 命令：向有序集合添加一个或多个成员，或更新其分数
CoroTask ZSetFamily::ZAdd(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto score_str = args[2];
  auto member = args[3];

  double score = 0.0;
  if (!absl::SimpleAtod(std::string(score_str), &score)) {
    cmd_cntx->rb()->BuildError("ERR value is not a valid float");
    co_return;
  }

  auto cb = [key, score, member](Transaction* tx,
                                 Shard* shard) -> OpResult<size_t> {
    ZSetObject* zset = GetOrCreateZSet(tx, shard, key);
    if (!zset) {
      return OpStatus::WRONG_TYPE;
    }

    size_t added = zset->Add(std::string(member), score);
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

// ZCARD 命令：返回有序集合的大小
CoroTask ZSetFamily::ZCard(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];

  auto cb = [key](Transaction* tx, Shard* shard) -> OpResult<size_t> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return 0ULL;  // 不存在 → 0
    }

    const PrimeValue* pv = res->value;
    if (pv->ObjType() != OBJ_ZSET) {
      return OpStatus::WRONG_TYPE;
    }

    const ZSetObject* zset = pv->GetZSet();
    return zset->Length();
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

// ZSCORE 命令：返回有序集合中成员的分数
CoroTask ZSetFamily::ZScore(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto member = args[2];

  auto cb = [key, member](Transaction* tx,
                          Shard* shard) -> OpResult<double> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return OpStatus::KEY_NOTFOUND;
    }

    const PrimeValue* pv = res->value;
    if (pv->ObjType() != OBJ_ZSET) {
      return OpStatus::WRONG_TYPE;
    }

    const ZSetObject* zset = pv->GetZSet();
    auto score = zset->Score(std::string(member));
    if (!score) {
      return OpStatus::KEY_NOTFOUND;  // 成员不存在
    }
    return *score;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    rb->BuildDouble(result.value());
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    rb->BuildNullBulkString();
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

// ZREM 命令：从有序集合中移除一个或多个成员
CoroTask ZSetFamily::ZRem(CommandContext* cmd_cntx, CmdArgList args) {
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
    if (f->obj_type() != OBJ_ZSET) {
      return OpStatus::WRONG_TYPE;
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

  if (result.status() == OpStatus::OK) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

// ZRANK 命令：返回有序集合中成员的排名（按分数升序，从 0 开始）
CoroTask ZSetFamily::ZRank(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto member = args[2];

  auto cb = [key, member](Transaction* tx, Shard* shard) -> OpResult<size_t> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return OpStatus::KEY_NOTFOUND;
    }

    const PrimeValue* pv = res->value;
    if (pv->ObjType() != OBJ_ZSET) {
      return OpStatus::WRONG_TYPE;
    }

    const ZSetObject* zset = pv->GetZSet();
    int64_t rank = zset->Rank(std::string(member));
    if (rank < 0) {
      return OpStatus::KEY_NOTFOUND;  // 成员不存在
    }
    return rank;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    rb->BuildNullBulkString();
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

// ZREVRANK 命令：返回有序集合中成员的排名（按分数降序，从 0 开始）
CoroTask ZSetFamily::ZRevRank(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  auto member = args[2];

  auto cb = [key, member](Transaction* tx, Shard* shard) -> OpResult<size_t> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return OpStatus::KEY_NOTFOUND;
    }

    const PrimeValue* pv = res->value;
    if (pv->ObjType() != OBJ_ZSET) {
      return OpStatus::WRONG_TYPE;
    }

    const ZSetObject* zset = pv->GetZSet();
    int64_t rank = zset->RevRank(std::string(member));
    if (rank < 0) {
      return OpStatus::KEY_NOTFOUND;  // 成员不存在
    }
    return rank;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    rb->BuildInteger(static_cast<int64_t>(result.value()));
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    rb->BuildNullBulkString();
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

// ZRANGE 命令：按排名范围返回有序集合的成员（升序）
CoroTask CmdZRange(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  int64_t start = 0, stop = 0;
  bool with_scores = args.size() == 4 &&
                     absl::EqualsIgnoreCase(args[3], "WITHSCORES");

  if (!absl::SimpleAtoi(std::string(args[2]), &start) ||
      !absl::SimpleAtoi(std::string(args[3]), &stop)) {
    cmd_cntx->rb()->BuildError("ERR value is not an integer or out of range");
    co_return;
  }

  auto cb =
      [key, start, stop](Transaction* tx,
                         Shard* shard)
          -> OpResult<std::vector<std::pair<std::string, double>>> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return {};  // 不存在 → 空数组
    }

    const PrimeValue* pv = res->value;
    if (pv->ObjType() != OBJ_ZSET) {
      return OpStatus::WRONG_TYPE;
    }

    const ZSetObject* zset = pv->GetZSet();
    // Range 内部处理负索引与越界
    return zset->Range(start, stop);
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    rb->StartArray(result.value().size());
    for (const auto& [member, score] : result.value()) {
      rb->SendBulkString(member);
      if (with_scores) {
        rb->SendDouble(score);
      }
    }
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

// ZREVRANGE 命令：按排名范围返回有序集合的成员（降序）
CoroTask ZSetFamily::ZRevRange(CommandContext* cmd_cntx, CmdArgList args) {
  auto key = args[1];
  int64_t start = 0, stop = 0;
  bool with_scores = args.size() == 4 &&
                     absl::EqualsIgnoreCase(args[3], "WITHSCORES");

  if (!absl::SimpleAtoi(std::string(args[2]), &start) ||
      !absl::SimpleAtoi(std::string(args[3]), &stop)) {
    cmd_cntx->rb()->BuildError("ERR value is not an integer or out of range");
    co_return;
  }

  auto cb =
      [key, start, stop](Transaction* tx,
                         Shard* shard)
          -> OpResult<std::vector<std::pair<std::string, double>>> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto res = storage.Find(cntx, key);
    if (!res) {
      return {};  // 不存在 → 空数组
    }

    const PrimeValue* pv = res->value;
    if (pv->ObjType() != OBJ_ZSET) {
      return OpStatus::WRONG_TYPE;
    }

    const ZSetObject* zset = pv->GetZSet();
    // RevRange 内部处理负索引与越界，返回降序
    return zset->RevRange(start, stop);
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();

  if (result.status() == OpStatus::OK) {
    rb->StartArray(result.value().size());
    for (const auto& [member, score] : result.value()) {
      rb->SendBulkString(member);
      if (with_scores) {
        rb->SendDouble(score);
      }
    }
  } else {
    rb->BuildError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }

  co_return;
}

// 命令目录：表驱动注册，constexpr 声明 + 编译期查重。
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

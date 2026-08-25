#include "generic_family.hpp"

#include <glog/logging.h>

#include <atomic>
#include <optional>

#include "cmd_support.hpp"
#include "command_registry.hpp"
#include "detail/common_types.hpp"
#include "detail/op_status.hpp"
#include "server/redis_server.hpp"

namespace dfly {
using namespace dfly::cmd;

CoroTask CmdDel(CommandContext* cmd_cntx, CmdArgList args) {
  (void)args;

  std::atomic<uint32_t> result = 0;
  auto cb = [&](Transaction* tx, Shard* es) -> OpResult<void> {
    ShardStorage& db_slice = es->GetShardStorage();
    const auto& cntx = tx->GetDbContext();
    uint32_t res = 0;
    const auto& slice = tx->GetSlice(es->shard_id());
    for (const auto& [key, keyId] : slice) {
      auto del_res = db_slice.Delete(cntx, key);
      if (del_res.has_value() && del_res.value()) {
        ++res;
      }
    }
    result.fetch_add(res, std::memory_order_relaxed);
    return {};
  };

  OpResult<void> res = co_await cmd::SingleHopT(cb);
  uint32_t del_cnt = result.load(std::memory_order_relaxed);

  auto* rb = cmd_cntx->rb();
  if (res.has_value())
    rb->BuildInteger(del_cnt);
  else
    rb->BuildError("ERR");
  co_return;
}

CoroTask GenericFamily::Delex(CommandContext* cmd_cntx, CmdArgList args) {
  DCHECK(!args.empty());
  return CmdDel(cmd_cntx, args);
}

CoroTask GenericFamily::Ping(CommandContext* cmd_cntx, CmdArgList args) {
  auto* rb = cmd_cntx->rb();
  if (args.size() > 1) {
    rb->BuildError("ERR");
    co_return;
  }
  rb->BuildSimpleString("PONG");
  co_return;
}

CoroTask CmdExists(CommandContext* cmd_cntx, CmdArgList args) {
  (void)args;

  auto Op = [](Transaction* tx, ShardStorage& db_slice) -> OpResult<uint32_t> {
    const auto& slice = tx->GetSlice(db_slice.shard_id());
    uint32_t res = 0;
    for (const auto& [key, keyId] : slice) {
      auto find_res = db_slice.Find(tx->GetDbContext(), key);
      res += find_res.has_value();
    }
    return {res};
  };

  std::atomic<uint32_t> result{0};

  auto cb = [&result, &Op](Transaction* t, Shard* es) -> OpResult<void> {
    auto res = Op(t, es->GetShardStorage());
    result.fetch_add(res.has_value() ? res.value() : 0,
                     std::memory_order_relaxed);
    return {};
  };

  OpResult<void> res = co_await cmd::SingleHopT(cb);

  auto* rb = cmd_cntx->rb();
  if (res.has_value()) {
    rb->BuildInteger(result.load());
  } else {
    rb->BuildInteger(0);
  }

  co_return;
}

CoroTask GenericFamily::Exists(CommandContext* cmd_cntx, CmdArgList args) {
  return CmdExists(cmd_cntx, args);
}

CoroTask CmdExpire(CommandContext* cmd_cntx, std::string_view key,
                   int64_t sec) {
  auto cb = [&](Transaction* t, Shard* es) -> OpResult<void> {
    auto& db_slice = es->GetShardStorage();
    const auto& cntx = t->GetDbContext();
    auto ttl_at = cntx.GetTimeNowMs() + static_cast<uint64_t>(sec) * 1000;
    return db_slice.SetTtl(cntx, key, ttl_at);
  };
  auto res = co_await cmd::SingleHopT(cb);

  auto* rb = cmd_cntx->rb();
  if (res.has_value()) {
    rb->BuildInteger(1);
  } else {
    rb->BuildInteger(0);
  }

  co_return;
}

CoroTask GenericFamily::Expire(CommandContext* cmd_cntx, CmdArgList args) {
  std::string_view key = args[1];
  std::string_view sec = args[2];
  int64_t int_arg = std::atoi(sec.data());
  return CmdExpire(cmd_cntx, key, int_arg);
}

// void GenericFamily::Keys(CmdArgList args, CommandContext* cmd_cntx) {
//     // TODO
// }

CoroTask CmdExpireTime(CommandContext* cmd_cntx, std::string_view key) {
  auto cb = [&](Transaction* t, Shard* es) -> OpResult<int64_t> {
    auto& db_slice = es->GetShardStorage();
    const auto& cntx = t->GetDbContext();
    auto et = db_slice.ExpireTime(cntx, key);
    if (!et.has_value()) {
      if (et.error() == OpStatus::KEY_NOTFOUND) {
        return util::make_unexpected(OpStatus::KEY_NOTFOUND);
      }
      return util::make_unexpected(OpStatus::SKIPPED);
    }

    return {static_cast<int64_t>(et.value() / 1000)};
  };

  OpResult<int64_t> res = co_await cmd::SingleHopT(cb);

  auto* rb = cmd_cntx->rb();
  if (res.has_value()) {
    rb->BuildInteger(res.value());
  } else {
    if (res.error() == OpStatus::KEY_NOTFOUND) {
      rb->BuildInteger(-2);
    } else if (res.error() == OpStatus::SKIPPED) {
      rb->BuildInteger(-1);
    } else {
      rb->BuildError("ERR");
    }
  }
  co_return;
}

CoroTask GenericFamily::ExpireTime(CommandContext* cmd_cntx, CmdArgList args) {
  return CmdExpireTime(cmd_cntx, args[1]);
}

CoroTask CmdTtl(CommandContext* cmd_cntx, std::string_view key) {
  auto cb = [&](Transaction* t, Shard* es) -> OpResult<int64_t> {
    auto& db_slice = es->GetShardStorage();
    const auto& cntx = t->GetDbContext();
    auto et = db_slice.ExpireTime(cntx, key);
    if (!et.has_value()) {
      if (et.error() == OpStatus::KEY_NOTFOUND) {
        return util::make_unexpected(OpStatus::KEY_NOTFOUND);
      }
      return util::make_unexpected(OpStatus::SKIPPED);
    }

    int64_t remain_ms = static_cast<int64_t>(et.value()) -
                        static_cast<int64_t>(cntx.GetTimeNowMs());
    if (remain_ms <= 0) {
      return util::make_unexpected(OpStatus::SKIPPED);
    }
    return {remain_ms / 1000};
  };

  OpResult<int64_t> res = co_await cmd::SingleHopT(cb);

  auto* rb = cmd_cntx->rb();
  if (res.has_value()) {
    rb->BuildInteger(res.value());
  } else {
    if (res.error() == OpStatus::KEY_NOTFOUND) {
      rb->BuildInteger(-2);
    } else if (res.error() == OpStatus::SKIPPED) {
      rb->BuildInteger(-1);
    } else {
      rb->BuildError("ERR");
    }
  }
  co_return;
}

CoroTask GenericFamily::Ttl(CommandContext* cmd_cntx, CmdArgList args) {
  return CmdTtl(cmd_cntx, args[1]);
}

CoroTask GenericFamily::Client_Info(CommandContext* cmd_cntx,
                                    CmdArgList /*args*/) {
  auto* rb = cmd_cntx->rb();
  rb->BuildSimpleString("OK");
  co_return;
}

CoroTask GenericFamily::ShutDown(CommandContext*, CmdArgList) {
  RedisServer::Instance().MainProactor()->DispatchBrief([] {
    LOG(INFO) << "[shutdown] ShutDown invoked";
    RedisServer::Instance().Stop();
  });
  co_return;
}

CoroTask GenericFamily::Debug(CommandContext* cmd_cntx, CmdArgList args) {
  std::string_view sub = (args.size() > 1) ? std::string_view(args[1]) : "";
  if (sub == "SHARD" || sub == "shard") {
    size_t sc = shard_pool->size();
    std::string out;
    for (size_t i = 2; i < args.size(); ++i) {
      std::string_view key = args[i];
      ShardId sid = ShardIndex(key, static_cast<ssize_t>(sc));
      LOG(INFO) << "[debug-shard] key='" << key << "' routes_to_shard=" << sid
                << " (shard_count=" << sc << ")";
      out += std::string(key) + "->shard" + std::to_string(sid) + " ";
    }
    cmd_cntx->rb()->BuildSimpleString(out.empty() ? "OK" : out);
    co_return;
  }
  auto cb = [](Transaction* /*tx*/, Shard* es) {
    ShardId sid = es->shard_id();
    auto& storage = es->GetShardStorage();
    for (DbIndex dbid = 0; dbid < storage.DbCount(); ++dbid) {
      storage.Traverse(
          dbid, [&](const PrimeKey& key, const PrimeValue& /*val*/) {
            LOG(INFO) << "[debug-db] shard=" << sid << " dbid=" << dbid
                      << " key='" << key.GetSlice() << "'";
          });
    }
    return true;
  };
  co_await cmd::SingleHopT(cb);
  cmd_cntx->rb()->BuildSimpleString("OK");
  co_return;
}

// void GenericFamily::Select(CmdArgList args, CommandContext* cmd_cntx) {
//   // TODO
// }

constexpr CommandSpec kCommands[] = {
    {"DEL", CO::JOURNALED, 1, -1, &GenericFamily::Delex},
    {"PING", CO::NO_KEY_TRANSACTIONAL, 0, 0, &GenericFamily::Ping},
    {"EXISTS", CO::READONLY, 1, -1, &GenericFamily::Exists},
    {"EXPIRE", CO::JOURNALED, 1, 1, &GenericFamily::Expire},
    {"EXPIRETIME", CO::READONLY, 1, 1, &GenericFamily::ExpireTime},
    {"TTL", CO::READONLY, 1, 1, &GenericFamily::Ttl},
    {"CLIENT", CO::NO_KEY_TRANSACTIONAL, 0, 0, &GenericFamily::Client_Info},
    {"HELLO", CO::NO_KEY_TRANSACTIONAL, 0, 0, &GenericFamily::Client_Info},
    {"SHUTDOWN", CO::NO_KEY_TRANSACTIONAL, 0, 0, &GenericFamily::ShutDown},
    {"DEBUG", CO::GLOBAL_TRANS, 0, 0, &GenericFamily::Debug},
};
static_assert(CheckUniqueNames(kCommands), "generic family: duplicate names");

void GenericFamily::Register(CommandRegistry* registry) {
  registry->Register(kCommands);
}

void RegisterGeneric(CommandRegistry* registry) {
  GenericFamily::Register(registry);
}

}  // namespace dfly

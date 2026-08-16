// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "generic_family.hpp"

#include <glog/logging.h>

#include <atomic>
#include <optional>

#include "cmd_support.hpp"
#include "detail/common.hpp"
#include "network/redis_server.hpp"
#include "sharding/db_slice.hpp"
#include "sharding/engine_shard_set.hpp"
#include "sharding/namespaces.hpp"
#include "sharding/op_status.hpp"

namespace dfly {
using namespace dfly::cmd;

facade::OpResult<uint32_t> OpDel(Transaction* tx, DbSlice& db_slice) {
  uint32_t res = 0;
  auto& slice = tx->GetSlice(db_slice.shard_id());
  for (const auto& [key, keyId] : slice) {
    auto it = db_slice.FindMutable(tx->GetDbContext(), key).it;
    if (!IsValid(it.GetInnerIt())) {
      continue;
    }
    db_slice.Del(tx->GetDbContext(), it, nullptr);
    ++res;
  }

  return res;
}

CoroTask CmdDel(CommandContext* cmd_cntx, CmdArgList args) {
  (void)args;

  std::atomic<uint32_t> result = 0;
  auto cb = [&](Transaction* tx, EngineShard* es) -> facade::OpResult<void> {
    DbSlice& dbslice = tx->GetDbSlice(es->shard_id());
    auto res = OpDel(tx, dbslice);
    result.fetch_add(res.value_or(0), std::memory_order_relaxed);
    return {OpStatus::OK};
  };

  facade::OpResult<void> res = co_await cmd::SingleHopT(cb);
  uint32_t del_cnt = result.load(std::memory_order_relaxed);

  auto* rb = cmd_cntx->rb();
  if (res.status() == OpStatus::OK)
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

  auto Op = [](Transaction* tx,
               DbSlice& db_slice) -> facade::OpResult<uint32_t> {
    auto& slice = tx->GetSlice(db_slice.shard_id());
    uint32_t res = 0;
    for (const auto& [key, keyId] : slice) {
      auto find_res = db_slice.FindReadOnly(tx->GetDbContext(), key);
      res += IsValid(find_res.GetInnerIt());
    }
    return {res};
  };

  std::atomic<uint32_t> result{0};

  auto cb = [&result, &Op](Transaction* t,
                           EngineShard* es) -> facade::OpResult<void> {
    auto res = Op(t, t->GetDbSlice(es->shard_id()));
    result.fetch_add(res.value_or(0), std::memory_order_relaxed);
    return {OpStatus::OK};
  };

  facade::OpResult<void> res = co_await cmd::SingleHopT(cb);

  auto* rb = cmd_cntx->rb();
  if (res.status() == OpStatus::OK) {
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
  auto cb = [&](Transaction* t, EngineShard* es) -> facade::OpResult<void> {
    auto& db_slice = t->GetDbSlice(es->shard_id());
    auto find_res = db_slice.FindMutable(t->GetDbContext(), key);
    if (!IsValid(find_res.it.GetInnerIt())) {
      return {OpStatus::KEY_NOTFOUND};
    }
    auto ttlTime = t->GetDbContext().GetTimeNowMs() / 1000 + sec;  // 精度丢失
    return db_slice.UpdateExpire(t->GetDbContext(), find_res.it, ttlTime);
  };
  auto res = co_await cmd::SingleHopT(cb);

  auto* rb = cmd_cntx->rb();
  if (res.status() == OpStatus::OK) {
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
  auto cb = [&](Transaction* t, EngineShard* es) -> facade::OpResult<int64_t> {
    auto& db_slice = t->GetDbSlice(es->shard_id());
    auto it = db_slice.FindReadOnly(t->GetDbContext(), key);
    if (!IsValid(it.GetInnerIt())) return {OpStatus::KEY_NOTFOUND};

    if (!it.GetInnerIt()->first.HasExpire()) return {OpStatus::SKIPPED};

    int64_t ttl_ms = it.GetInnerIt()->first.GetExpireTime();

    return {ttl_ms};
  };

  facade::OpResult<int64_t> res = co_await cmd::SingleHopT(cb);

  auto* rb = cmd_cntx->rb();
  if (res.status() == OpStatus::OK) {
    rb->BuildInteger(res.value());
  } else {
    if (res.status() == OpStatus::KEY_NOTFOUND) {
      rb->BuildInteger(-2);
    } else if (res.status() == OpStatus::SKIPPED) {
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
  auto cb = [&](Transaction* t, EngineShard* es) -> facade::OpResult<int64_t> {
    auto& db_slice = t->GetDbSlice(es->shard_id());
    auto it = db_slice.FindReadOnly(t->GetDbContext(), key);
    if (!IsValid(it.GetInnerIt())) return {OpStatus::KEY_NOTFOUND};

    if (!it.GetInnerIt()->first.HasExpire()) return {OpStatus::SKIPPED};

    auto ttlTime = it.GetInnerIt()->first.GetExpireTime() -
                   t->GetDbContext().GetTimeNowMs() / 1000;
    DCHECK_GT(ttlTime, 0);
    return ttlTime;
  };

  facade::OpResult<int64_t> res = co_await cmd::SingleHopT(cb);

  auto* rb = cmd_cntx->rb();
  if (res.status() == OpStatus::OK) {
    rb->BuildInteger(res.value());
  } else {
    if (res.status() == OpStatus::KEY_NOTFOUND) {
      rb->BuildInteger(-2);
    } else if (res.status() == OpStatus::SKIPPED) {
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
  ser->MainProactor()->DispatchBrief([] {
    LOG(INFO) << "[shutdown] ShutDown invoked";
    ser->SaveAndStop();
  });
  co_return;
}

CoroTask CmdSave(CommandContext* cmd_cntx) {
  LOG(INFO) << "[save] CmdSave invoked, dir=" << ser->data_dir();
  std::atomic_bool all_ok{true};
  auto cb = [&all_ok](Transaction* /*tx*/, EngineShard* es) {
    LOG(INFO) << "[save] CmdSave callback on shard " << es->shard_id();
    if (!RdbSerializer::SaveShard(es->shard_id(), ser->data_dir())) {
      all_ok.store(false);
    }
    return true;
  };
  co_await cmd::SingleHopT(cb);
  LOG(INFO) << "[save] CmdSave SingleHop done, all_ok=" << all_ok.load();
  cmd_cntx->rb()->BuildSimpleString(all_ok.load() ? "OK"
                                                  : "ERR RDB save failed");
  co_return;
}

CoroTask GenericFamily::Save(CommandContext* cmd_cntx, CmdArgList /*args*/) {
  return CmdSave(cmd_cntx);
}

CoroTask GenericFamily::Debug(CommandContext* cmd_cntx, CmdArgList args) {
  std::string_view sub = (args.size() > 1) ? std::string_view(args[1]) : "";
  if (sub == "SHARD" || sub == "shard") {
    size_t sc = shard_set->size();
    std::string out;
    for (size_t i = 2; i < args.size(); ++i) {
      std::string_view key = args[i];
      ShardId sid = Shard(key, static_cast<ssize_t>(sc));
      LOG(INFO) << "[debug-shard] key='" << key << "' routes_to_shard=" << sid
                << " (shard_count=" << sc << ")";
      out += std::string(key) + "->shard" + std::to_string(sid) + " ";
    }
    cmd_cntx->rb()->BuildSimpleString(out.empty() ? "OK" : out);
    co_return;
  }
  auto cb = [](Transaction* /*tx*/, EngineShard* es) {
    ShardId sid = es->shard_id();
    auto& ns = namespaces->GetDefaultNamespace();
    auto& db_slice = ns.GetDbSlice(sid);
    for (DbIndex dbid = 0; dbid < db_slice.DbCount(); ++dbid) {
      db_slice.TraverseTable(
          dbid, [&](const PrimeKey& key, const PrimeValue& /*val*/) {
            std::string scratch;
            LOG(INFO) << "[debug-db] shard=" << sid << " dbid=" << dbid
                      << " key='" << key.GetSlice(&scratch) << "'";
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

using CI = CommandId;
void GenericFamily::Register(CommandRegistry* registry) {
  registry->StartFamily();
  *registry
      << CI{"DEL", CO::JOURNALED, 1, -1}.SetHandler(&GenericFamily::Delex)
      << CI{"PING", CO::NO_KEY_TRANSACTIONAL, 0, 0}.SetHandler(
             &GenericFamily::Ping)
      << CI{"EXISTS", CO::READONLY, 1, -1}.SetHandler(&GenericFamily::Exists)
      << CI{"EXPIRE", CO::JOURNALED, 1, 1}.SetHandler(&GenericFamily::Expire)
      << CI{"EXPIRETIME", CO::READONLY, 1, 1}.SetHandler(
             &GenericFamily::ExpireTime)
      << CI{"TTL", CO::READONLY, 1, 1}.SetHandler(&GenericFamily::Ttl)
      << CI{"CLIENT", CO::NO_KEY_TRANSACTIONAL, 0, 0}.SetHandler(
             &GenericFamily::Client_Info)
      << CI{"HELLO", CO::NO_KEY_TRANSACTIONAL, 0, 0}.SetHandler(
             &GenericFamily::Client_Info)
      << CI{"SHUTDOWN", CO::NO_KEY_TRANSACTIONAL, 0, 0}.SetHandler(
             &GenericFamily::ShutDown)
      << CI{"SAVE", CO::GLOBAL_TRANS, 0, 0}.SetHandler(&GenericFamily::Save)
      << CI{"DEBUG", CO::GLOBAL_TRANS, 0, -1}.SetHandler(&GenericFamily::Debug);
}

void RegisterGeneric(CommandRegistry* registry) {
  GenericFamily::Register(registry);
}

}  // namespace dfly

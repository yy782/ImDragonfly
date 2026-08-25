#include "string_family.hpp"

#include <glog/logging.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

#include "cmd_support.hpp"
#include "command_registry.hpp"
#include "detail/common_types.hpp"
#include "detail/conn_context.hpp"
#include "detail/op_status.hpp"
#include "redis/facade/ParseRESP.hpp"
#include "sharding/shard.hpp"
#include "transaction_layer/transaction.hpp"
#include "util/Strings.hpp"
#include "util/arg_parse.hpp"
namespace dfly {

using cmd::CoroTask;

namespace {

constexpr uint32_t kMaxStrLen [[maybe_unused]] = 1 << 28;

using util::GetArgs;
using util::ParseOne;

class SetContext {
 public:
  static std::optional<SetContext> Parse(CmdArgList args, size_t off) {
    SetContext c;
    bool has_ex = false, has_px = false;
    for (size_t i = off; i < args.size(); ++i) {
      if (util::EqualsIgnoreCaseStd(args[i], std::string_view("EX"))) {
        if (has_px || c.keepttl_ || i + 1 >= args.size()) return std::nullopt;
        int64_t sec;
        if (!ParseOne(args[++i], sec) || sec < 0 ||
            uint64_t(sec) > UINT64_MAX / 1000)
          return std::nullopt;
        c.expire_ms_ = sec * 1000;
        has_ex = true;
      } else if (util::EqualsIgnoreCaseStd(args[i], std::string_view("PX"))) {
        if (has_ex || c.keepttl_ || i + 1 >= args.size()) return std::nullopt;
        int64_t ms;
        if (!ParseOne(args[++i], ms) || ms < 0) return std::nullopt;
        c.expire_ms_ = ms;
        has_px = true;
      } else if (util::EqualsIgnoreCaseStd(args[i],
                                           std::string_view("KEEPTTL"))) {
        if (has_ex || has_px) return std::nullopt;
        c.keepttl_ = true;
      } else if (util::EqualsIgnoreCaseStd(args[i], std::string_view("NX"))) {
        if (c.xx_) return std::nullopt;
        c.nx_ = true;
      } else if (util::EqualsIgnoreCaseStd(args[i], std::string_view("XX"))) {
        if (c.nx_) return std::nullopt;
        c.xx_ = true;
      } else {
        return std::nullopt;
      }
    }
    return c;
  }

  bool CanSet(bool is_new) const {
    return !(nx_ && !is_new) && !(xx_ && is_new);
  }

  bool ShouldRollback(bool is_new) const { return xx_ && is_new; }

  void ApplyTtl(ShardStorage::WriteIterator& it, const DbContext& cntx) const {
    if (keepttl_) return;
    if (expire_ms_ != 0) {
      it.set_ttl(uint64_t(expire_ms_) + cntx.GetTimeNowMs());
    } else if (!it.is_new()) {
      it.set_ttl(0);
    }
  }

 private:
  SetContext() = default;

  bool nx_ = false, xx_ = false, keepttl_ = false;
  int64_t expire_ms_ = 0;
};

}  // namespace

CoroTask StringFamily::Set(CommandContext* cmd_cntx, CmdArgList args) {
  DCHECK_GE(args.size(), 3);
  std::string_view key = args[1], value = args[2];
  auto ctx = SetContext::Parse(args, 3);
  if (!ctx) {
    cmd_cntx->rb()->BuildError("syntax error");
    co_return;
  }

  auto cb = [key, value, ctx](Transaction* tx, Shard* shard) -> OpResult<bool> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto ent = storage.FindOrInsert(cntx, key);
    if (!ent) return util::make_unexpected(ent.error());
    auto& e = ent.value();

    if (!ctx->CanSet(e.is_new())) {
      if (ctx->ShouldRollback(e.is_new())) e.erase();
      return false;
    }

    e.value() = PrimeValue{value};
    ctx->ApplyTtl(e, cntx);
    return true;
  };
  auto result = co_await cmd::SingleHopT(cb);

  auto* rb = cmd_cntx->rb();
  if (result.has_value() && result.value()) {
    rb->BuildOk();
  } else if (!result.has_value() && result.error() == OpStatus::SYNTAX_ERROR) {
    rb->BuildError("syntax error");
  } else {
    rb->BuildNullBulkString();
  }
  co_return;
}

CoroTask StringFamily::MSet(CommandContext* cmd_cntx, CmdArgList args) {
  auto cb = [&args](Transaction* tx, Shard* es) -> OpResult<void> {
    auto& shard = es->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();
    for (const auto& [key, keyId] : tx->GetSlice(es->shard_id())) {
      auto& value = args[keyId + 1];
      auto res = shard.Upsert(cntx, key, PrimeValue{value}, 0);
      if (!res) return util::make_unexpected(res.error());
    }
    return {};
  };

  co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  rb->BuildSimpleString("OK");
  co_return;
}

CoroTask StringFamily::MGet(CommandContext* cmd_cntx, CmdArgList /*args*/) {
  std::vector<std::string> vec(cmd_cntx->tx()->GetKeyNum());
  auto cb = [&vec](Transaction* tx, Shard* es) -> OpResult<void> {
    auto& shard = es->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();
    for (const auto& [key, keyId] : tx->GetSlice(es->shard_id())) {
      auto res = shard.Find(cntx, key);
      if (res) {
        vec[keyId - 1] = res.value()->second.ToString();
      } else {
        vec[keyId - 1] = "";  // args第一个参数是MGET,与vec不同，要减一
      }
    }
    return {};
  };
  co_await cmd::SingleHopT(cb);

  auto* rb = cmd_cntx->rb();
  rb->BuildArray(std::move(vec));
  co_return;
}

CoroTask StringFamily::Get(CommandContext* cmd_cntx, CmdArgList args) {
  auto cb = [key = args[1]](Transaction* tx,
                            Shard* es) -> OpResult<std::string> {
    DCHECK_EQ(Shard::tlocal()->shard_id(), es->shard_id());
    auto res = es->GetShardStorage().Find(tx->GetDbContext(), key);

    if (!res) {
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }
    const PrimeValue& pv = res.value()->second;
    if (pv.IsInt()) {
      return EncodeBulkString(pv.AsInt());
    }
    return EncodeBulkString(pv.GetSlice());
  };
  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  if (result.has_value()) {
    rb->SendRaw(std::move(result.value()));
  } else {
    rb->BuildNullBulkString();
  }

  co_return;
}

bool TryGetInt64(const PrimeValue& pv, int64_t* out) {
  if (pv.IsInt()) {
    *out = pv.AsInt();
    return true;
  }
  std::string_view s = pv.GetSlice();
  auto res = std::from_chars(s.data(), s.data() + s.size(), *out);
  return res.ec == std::errc() && res.ptr == s.data() + s.size();
}

CoroTask IncrByImpl(CommandContext* cmd_cntx, std::string_view key,
                    int64_t delta) {
  auto cb = [key, delta](Transaction* tx, Shard* es) -> OpResult<int64_t> {
    auto& shard = es->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    int64_t next = 0;
    OpStatus err = OpStatus::OK;
    auto up = shard.Mutate(cntx, key, [&](PrimeValue* pv) {
      if (pv->IsEmpty()) {
        next = delta;
        pv->SetInt(next);
        return;
      }
      if (pv->IsRobj()) {
        err = OpStatus::WRONG_TYPE;
        return;
      }
      int64_t cur = 0;
      if (pv->IsInt()) {
        cur = pv->AsInt();
      } else if (!TryGetInt64(*pv, &cur)) {
        err = OpStatus::INVALID_VALUE;
        return;
      }
      if (__builtin_add_overflow(cur, delta, &next)) {
        err = OpStatus::OUT_OF_RANGE;
        return;
      }
      pv->SetInt(next);
    });
    if (err != OpStatus::OK) return util::make_unexpected(err);
    if (!up) return util::make_unexpected(up.error());
    return next;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  if (result.has_value()) {
    rb->BuildInteger(result.value());
  } else {
    switch (result.error()) {
      case OpStatus::INVALID_VALUE:
        rb->BuildError("value is not an integer or out of range");
        break;
      case OpStatus::OUT_OF_RANGE:
        rb->BuildError("increment or decrement would overflow");
        break;
      default:
        rb->BuildError(
            "WRONG_TYPE Operation against a key holding the wrong "
            "kind of value");
    }
  }
  co_return;
}

CoroTask StringFamily::Incr(CommandContext* cmd_cntx, CmdArgList args) {
  DCHECK_GE(args.size(), 2);
  std::string_view key = args[1];
  return IncrByImpl(cmd_cntx, key, 1);
}

CoroTask StringFamily::Decr(CommandContext* cmd_cntx, CmdArgList args) {
  DCHECK_GE(args.size(), 2);
  std::string_view key = args[1];
  return IncrByImpl(cmd_cntx, key, -1);
}

CoroTask StringFamily::IncrBy(CommandContext* cmd_cntx, CmdArgList args) {
  std::string_view key;
  int64_t delta;
  if (!GetArgs(args, 1, key, delta)) {
    cmd_cntx->rb()->BuildError("syntax error");
    return CoroTask{};
  }
  return IncrByImpl(cmd_cntx, key, delta);
}

CoroTask StringFamily::DecrBy(CommandContext* cmd_cntx, CmdArgList args) {
  std::string_view key;
  int64_t delta;
  if (!GetArgs(args, 1, key, delta)) {
    cmd_cntx->rb()->BuildError("syntax error");
    return CoroTask{};
  }
  int64_t ndelta;
  if (__builtin_sub_overflow((int64_t)0, delta, &ndelta)) {
    cmd_cntx->rb()->BuildError("increment or decrement would overflow");
    return CoroTask{};
  }
  return IncrByImpl(cmd_cntx, key, ndelta);
}

CoroTask StringFamily::Append(CommandContext* cmd_cntx, CmdArgList args) {
  DCHECK_GE(args.size(), 3);
  std::string_view key = args[1], val = args[2];

  auto cb = [key, val](Transaction* tx, Shard* es) -> OpResult<size_t> {
    auto& shard = es->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    size_t new_len = 0;
    OpStatus err = OpStatus::OK;
    auto up = shard.Mutate(cntx, key, [&](PrimeValue* pv) {
      if (pv->IsEmpty()) {
        pv->SetString(val);
        new_len = val.size();
        return;
      }
      if (pv->IsRobj()) {
        err = OpStatus::WRONG_TYPE;
        return;
      }
      std::string cur = pv->ToString();
      cur.append(val);
      new_len = cur.size();
      pv->SetString(std::move(cur));
    });
    if (err != OpStatus::OK) return util::make_unexpected(err);
    if (!up) return util::make_unexpected(up.error());
    return new_len;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  if (result.has_value()) {
    rb->BuildInteger(result.value());
  } else {
    rb->BuildError(
        "WRONG_TYPE Operation against a key holding the wrong kind "
        "of value");
  }
  co_return;
}

CoroTask StringFamily::Strlen(CommandContext* cmd_cntx, CmdArgList args) {
  DCHECK_GE(args.size(), 2);
  std::string_view key = args[1];

  auto cb = [key](Transaction* tx, Shard* es) -> OpResult<size_t> {
    auto res = es->GetShardStorage().Find(tx->GetDbContext(), key);
    if (!res) {
      return size_t{0};
    }
    return res.value()->second.ToString().size();
  };

  auto result = co_await cmd::SingleHopT(cb);
  cmd_cntx->rb()->BuildInteger(result.value());
  co_return;
}

CoroTask StringFamily::Setnx(CommandContext* cmd_cntx, CmdArgList args) {
  DCHECK_GE(args.size(), 3);
  std::string_view key = args[1], value = args[2];

  auto cb = [key, value](Transaction* tx, Shard* es) -> OpResult<int> {
    auto& shard = es->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();
    auto ent = shard.FindOrInsert(cntx, key);
    if (!ent) return util::make_unexpected(ent.error());
    auto& e = ent.value();
    if (!e.is_new()) return 0;

    e.value() = PrimeValue{value};
    return 1;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  if (result.has_value()) {
    rb->BuildInteger(result.value());
  } else {
    rb->BuildError(
        "WRONG_TYPE Operation against a key holding the wrong kind "
        "of value");
  }
  co_return;
}

CoroTask StringFamily::Getset(CommandContext* cmd_cntx, CmdArgList args) {
  DCHECK_GE(args.size(), 3);
  std::string_view key = args[1], value = args[2];

  auto cb = [key, value](Transaction* tx, Shard* es) -> OpResult<std::string> {
    auto& shard = es->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto ent = shard.FindOrInsert(cntx, key);
    if (!ent) return util::make_unexpected(ent.error());
    auto& e = ent.value();

    if (!e.is_new()) {
      if (e.value().IsRobj()) {
        return util::make_unexpected(OpStatus::WRONG_TYPE);
      }
      std::string old = e.value().ToString();
      e.value() = PrimeValue{value};
      e.set_ttl(0);
      return old;
    }

    e.value() = PrimeValue{value};
    return util::make_unexpected(OpStatus::KEY_NOTFOUND);
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  if (result.has_value()) {
    rb->BuildBulkString(result.value());
  } else if (result.error() == OpStatus::KEY_NOTFOUND) {
    rb->BuildNullBulkString();
  } else {
    rb->BuildError(
        "WRONG_TYPE Operation against a key holding the wrong kind "
        "of value");
  }
  co_return;
}

CoroTask StringFamily::Getrange(CommandContext* cmd_cntx, CmdArgList args) {
  std::string_view key;
  int64_t start, end;
  if (!GetArgs(args, 1, key, start, end)) {
    cmd_cntx->rb()->BuildError("syntax error");
    co_return;
  }

  auto cb = [key, start, end](Transaction* tx,
                              Shard* es) -> OpResult<std::string> {
    auto res = es->GetShardStorage().Find(tx->GetDbContext(), key);
    if (!res) {
      return std::string{};
    }

    std::string s = res.value()->second.ToString();
    int64_t len = (int64_t)s.size();
    int64_t st = start < 0 ? len + start : start;
    int64_t en = end < 0 ? len + end : end;
    st = std::max<int64_t>(st, 0);
    en = std::min<int64_t>(en, len - 1);
    if (len == 0 || st > en) {
      return std::string{};
    }
    return s.substr((size_t)st, (size_t)(en - st + 1));
  };

  auto result = co_await cmd::SingleHopT(cb);
  cmd_cntx->rb()->BuildBulkString(result.value());
  co_return;
}

CoroTask StringFamily::Setrange(CommandContext* cmd_cntx, CmdArgList args) {
  std::string_view key, value;
  int64_t offset;
  if (!GetArgs(args, 1, key, offset, value)) {
    cmd_cntx->rb()->BuildError("syntax error");
    co_return;
  }
  if (offset < 0) {
    cmd_cntx->rb()->BuildError("offset is out of range");
    co_return;
  }

  auto cb = [key, offset, value](Transaction* tx,
                                 Shard* es) -> OpResult<size_t> {
    if ((size_t)offset > kMaxStrLen) {
      return util::make_unexpected(OpStatus::OUT_OF_RANGE);
    }

    auto& shard = es->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    size_t new_len = 0;
    OpStatus err = OpStatus::OK;
    auto up = shard.Mutate(cntx, key, [&](PrimeValue* pv) {
      std::string cur = pv->IsEmpty() ? std::string{} : pv->ToString();
      if ((size_t)offset + value.size() > kMaxStrLen) {
        err = OpStatus::OUT_OF_RANGE;
        return;
      }
      if (cur.size() < (size_t)offset) {
        cur.resize((size_t)offset, '\0');
      }
      cur.replace((size_t)offset, value.size(), value);
      new_len = cur.size();
      pv->SetString(std::move(cur));
    });
    if (err != OpStatus::OK) return util::make_unexpected(err);
    if (!up) return util::make_unexpected(up.error());
    return new_len;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  if (result.has_value()) {
    rb->BuildInteger(result.value());
  } else if (result.error() == OpStatus::OUT_OF_RANGE) {
    rb->BuildError("string exceeds maximum allowed size");
  } else {
    rb->BuildError(
        "WRONG_TYPE Operation against a key holding the wrong kind "
        "of value");
  }
  co_return;
}

CoroTask StringFamily::Getdel(CommandContext* cmd_cntx, CmdArgList args) {
  DCHECK_GE(args.size(), 2);
  std::string_view key = args[1];

  auto cb = [key](Transaction* tx, Shard* es) -> OpResult<std::string> {
    auto& shard = es->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto f = shard.Find(cntx, key);
    if (!f) {
      if (f.error() != OpStatus::KEY_NOTFOUND)
        return util::make_unexpected(f.error());
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }
    std::string old = f.value()->second.ToString();
    auto del = shard.Delete(cntx, key);
    if (!del) return util::make_unexpected(del.error());
    return old;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  if (result.has_value()) {
    rb->BuildBulkString(result.value());
  } else {
    rb->BuildNullBulkString();
  }
  co_return;
}

namespace {
constexpr CommandSpec kCommands[] = {
    {"SET", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1,
     &StringFamily::Set},
    {"GET", CO::READONLY, 1, 1, &StringFamily::Get},
    {"MGET", CO::READONLY | CO::IDEMPOTENT, 1, -1, &StringFamily::MGet},
    {"MSET", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, -1,
     &StringFamily::MSet, 2},
    {"APPEND", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1,
     &StringFamily::Append},
    {"STRLEN", CO::READONLY, 1, 1, &StringFamily::Strlen},
    {"INCR", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1,
     &StringFamily::Incr},
    {"INCRBY", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1,
     &StringFamily::IncrBy},
    {"DECR", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1,
     &StringFamily::Decr},
    {"DECRBY", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1,
     &StringFamily::DecrBy},
    {"SETNX", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1,
     &StringFamily::Setnx},
    {"GETSET", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1,
     &StringFamily::Getset},
    {"GETRANGE", CO::READONLY, 1, 1, &StringFamily::Getrange},
    {"SETRANGE", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1,
     &StringFamily::Setrange},
    {"GETDEL", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1,
     &StringFamily::Getdel},
};
static_assert(CheckUniqueNames(kCommands), "string family: duplicate names");

}  // namespace

void RegisterStringFamily(CommandRegistry* registry) {
  registry->Register(kCommands);
}

}  // namespace dfly

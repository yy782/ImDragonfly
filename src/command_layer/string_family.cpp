// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
// export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:$LD_LIBRARY_PATH

#include <glog/logging.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

#include "string_family.hpp"

#include "cmd_support.hpp"
#include "command_registry.hpp"
#include "detail/conn_context.hpp"
#include "detail/op_status.hpp"
#include "detail/tx_base.hpp"
#include "sharding/shard.hpp"
#include "transaction_layer/transaction.hpp"
namespace dfly {

using cmd::CoroTask;

namespace {

constexpr uint32_t kMaxStrLen [[maybe_unused]] = 1 << 28;

// ---- 极简参数解析 -----------------------------------------------------------
// 从 args[off] 起顺序取参数：std::string_view 直接取，int64_t 用 from_chars 解析。
// 任一参数缺失或解析失败即返回 false。

bool ParseOne(std::string_view s, std::string_view& out) {
  out = s;
  return true;
}

bool ParseOne(std::string_view s, int64_t& out) {
  auto res = std::from_chars(s.data(), s.data() + s.size(), out);
  return res.ec == std::errc() && res.ptr == s.data() + s.size();
}

template <typename... Ts>
bool GetArgs(CmdArgList args, size_t off, Ts&... out) {
  if (args.size() < off + sizeof...(Ts)) return false;
  size_t i = off;
  return (ParseOne(args[i++], out) && ...);
}

// ASCII 忽略大小写比较（选项均为大写字母）
bool EqNoCase(std::string_view a, std::string_view b) {
  return a.size() == b.size() &&
         std::equal(a.begin(), a.end(), b.begin(),
                    [](char x, char y) { return (x | 0x20) == (y | 0x20); });
}

// ---- SET --------------------------------------------------------------------

// SET 可选参数解析结果
struct SetOpts {
  bool nx = false, xx = false, keepttl = false;
  int64_t expire_ms = 0;  // 0 = 不设置过期
};

// 解析 SET 的 EX/PX/KEEPTTL/NX/XX 选项；语法错误返回 nullopt。
std::optional<SetOpts> ParseSetOpts(CmdArgList args, size_t off) {
  SetOpts o;
  bool has_ex = false, has_px = false;
  for (size_t i = off; i < args.size(); ++i) {
    if (EqNoCase(args[i], "EX")) {
      if (has_px || o.keepttl || i + 1 >= args.size()) return std::nullopt;
      int64_t sec;
      if (!ParseOne(args[++i], sec) || sec < 0 ||
          uint64_t(sec) > UINT64_MAX / 1000)
        return std::nullopt;
      o.expire_ms = sec * 1000;
      has_ex = true;
    } else if (EqNoCase(args[i], "PX")) {
      if (has_ex || o.keepttl || i + 1 >= args.size()) return std::nullopt;
      int64_t ms;
      if (!ParseOne(args[++i], ms) || ms < 0) return std::nullopt;
      o.expire_ms = ms;
      has_px = true;
    } else if (EqNoCase(args[i], "KEEPTTL")) {
      if (has_ex || has_px) return std::nullopt;
      o.keepttl = true;
    } else if (EqNoCase(args[i], "NX")) {
      if (o.xx) return std::nullopt;
      o.nx = true;
    } else if (EqNoCase(args[i], "XX")) {
      if (o.nx) return std::nullopt;
      o.xx = true;
    } else {
      return std::nullopt;
    }
  }
  return o;
}

// 用零拷贝 view 就地编码成完整 RESP bulk string 帧 "$<len>\r\n<value>\r\n"。
// 值内容只拷贝这一次（直接进最终回复 buffer），避免 ToString() 临时 string
// 再经 BuildBulkString 二次拷贝。
std::string EncodeBulkString(std::string_view v) {
  std::string out;
  out.reserve(v.size() + 32);
  out.push_back('$');
  char buf[24];
  auto res = std::to_chars(buf, buf + sizeof(buf), v.size());
  out.append(buf, res.ptr - buf);
  out.append("\r\n");
  out.append(v);
  out.append("\r\n");
  return out;
}

CoroTask StringFamily::Set(CommandContext* cmd_cntx, CmdArgList args) {
  std::string_view key, value;
  if (!GetArgs(args, 1, key, value)) {
    cmd_cntx->rb()->BuildError("syntax error");
    co_return;
  }
  auto opts = ParseSetOpts(args, 3);
  if (!opts) {
    cmd_cntx->rb()->BuildError("syntax error");
    co_return;
  }

  auto cb = [key, value, opts](Transaction* tx, Shard* shard)
      -> OpResult<bool> {
    auto& storage = shard->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();
    const SetOpts& o = *opts;

    // ttl_at 为绝对毫秒（0 = 无 TTL，同时清除已有 TTL）
    uint64_t at_ms =
        o.expire_ms ? uint64_t(o.expire_ms) + cntx.GetTimeNowMs() : 0;

    if (o.nx || o.xx || o.keepttl) {
      bool exists = storage.Exists(cntx, key);
      if (o.xx && !exists) return false;  // XX 未命中：不写
      if (o.nx && exists) return false;   // NX 命中已有 key：不写
      if (o.keepttl) {
        // 保留旧 TTL（0 = 无 TTL）；新键不带 TTL
        at_ms = exists ? storage.ExpireTime(cntx, key).value_or(0) : 0;
      }
    }

    auto up = storage.Upsert(cntx, key, PrimeValue{value}, at_ms);
    if (!up) return util::make_unexpected(up.error());
    return up.value();  // true=新建，false=覆盖
  };
  auto result = co_await cmd::SingleHopT(cb);

  if (result.has_value() && result.value()) {
    cmd_cntx->rb()->BuildOk();
  } else {
    cmd_cntx->rb()->BuildNullBulkString();  // NX/XX 条件未命中或底层错误 → null
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
        vec[keyId - 1] = res.value().value->ToString();
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
                            EngineShard* es) -> OpResult<std::string> {
    DCHECK_EQ(Shard::tlocal()->shard_id(), es->shard_id());
    auto res = es->GetShardStorage().Find(tx->GetDbContext(), key);

    if (!res) {  // 没找到
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }

    // 这里在 shard 线程内，值在 dict 中稳定，用 GetSlice 拿零拷贝 view 就地
    // 编码成 RESP 帧；co_await 后协程在同一 shard 线程恢复，直接 SendRaw 发送。
    std::string scratch;
    std::string_view v = res.value().value->GetSlice(&scratch);
    return EncodeBulkString(v);
  };
  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  if (result.has_value()) {
    rb->SendRaw(std::move(result.value()));  // rb 不再加工，直接发送
  } else {
    rb->BuildNullBulkString();
  }

  co_return;
}

// 把存储值解析为 int64_t，用于 INCR/DECR 系列。非整数返回 false。
bool TryGetInt64(const PrimeValue& pv, int64_t* out) {
  if (pv.IsInt()) {
    *out = pv.AsInt();
    return true;
  }
  std::string scratch;
  std::string_view s = pv.GetSlice(&scratch);
  auto res = std::from_chars(s.data(), s.data() + s.size(), *out);
  return res.ec == std::errc() && res.ptr == s.data() + s.size();
}

CoroTask IncrByImpl(CommandContext* cmd_cntx, std::string_view key,
                    int64_t delta) {
  auto cb = [key, delta](Transaction* tx,
                         EngineShard* es) -> OpResult<int64_t> {
    auto& shard = es->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    int64_t next = 0;
    OpStatus err = OpStatus::OK;
    auto up = shard.Mutate(
        cntx, key,
        [&](PrimeValue* pv) {
          if (pv->IsEmpty()) {  // 新建键：cur = 0
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
            err = OpStatus::OUT_OF_RANGE;  // 溢出
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
  std::string_view key;
  if (!GetArgs(args, 1, key)) {
    cmd_cntx->rb()->BuildError("syntax error");
    return CoroTask{};
  }
  return IncrByImpl(cmd_cntx, key, 1);
}

CoroTask StringFamily::Decr(CommandContext* cmd_cntx, CmdArgList args) {
  std::string_view key;
  if (!GetArgs(args, 1, key)) {
    cmd_cntx->rb()->BuildError("syntax error");
    return CoroTask{};
  }
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
  std::string_view key, val;
  if (!GetArgs(args, 1, key, val)) {
    cmd_cntx->rb()->BuildError("syntax error");
    co_return;
  }

  auto cb = [key, val](Transaction* tx, Shard* es) -> OpResult<size_t> {
    auto& shard = es->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    size_t new_len = 0;
    OpStatus err = OpStatus::OK;
    auto up = shard.Mutate(
        cntx, key,
        [&](PrimeValue* pv) {
          if (pv->IsEmpty()) {  // 新建键
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
  std::string_view key;
  if (!GetArgs(args, 1, key)) {
    cmd_cntx->rb()->BuildError("syntax error");
    co_return;
  }

  auto cb = [key](Transaction* tx, Shard* es) -> OpResult<size_t> {
    auto res = es->GetShardStorage().Find(tx->GetDbContext(), key);
    if (!res) {
      return size_t{0};  // 不存在 → 长度 0
    }
    return res.value().value->ToString().size();
  };

  auto result = co_await cmd::SingleHopT(cb);
  cmd_cntx->rb()->BuildInteger(result.value());
  co_return;
}

CoroTask CmdSetnx(CommandContext* cmd_cntx, CmdArgList args) {
  std::string_view key, value;
  if (!GetArgs(args, 1, key, value)) {
    cmd_cntx->rb()->BuildError("syntax error");
    co_return;
  }

  auto cb = [key, value](Transaction* tx, Shard* es) -> OpResult<int> {
    auto& shard = es->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();
    if (shard.Exists(cntx, key)) {
      return 0;  // key 已存在，设置失败
    }
    auto up = shard.Upsert(cntx, key, PrimeValue{value}, 0);
    if (!up) return util::make_unexpected(up.error());
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
  std::string_view key, value;
  if (!GetArgs(args, 1, key, value)) {
    cmd_cntx->rb()->BuildError("syntax error");
    co_return;
  }

  auto cb = [key, value](Transaction* tx,
                         EngineShard* es) -> OpResult<std::string> {
    auto& shard = es->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto f = shard.Find(cntx, key);
    if (!f) {
      if (f.error() != OpStatus::KEY_NOTFOUND)
        return util::make_unexpected(f.error());
      // 旧值不存在：直接写入，返回 null
      auto up = shard.Upsert(cntx, key, PrimeValue{value}, 0);
      if (!up) return util::make_unexpected(up.error());
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);
    }

    if (f.value().value->IsRobj()) {
      return util::make_unexpected(OpStatus::WRONG_TYPE);
    }

    std::string old = f.value().value->ToString();
    // GETSET 会清除已有 key 的 TTL（Redis 语义）→ Upsert 传 ttl_at=0
    auto up = shard.Upsert(cntx, key, PrimeValue{value}, 0);
    if (!up) return util::make_unexpected(up.error());
    return old;
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
                              EngineShard* es) -> OpResult<std::string> {
    auto res = es->GetShardStorage().Find(tx->GetDbContext(), key);
    if (!res) {
      return std::string{};  // 不存在 → 空串
    }

    std::string s = res.value().value->ToString();
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
                                 EngineShard* es) -> OpResult<size_t> {
    if ((size_t)offset > kMaxStrLen) {
      return util::make_unexpected(OpStatus::OUT_OF_RANGE);
    }

    auto& shard = es->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    size_t new_len = 0;
    OpStatus err = OpStatus::OK;
    auto up = shard.Mutate(
        cntx, key,
        [&](PrimeValue* pv) {
          std::string cur = pv->IsEmpty() ? std::string{} : pv->ToString();
          if ((size_t)offset + value.size() > kMaxStrLen) {
            err = OpStatus::OUT_OF_RANGE;  // 超出最大字符串长度
            return;
          }
          if (cur.size() < (size_t)offset) {
            cur.resize((size_t)offset, '\0');  // 中间空洞用 \0 填充
          }
          cur.replace((size_t)offset, value.size(), value);
          new_len = cur.size();
          pv->SetString(std::move(cur));  // 必须先取长度，move 后源字符串 size 未定义
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
  std::string_view key;
  if (!GetArgs(args, 1, key)) {
    cmd_cntx->rb()->BuildError("syntax error");
    co_return;
  }

  auto cb = [key](Transaction* tx, Shard* es) -> OpResult<std::string> {
    auto& shard = es->GetShardStorage();
    const DbContext cntx = tx->GetDbContext();

    auto f = shard.Find(cntx, key);
    if (!f) {
      if (f.error() != OpStatus::KEY_NOTFOUND)
        return util::make_unexpected(f.error());
      return util::make_unexpected(OpStatus::KEY_NOTFOUND);  // 不存在 → null
    }
    std::string old = f.value().value->ToString();
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

// 命令目录：表驱动注册，constexpr 声明 + 编译期查重。
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

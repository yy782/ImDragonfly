// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
// export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:$LD_LIBRARY_PATH

#include <glog/logging.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <string>
#include <variant>

#include "cmd_arg_parser.hpp"
#include "cmd_support.hpp"
#include "command_registry.hpp"
#include "detail/conn_context.hpp"
#include "sharding/db_slice.hpp"
#include "sharding/engine_shard.hpp"
#include "sharding/op_status.hpp"
#include "transaction_layer/transaction.hpp"
namespace dfly {

namespace {

using CI = CommandId;

constexpr uint32_t kMaxStrLen [[maybe_unused]] = 1 << 28;

using ::cmd::CmdArgParser;
using cmd::CoroTask;
using Slice = Transaction::Slice;

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

class SetCmd {  // SET 命令处理器
 public:
  explicit SetCmd(const Slice& slice) : slice_(slice) {}

  // 条件设置失败（NX 命中已有 key / XX 未命中）时返回 NOT_SET，
  // 调用方应回复 null。
  enum class SetResult { OK, NOT_SET };

  enum SetFlags {
    SET_ALWAYS = 0,
    SET_KEEP_EXPIRE = 1 << 2,     /* KEEPTTL: Set and keep the ttl */
    SET_EXPIRE_AFTER_MS = 1 << 4, /* EX,PX: Expire after ms. */
    SET_NX = 1 << 5,              /* NX: only set if key does not exist */
    SET_XX = 1 << 6,              /* XX: only set if key already exists */
  };

  struct SetParams {
    uint16_t flags_ = SET_ALWAYS;
    uint64_t expire_after_ms_ = 0;

    constexpr bool IsConditionalSet() const {
      return flags_ & (SET_NX | SET_XX);
    }
  };

  facade::OpResult<SetResult> Set(const SetParams& params, std::string_view key,
                                  std::string_view value);

 private:
  facade::OpResult<void> SetExisting(const SetParams& params,
                                     std::string_view value,
                                     DbSlice::ItAndUpdater* it_upd);

  void AddNew(const SetParams& params, const DbSlice::Iterator& it,
              std::string_view key, std::string_view value);

  Slice slice_;
};

facade::OpResult<SetCmd::SetResult> SetCmd::Set(const SetParams& params,
                                                std::string_view key,
                                                std::string_view value) {
  DbSlice& db_slice = slice_.GetDbSlice();
  auto op_res = db_slice.AddOrFind(slice_.GetDbContext(), key, std::nullopt);
  if (!op_res) {
    return op_res.status();
  }

  if (!op_res->is_new) {
    if (params.flags_ & SET_NX) {
      return SetCmd::SetResult::NOT_SET;  // 已存在且要求 NX
    }
    auto st = SetExisting(params, value, &(*op_res));
    if (!st) {
      return st.status();
    }
    return SetCmd::SetResult::OK;
  } else {
    if (params.flags_ & SET_XX) {
      // AddOrFind 已创建空 key，需要回滚，避免 XX 未命中时留下空 key
      slice_.GetDbSlice().DelMutable(slice_.GetDbContext(), std::move(*op_res));
      return SetCmd::SetResult::NOT_SET;
    }
    AddNew(params, op_res->it, key, value);
    return SetCmd::SetResult::OK;
  }
}

facade::OpResult<void> SetCmd::SetExisting(const SetParams& params,
                                           std::string_view value,
                                           DbSlice::ItAndUpdater* it_upd) {
  PrimeValue& prime_value = it_upd->it->second;

  auto& db_slice = slice_.GetDbSlice();
  uint64_t at_ms =
      params.expire_after_ms_
          ? params.expire_after_ms_ + slice_.GetDbContext().GetTimeNowMs()
          : 0;

  if (!(params.flags_ & SET_KEEP_EXPIRE)) {
    if (at_ms) {
      db_slice.AddExpire(slice_.GetDbContext().GetDbIndex(), it_upd->it, at_ms);
    } else {
      db_slice.RemoveExpire(slice_.GetDbContext().GetDbIndex(), it_upd->it);
    }
  }
  prime_value.SetString(value);
  return OpStatus::OK;
}

void SetCmd::AddNew(const SetParams& params, const DbSlice::Iterator& it,
                    std::string_view key, std::string_view value) {
  (void)key;

  auto& db_slice = slice_.GetDbSlice();

  it->second = PrimeValue{value};

  if (params.expire_after_ms_) {
    db_slice.AddExpire(
        slice_.GetDbContext().GetDbIndex(), it,
        params.expire_after_ms_ + slice_.GetDbContext().GetTimeNowMs());
  }
}

struct ErrorReply {};
std::variant<SetCmd::SetParams, ErrorReply> ParseSetParams(
    CmdArgParser parser) {
  SetCmd::SetParams sparams;
  bool has_ex = false, has_px = false;
  while (parser.HasNext()) {
    if (parser.Check("EX")) {
      if (has_px) return ErrorReply{};  // EX 与 PX 互斥
      if (sparams.flags_ & SetCmd::SET_KEEP_EXPIRE)
        return ErrorReply{};  // 与 KEEPTTL 互斥
      if (!parser.HasNext()) return ErrorReply{};
      int64_t sec = parser.Next<int64_t>();
      if (parser.HasError() || sec < 0) return ErrorReply{};
      if (uint64_t(sec) > UINT64_MAX / 1000) return ErrorReply{};
      sparams.flags_ |= SetCmd::SET_EXPIRE_AFTER_MS;
      sparams.expire_after_ms_ = uint64_t(sec) * 1000;
      has_ex = true;
    } else if (parser.Check("PX")) {
      if (has_ex) return ErrorReply{};  // EX 与 PX 互斥
      if (sparams.flags_ & SetCmd::SET_KEEP_EXPIRE)
        return ErrorReply{};  // 与 KEEPTTL 互斥
      if (!parser.HasNext()) return ErrorReply{};
      int64_t ms = parser.Next<int64_t>();
      if (parser.HasError() || ms < 0) return ErrorReply{};
      sparams.flags_ |= SetCmd::SET_EXPIRE_AFTER_MS;
      sparams.expire_after_ms_ = uint64_t(ms);
      has_px = true;
    } else if (parser.Check("KEEPTTL")) {
      if (sparams.flags_ & SetCmd::SET_EXPIRE_AFTER_MS)
        return ErrorReply{};  // 与 EX/PX 互斥
      sparams.flags_ |= SetCmd::SET_KEEP_EXPIRE;
    } else if (parser.Check("NX")) {
      sparams.flags_ |= SetCmd::SET_NX;
    } else if (parser.Check("XX")) {
      sparams.flags_ |= SetCmd::SET_XX;
    } else {
      return ErrorReply{};
    }
  }
  if ((sparams.flags_ & SetCmd::SET_NX) && (sparams.flags_ & SetCmd::SET_XX)) {
    return ErrorReply{};  // NX 与 XX 互斥
  }
  return sparams;
}

CoroTask CmdSet(CommandContext* cmd_cntx, CmdArgList args) {
  args = args.subspan(1);

  CmdArgParser parser{args};

  auto [key, value] = parser.Next<std::string_view, std::string_view>();
  auto params_result = ParseSetParams(parser);
  if (std::holds_alternative<ErrorReply>(params_result)) {
    cmd_cntx->rb()->BuildError("syntax error");
    co_return;
  }
  auto& sparams = std::get<SetCmd::SetParams>(params_result);

  auto cb = [key, value, sparams](
                Transaction* t,
                EngineShard* shard) -> OpResult<SetCmd::SetResult> {
    DCHECK_EQ(EngineShard::tlocal()->shard_id(), shard->shard_id());
    return SetCmd(t->GetSlice(shard->shard_id())).Set(sparams, key, value);
  };

  auto result = co_await cmd::SingleHopT(cb);

  auto* rb = cmd_cntx->rb();
  if (result.status() == OpStatus::OK &&
      result.value() == SetCmd::SetResult::OK) {
    rb->BuildOk();
  } else {
    rb->BuildNullBulkString();  // NX/XX 条件未命中，回复 null
  }

  co_return;
}

CoroTask CmdMSet(CommandContext* cmd_cntx, CmdArgList args) {
  auto cb = [&args](Transaction* tx, EngineShard* es) -> OpResult<void> {
    auto& slice = tx->GetSlice(es->shard_id());
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    for (const auto& [key, keyId] : slice) {
      auto& value = args[keyId + 1];
      auto it_res =
          db_slice.AddOrUpdate(tx->GetDbContext(), key, PrimeValue{value}, 0);
      if (it_res.status() != facade::OpStatus::OK) {
        // TODO
      }
    }
    return {};
  };

  co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  rb->BuildSimpleString("OK");
  co_return;
}

CoroTask CmdMGet(CommandContext* cmd_cntx, CmdArgList /*args*/) {
  std::vector<std::string> vec(cmd_cntx->tx()->GetKeyNum());
  auto cb = [&vec](Transaction* tx, EngineShard* es) -> OpResult<void> {
    auto& slice = tx->GetSlice(es->shard_id());
    for (auto& [key, keyId] : slice) {
      auto it_res =
          tx->GetDbSlice(es->shard_id()).FindReadOnly(tx->GetDbContext(), key);
      if (it_res.GetInnerIt().owner() == nullptr) {  // 没找到
        vec[keyId - 1] = "";  // args第一个参数是MGET,与vec不同，要减一
      } else {
        vec[keyId - 1] = it_res.GetInnerIt()->second.ToString();
      }
    }
    return {};
  };
  co_await cmd::SingleHopT(cb);

  auto* rb = cmd_cntx->rb();
  rb->BuildArray(std::move(vec));
  co_return;
}

CoroTask CmdGet(CommandContext* cmd_cntx, CmdArgList args) {
  auto cb = [key = args[1]](Transaction* tx,
                            EngineShard* es) -> OpResult<std::string> {
    DCHECK_EQ(EngineShard::tlocal()->shard_id(), es->shard_id());
    auto it_res =
        tx->GetDbSlice(es->shard_id()).FindReadOnly(tx->GetDbContext(), key);

    if (it_res.GetInnerIt().owner() == nullptr) {  // 没找到
      return OpStatus::KEY_NOTFOUND;
    }

    // 这里在 shard 线程内，值在 dict 中稳定，用 GetSlice 拿零拷贝 view 就地
    // 编码成 RESP 帧；co_await 后协程在同一 shard 线程恢复，直接 SendRaw 发送。
    std::string scratch;
    std::string_view v = it_res.GetInnerIt()->second.GetSlice(&scratch);
    return {EncodeBulkString(v)};
  };
  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  if (result.status() == OpStatus::OK) {
    rb->SendRaw(*std::move(result));  // rb 不再加工，直接发送
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
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto it_res = db_slice.AddOrFind(tx->GetDbContext(), key, std::nullopt);
    if (!it_res) {
      return it_res.status();
    }

    auto& pv = it_res->it->second;
    int64_t cur = 0;
    if (!it_res->is_new) {
      if (pv.IsRobj()) {
        return OpStatus::WRONG_TYPE;
      }
      if (!TryGetInt64(pv, &cur)) {
        return OpStatus::INVALID_VALUE;
      }
    }

    int64_t next;
    if (__builtin_add_overflow(cur, delta, &next)) {
      return OpStatus::OUT_OF_RANGE;  // 溢出
    }
    pv.SetInt(next);
    return next;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  switch (result.status()) {
    case OpStatus::OK:
      rb->BuildInteger(result.value());
      break;
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
  co_return;
}

CoroTask Incr(CommandContext* cmd_cntx, CmdArgList args) {
  args = args.subspan(1);
  CmdArgParser parser{args};
  std::string_view key = parser.Next<std::string_view>();
  return IncrByImpl(cmd_cntx, key, 1);
}

CoroTask Decr(CommandContext* cmd_cntx, CmdArgList args) {
  args = args.subspan(1);
  CmdArgParser parser{args};
  std::string_view key = parser.Next<std::string_view>();
  return IncrByImpl(cmd_cntx, key, -1);
}

CoroTask IncrBy(CommandContext* cmd_cntx, CmdArgList args) {
  args = args.subspan(1);
  CmdArgParser parser{args};
  auto [key, delta] = parser.Next<std::string_view, int64_t>();
  if (parser.HasError()) {
    cmd_cntx->rb()->BuildError("syntax error");
    return CoroTask{};
  }
  return IncrByImpl(cmd_cntx, key, delta);
}

CoroTask DecrBy(CommandContext* cmd_cntx, CmdArgList args) {
  args = args.subspan(1);
  CmdArgParser parser{args};
  auto [key, delta] = parser.Next<std::string_view, int64_t>();
  if (parser.HasError()) {
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

CoroTask CmdAppend(CommandContext* cmd_cntx, CmdArgList args) {
  args = args.subspan(1);
  CmdArgParser parser{args};
  auto [key, val] = parser.Next<std::string_view, std::string_view>();

  auto cb = [key, val](Transaction* tx, EngineShard* es) -> OpResult<size_t> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto it_res = db_slice.AddOrFind(tx->GetDbContext(), key, std::nullopt);
    if (!it_res) {
      return it_res.status();
    }

    auto& pv = it_res->it->second;
    if (!it_res->is_new && pv.IsRobj()) {
      return OpStatus::WRONG_TYPE;
    }

    size_t new_len;
    if (it_res->is_new) {
      pv.SetString(val);
      new_len = val.size();
    } else {
      std::string cur = pv.ToString();
      cur.append(val);
      new_len = cur.size();
      pv.SetString(std::move(cur));
    }
    return new_len;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  if (result.status() == OpStatus::OK) {
    rb->BuildInteger(result.value());
  } else {
    rb->BuildError(
        "WRONG_TYPE Operation against a key holding the wrong kind "
        "of value");
  }
  co_return;
}

CoroTask CmdStrlen(CommandContext* cmd_cntx, CmdArgList args) {
  args = args.subspan(1);
  CmdArgParser parser{args};
  std::string_view key = parser.Next<std::string_view>();

  auto cb = [key](Transaction* tx, EngineShard* es) -> OpResult<size_t> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);
    if (it_res.GetInnerIt().owner() == nullptr) {
      return size_t{0};  // 不存在 → 长度 0
    }
    return it_res.GetInnerIt()->second.ToString().size();
  };

  auto result = co_await cmd::SingleHopT(cb);
  cmd_cntx->rb()->BuildInteger(result.value());
  co_return;
}

CoroTask CmdSetnx(CommandContext* cmd_cntx, CmdArgList args) {
  args = args.subspan(1);
  CmdArgParser parser{args};
  auto [key, value] = parser.Next<std::string_view, std::string_view>();

  auto cb = [key, value](Transaction* tx, EngineShard* es) -> OpResult<int> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto it_res = db_slice.AddOrFind(tx->GetDbContext(), key, std::nullopt);
    if (!it_res) {
      return it_res.status();
    }
    if (!it_res->is_new) {
      return 0;  // key 已存在，设置失败
    }
    it_res->it->second.SetString(value);
    return 1;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  if (result.status() == OpStatus::OK) {
    rb->BuildInteger(result.value());
  } else {
    rb->BuildError(
        "WRONG_TYPE Operation against a key holding the wrong kind "
        "of value");
  }
  co_return;
}

CoroTask CmdGetset(CommandContext* cmd_cntx, CmdArgList args) {
  args = args.subspan(1);
  CmdArgParser parser{args};
  auto [key, value] = parser.Next<std::string_view, std::string_view>();

  auto cb = [key, value](Transaction* tx,
                         EngineShard* es) -> OpResult<std::string> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto it_res = db_slice.AddOrFind(tx->GetDbContext(), key, std::nullopt);
    if (!it_res) {
      return it_res.status();
    }

    auto& pv = it_res->it->second;
    if (!it_res->is_new && pv.IsRobj()) {
      return OpStatus::WRONG_TYPE;
    }

    if (it_res->is_new) {
      pv.SetString(value);
      return OpStatus::KEY_NOTFOUND;  // 旧值不存在 → null
    }

    // GETSET 会清除已有 key 的 TTL（Redis 语义）
    db_slice.RemoveExpire(tx->GetDbContext().GetDbIndex(), it_res->it);

    std::string old = pv.ToString();
    pv.SetString(value);
    return old;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  if (result.status() == OpStatus::OK) {
    rb->BuildBulkString(result.value());
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    rb->BuildNullBulkString();
  } else {
    rb->BuildError(
        "WRONG_TYPE Operation against a key holding the wrong kind "
        "of value");
  }
  co_return;
}

CoroTask CmdGetrange(CommandContext* cmd_cntx, CmdArgList args) {
  args = args.subspan(1);
  CmdArgParser parser{args};
  auto [key, start, end] = parser.Next<std::string_view, int64_t, int64_t>();
  if (parser.HasError()) {
    cmd_cntx->rb()->BuildError("syntax error");
    co_return;
  }

  auto cb = [key, start, end](Transaction* tx,
                              EngineShard* es) -> OpResult<std::string> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto it_res = db_slice.FindReadOnly(tx->GetDbContext(), key);
    if (it_res.GetInnerIt().owner() == nullptr) {
      return std::string{};  // 不存在 → 空串
    }

    std::string s = it_res.GetInnerIt()->second.ToString();
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

CoroTask CmdSetrange(CommandContext* cmd_cntx, CmdArgList args) {
  args = args.subspan(1);
  CmdArgParser parser{args};
  auto [key, offset, value] =
      parser.Next<std::string_view, int64_t, std::string_view>();
  if (parser.HasError()) {
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
      return OpStatus::OUT_OF_RANGE;
    }

    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto it_res = db_slice.AddOrFind(tx->GetDbContext(), key, std::nullopt);
    if (!it_res) {
      return it_res.status();
    }

    auto& pv = it_res->it->second;
    if (!it_res->is_new && pv.IsRobj()) {
      return OpStatus::WRONG_TYPE;
    }

    std::string cur = it_res->is_new ? std::string{} : pv.ToString();
    if ((size_t)offset + value.size() > kMaxStrLen) {
      return OpStatus::OUT_OF_RANGE;  // 超出最大字符串长度
    }
    if (cur.size() < (size_t)offset) {
      cur.resize((size_t)offset, '\0');  // 中间空洞用 \0 填充
    }
    cur.replace((size_t)offset, value.size(), value);
    size_t new_len = cur.size();
    pv.SetString(std::move(cur));  // 必须先取长度，move 后源字符串 size 未定义
    return new_len;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  if (result.status() == OpStatus::OK) {
    rb->BuildInteger(result.value());
  } else if (result.status() == OpStatus::OUT_OF_RANGE) {
    rb->BuildError("string exceeds maximum allowed size");
  } else {
    rb->BuildError(
        "WRONG_TYPE Operation against a key holding the wrong kind "
        "of value");
  }
  co_return;
}

CoroTask CmdGetdel(CommandContext* cmd_cntx, CmdArgList args) {
  args = args.subspan(1);
  CmdArgParser parser{args};
  std::string_view key = parser.Next<std::string_view>();

  auto cb = [key](Transaction* tx, EngineShard* es) -> OpResult<std::string> {
    auto& db_slice = tx->GetDbSlice(es->shard_id());
    auto it_res = db_slice.FindMutable(tx->GetDbContext(), key);
    if (it_res.it.GetInnerIt().owner() == nullptr) {
      return OpStatus::KEY_NOTFOUND;  // 不存在 → null
    }
    std::string old = it_res.it->second.ToString();
    db_slice.DelMutable(tx->GetDbContext(), std::move(it_res));
    return old;
  };

  auto result = co_await cmd::SingleHopT(cb);
  auto* rb = cmd_cntx->rb();
  if (result.status() == OpStatus::OK) {
    rb->BuildBulkString(result.value());
  } else {
    rb->BuildNullBulkString();
  }
  co_return;
}

}  // namespace

void RegisterStringFamily(CommandRegistry* registry) {
  registry->StartFamily();
  *registry
      << CI{"SET", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1}
             .SetHandler(CmdSet)
      << CI{"GET", CO::READONLY, 1, 1}.SetHandler(CmdGet)
      << CI{"MGET", CO::READONLY | CO::IDEMPOTENT, 1, -1}.SetHandler(CmdMGet)
      << CI{"MSET", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, -1}
             .SetInterleavedStep(2)
             .SetHandler(CmdMSet)
      << CI{"APPEND", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1}
             .SetHandler(CmdAppend)
      << CI{"STRLEN", CO::READONLY, 1, 1}.SetHandler(CmdStrlen)
      << CI{"INCR", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1}
             .SetHandler(Incr)
      << CI{"INCRBY", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1}
             .SetHandler(IncrBy)
      << CI{"DECR", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1}
             .SetHandler(Decr)
      << CI{"DECRBY", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1}
             .SetHandler(DecrBy)
      << CI{"SETNX", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1}
             .SetHandler(CmdSetnx)
      << CI{"GETSET", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1}
             .SetHandler(CmdGetset)
      << CI{"GETRANGE", CO::READONLY, 1, 1}.SetHandler(CmdGetrange)
      << CI{"SETRANGE", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1}
             .SetHandler(CmdSetrange)
      << CI{"GETDEL", CO::JOURNALED | CO::DENYOOM | CO::NO_AUTOJOURNAL, 1, 1}
             .SetHandler(CmdGetdel);
}

}  // namespace dfly

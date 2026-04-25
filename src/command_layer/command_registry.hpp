// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/types/span.h>

#include <functional>
#include <optional>

#include "base/function2.hpp"
#include "command_id.hpp"
#include "facade_types.hpp"

struct hdr_histogram;

namespace dfly {

namespace CO {

enum CommandOpt : uint32_t { // 命令选项枚举
    READONLY = 1U << 0, // 命令是只读的，不会修改数据
    DENYOOM = 1U << 4,    // use-memory in redis. // 内存不足时拒绝执行

    VARIADIC_KEYS = 1U << 6,  // arg 2 determines number of keys. Relevant for ZUNIONSTORE, EVAL etc. // 第二个参数决定键的数量（如 ZUNIONSTORE、EVAL 等）

    ADMIN = 1U << 7,  // implies NOSCRIPT, // 管理命令，隐含 NOSCRIPT
    NOSCRIPT = 1U << 8, // 非脚本命令
    BLOCKING = 1U << 9, // 阻塞命令
    HIDDEN = 1U << 10,  // does not show in COMMAND command output // 在 COMMAND 命令输出中不显示

    STORE_LAST_KEY = 1U << 13,  // The command my have a store key as the last argument. // 命令可能有一个存储键作为最后一个参数



    // Allows commands without keys to respect transaction ordering and enables journaling by default
    NO_KEY_TRANSACTIONAL = 1U << 16, // 允许无键命令尊重事务顺序并默认启用日志
    NO_KEY_TX_SPAN_ALL = 1U << 17,  // All shards are active for the no-key-transactional command // 无键事务命令会在所有分片上执行

    // The same callback can be run multiple times without corrupting the result. Used for
    // opportunistic optimizations where inconsistencies can only be detected afterwards.
    IDEMPOTENT = 1U << 18, // 回调可以多次运行而不会损坏结果
};

enum class PubSubKind : uint8_t { 
    REGULAR = 0, // 常规发布订阅
    PATTERN = 1, // 模式发布订阅
    SHARDED = 2 // 分片发布订阅
};

// // Commands controlling any multi command execution.
// // They often need to be handled separately from regular commands in many contexts
// enum class MultiControlKind : uint8_t {
//   EVAL,  // EVAL, EVAL_RO, EVALSHA, EVALSHA_RO // 脚本执行命令
//   EXEC,  // EXEC, MULTI, DISCARD // 事务控制命令
// };

};  // namespace CO

// Per thread vector of command stats. Each entry is {cmd_calls, cmd_latency_agg in usec}.
using CmdCallStats = std::pair<uint64_t, uint64_t>;

class CommandId;
class CommandContext;


class CommandId : public facade::CommandId {
public:
    using CmdArgList = facade::CmdArgList;

    CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key, int8_t last_key,
              std::optional<uint32_t> acl_categories = std::nullopt);

    CommandId(CommandId&& o) = default;

    ~CommandId();

    [[nodiscard]] CommandId Clone(std::string_view name) const;


    using Handler = fu2::function_base<true, true, fu2::capacity_default, false, false,
                                      void(CmdArgList, CommandContext*) const>;
    using ArgValidator = fu2::function_base<true, true, fu2::capacity_default, false, false,
                                            std::optional<facade::ErrorReply>(CmdArgList) const>;

    // Returns the invoke time in usec.
    void Invoke(CmdArgList args, CommandContext* cmd_cntx) const {
        handler_(args, cmd_cntx);
    }



    bool IsReadOnly() const {
        return opt_mask_ & CO::READONLY;
    }



    bool IsBlocking() const {
        return opt_mask_ & CO::BLOCKING;
    }


    template <typename RT> 
    CommandId&& SetAsyncHandler(RT f(CmdArgList, CommandContext*)) && {
        support_async_ = true;
        handler_ = [f](CmdArgList args, CommandContext* cntx) { f(args, cntx); };
        return std::move(*this);
    }

    CommandId&& SetHandler(Handler f, bool async_support = false) && {
        support_async_ |= async_support;
        handler_ = std::move(f);
        return std::move(*this);
    }

    CommandId&& SetValidator(ArgValidator f) && {
        validator_ = std::move(f);
        return std::move(*this);
    }

    std::optional<CO::PubSubKind> PubSubKind() const {
        return kind_pubsub_;
    }

    bool SupportsAsync() const {
        return support_async_;
    }

 private:
    std::optional<CO::PubSubKind> kind_pubsub_;

    bool support_async_{false};// 是否支持异步执行

    Handler handler_;
};

class CommandRegistry {
 public:
  CommandRegistry();

  CommandRegistry& operator<<(CommandId cmd);

  const CommandId* Find(std::string_view cmd) const {
    auto it = cmd_map_.find(cmd);
    return it == cmd_map_.end() ? nullptr : &it->second;
  }

  CommandId* Find(std::string_view cmd) {
    auto it = cmd_map_.find(cmd);
    return it == cmd_map_.end() ? nullptr : &it->second;
  }

  using TraverseCb = std::function<void(std::string_view, const CommandId&)>;

  void Traverse(TraverseCb cb) {
    for (const auto& k_v : cmd_map_) {
      cb(k_v.first, k_v.second);
    }
  }

  void ResetCallStats(unsigned thread_index) {
    for (auto& k_v : cmd_map_) {
      k_v.second.ResetStats(thread_index);
    }
  }

  void MergeCallStats(unsigned thread_index,
                      std::function<void(std::string_view, const CmdCallStats&)> cb) const {
    for (const auto& k_v : cmd_map_) {
      auto src = k_v.second.GetStats(thread_index);
      if (src.first == 0)
        continue;
      cb(k_v.second.name(), src);
    }
  }

  void StartFamily(std::optional<uint32_t> acl_category = std::nullopt);

  std::string_view RenamedOrOriginal(std::string_view orig) const;

  using FamiliesVec = std::vector<std::vector<std::string>>;
  FamiliesVec GetFamilies();

  std::pair<const CommandId*, facade::ParsedArgs> FindExtended(facade::ParsedArgs args) const;

  absl::flat_hash_map<std::string, hdr_histogram*> LatencyMap() const;

 private:
    absl::flat_hash_map<std::string, CommandId> cmd_map_;

    FamiliesVec family_of_commands_;
    size_t bit_index_;
    std::optional<uint32_t> acl_category_;  // category of family currently being built
};

}  // namespace dfly

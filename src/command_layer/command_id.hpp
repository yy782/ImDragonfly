// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "command_layer/cmd_support.hpp"
#include "command_layer/cmn_types.hpp"

namespace dfly {

namespace CO {

enum CommandOpt : uint32_t {
  READONLY = 1U << 0,
  JOURNALED = 1U << 2,  // 记录到 AOF / Journal
  DENYOOM = 1U << 4,    // 内存不足时拒绝执行
  GLOBAL_TRANS = 1U << 12,
  NO_AUTOJOURNAL = 1U << 15,        // 事务内跳过自动 journal
  NO_KEY_TRANSACTIONAL = 1U << 16,  // 无 key 但遵循事务顺序
  IDEMPOTENT = 1U << 18,            // 回调可安全重复执行
};

}  // namespace CO

class CommandContext;
struct CommandSpec;  // 命令目录条目（数据行），定义于 command_registry.hpp

// 命令描述符：元数据 + 键遍历 + 执行入口，单类内聚。
class CommandId {
 public:
  using CmdArgList = ::cmn::CmdArgList;
  using Arg = ::cmn::Arg;

  // handler 为静态成员 / 自由函数指针，返回命令协程（由调用方 co_await）。
  using Handler = cmd::CoroTask (*)(CommandContext*, CmdArgList);

  // 从命令目录数据行构造（表驱动注册的唯一入口）。
  explicit CommandId(const CommandSpec& spec);

  CommandId(CommandId&& o) = default;
  CommandId& operator=(CommandId&& o) = default;

  CommandId(const CommandId&) = delete;
  CommandId& operator=(const CommandId&) = delete;

  std::string_view name() const { return name_; }
  uint32_t opt_mask() const { return opt_mask_; }
  int8_t first_key_pos() const { return first_key_; }
  int8_t last_key_pos() const { return last_key_; }
  int8_t key_step() const { return key_step_; }  // 被事务层消费，名字暂不动

  // 键迭代器：按命令的 first/last/step 规则步进遍历参数中的键
  // （如 MSET 键位于 1, 3, 5...，step 步进即可跳过值位）。
  // 仅需键遍历：值由命令自身按约定解析，不属于键元数据。
  class KeyIterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = Arg;
    using difference_type = std::ptrdiff_t;
    using pointer = const Arg*;
    using reference = Arg;

    reference operator*() const { return args_[pos_]; }
    Arg key() const { return args_[pos_]; }

    KeyIterator& operator++() {
      pos_ += step_;
      if (pos_ >= end_)
        pos_ = end_;
      return *this;
    }
    KeyIterator operator++(int) {
      KeyIterator tmp = *this;
      ++*this;
      return tmp;
    }
    bool operator==(const KeyIterator& o) const {
      return args_.data() == o.args_.data() && pos_ == o.pos_;
    }
    bool operator!=(const KeyIterator& o) const { return !(*this == o); }

   private:
    friend class CommandId;
    KeyIterator(cmn::CmdArgList args, unsigned pos, unsigned end, unsigned step)
        : args_(args), pos_(pos), end_(end), step_(step) {}

    cmn::CmdArgList args_;
    unsigned pos_;
    unsigned end_;
    unsigned step_;
  };

  // Keys() 返回的范围，支持 range-for 与显式迭代器访问。
  class KeyRange {
   public:
    KeyIterator begin() const { return begin_; }
    KeyIterator end() const { return end_; }

   private:
    friend class CommandId;
    KeyRange(KeyIterator b, KeyIterator e) : begin_(b), end_(e) {}
    KeyIterator begin_, end_;
  };

  // 遍历参数中的键：
  //   for (std::string_view k : cid->Keys(args)) { ... }
  KeyRange Keys(cmn::CmdArgList args) const;

  // 执行命令：返回命令协程，由调用方 co_await。
  cmd::CoroTask Invoke(CommandContext* cmd_cntx, CmdArgList args) const {
    return handler_(cmd_cntx, args);
  }

 private:
  std::string name_;
  uint32_t opt_mask_;
  int8_t first_key_;
  int8_t last_key_;
  int8_t key_step_{0};  // 键在参数中的步长（如 MSET 为 2），0 表示默认 1
  Handler handler_{nullptr};
};

}  // namespace dfly

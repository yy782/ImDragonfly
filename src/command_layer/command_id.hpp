#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "detail/common_types.hpp"

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
struct CommandSpec;

class CommandId {
 public:
  using CmdArgList = ::dfly::CmdArgList;
  using Arg = ::dfly::Arg;

  using Handler = cmd::CoroTask (*)(CommandContext*, CmdArgList);

  explicit CommandId(const CommandSpec& spec);

  CommandId(CommandId&& o) = default;
  CommandId& operator=(CommandId&& o) = default;

  CommandId(const CommandId&) = delete;
  CommandId& operator=(const CommandId&) = delete;

  std::string_view name() const { return name_; }
  uint32_t opt_mask() const { return opt_mask_; }
  int8_t first_key_pos() const { return first_key_; }
  int8_t last_key_pos() const { return last_key_; }
  unsigned key_step() const { return unsigned(key_step_); }

  //   for (const auto& kv : cid->Keys(args)) { kv.key, kv.pos }
  struct KeyPos {
    Arg key;
    unsigned pos;  // 键在 full_args 中的下标
  };

  class KeyIterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = KeyPos;
    using difference_type = std::ptrdiff_t;
    using pointer = const KeyPos*;
    using reference = KeyPos;

    reference operator*() const { return {args_[pos_], pos_}; }
    Arg key() const { return args_[pos_]; }

    KeyIterator& operator++() {
      pos_ += step_;
      if (pos_ >= end_) pos_ = end_;
      return *this;
    }
    KeyIterator operator++(int) {
      KeyIterator tmp = *this;
      ++*this;
      return tmp;
    }
    bool operator==(const KeyIterator& o) const {
      return pos_ == o.pos_;  // for遍历直接pos对比就可以了，比较data多余
    }
    bool operator!=(const KeyIterator& o) const { return !(*this == o); }

   private:
    friend class CommandId;
    KeyIterator(::dfly::CmdArgList args, unsigned pos, unsigned end,
                unsigned step)
        : args_(args), pos_(pos), end_(end), step_(step) {}

    ::dfly::CmdArgList args_;
    unsigned pos_;
    unsigned end_;
    unsigned step_;
  };

  class KeyRange {
   public:
    KeyIterator begin() const { return begin_; }
    KeyIterator end() const { return end_; }

   private:
    friend class CommandId;
    KeyRange(KeyIterator b, KeyIterator e) : begin_(b), end_(e) {}
    KeyIterator begin_, end_;
  };

  // 遍历参数中的键（含原始下标与步长）：
  //   for (const auto& kv : cid->Keys(args)) { ... }
  KeyRange Keys(::dfly::CmdArgList args) const;

  cmd::CoroTask Invoke(CommandContext* cmd_cntx, CmdArgList args) const;

 private:
  std::string name_;
  uint32_t opt_mask_;
  int8_t first_key_;
  int8_t last_key_;
  int8_t key_step_{1};
  Handler handler_{nullptr};
};

}  // namespace dfly

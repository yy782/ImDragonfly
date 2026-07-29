// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "base/function.hpp"
#include "command_layer/cmn_types.hpp"
namespace facade {

class CommandId {
 public:
  CommandId(const char* name, uint32_t mask, int8_t first_key, int8_t last_key);

  std::string_view name() const { return name_; }

  uint32_t opt_mask() const { return opt_mask_; }
  int8_t first_key_pos() const { return first_key_; }
  int8_t last_key_pos() const { return last_key_; }

  void SetFamily(size_t fam) { family_ = fam; }

  void SetFlag(uint32_t flag) { opt_mask_ |= flag; }

 protected:
  std::string name_;
  uint32_t opt_mask_;

  int8_t first_key_;
  int8_t last_key_;
  size_t family_;
};

}  // namespace facade

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

class CommandId;
class CommandContext;

class CommandId : public facade::CommandId {
 public:
  using CmdArgList = ::cmn::CmdArgList;

  using Handler =
      base::function_base<true, true, fu2::capacity_default, false, false,
                          void(CommandContext*, CmdArgList) const>;

  CommandId(const char* name, uint32_t mask, int8_t first_key, int8_t last_key);

  CommandId(CommandId&& o) = default;

  void Invoke(CommandContext* cmd_cntx, CmdArgList args) const {
    handler_(cmd_cntx, args);
  }

  // bool IsTransactional() const;

  int8_t interleaved_step() const { return interleave_step_; }

  CommandId&& SetHandler(Handler f) && {
    handler_ = std::move(f);
    return std::move(*this);
  }

  CommandId&& SetInterleavedStep(int8_t step) && {
    interleave_step_ = step;
    return std::move(*this);
  }

 private:
  int8_t interleave_step_{0};
  Handler handler_;
};

}  // namespace dfly

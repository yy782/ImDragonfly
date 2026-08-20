// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "command_layer/command_id.hpp"

#include <glog/logging.h>

#include "command_layer/command_registry.hpp"  // CommandSpec 完整定义

namespace dfly {

// 从命令目录数据行构造：表驱动注册的唯一入口，元数据 + 步长 + 执行器一次到位。
CommandId::CommandId(const CommandSpec& spec)
    : name_(spec.name),
      opt_mask_(spec.mask),
      first_key_(spec.first_key),
      last_key_(spec.last_key),
      key_step_(spec.step),
      handler_(spec.handler) {}

// 按 first/last/step 规则（Redis 约定，last 为 -1 表示"到参数末尾"）
// 遍历参数中的键；全局事务 / 无 key 事务命令返回空区间。
CommandId::KeyRange CommandId::Keys(cmn::CmdArgList args) const {
  if (opt_mask_ & (CO::GLOBAL_TRANS | CO::NO_KEY_TRANSACTIONAL))
    return KeyRange{KeyIterator(args, 0, 0, 1), KeyIterator(args, 0, 0, 1)};

  if (first_key_ <= 0) {
    LOG(FATAL) << "TBD: Not supported " << name_;
  }
  unsigned start = unsigned(first_key_);
  unsigned end = last_key_ > 0 ? unsigned(last_key_ + 1)
                               : unsigned(int(args.size()) + last_key_ + 1);
  unsigned step = key_step_ ? unsigned(key_step_) : 1;

  return KeyRange{KeyIterator(args, start, end, step),
                  KeyIterator(args, end, end, step)};
}

}  // namespace dfly

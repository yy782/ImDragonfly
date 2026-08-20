// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "command_registry.hpp"

#include <glog/logging.h>

namespace dfly {

CommandRegistry::CommandRegistry() {}

// 表驱动注册：各模块把自己的 constexpr 命令目录（CommandSpec 表）交进来，
// 这里统一构建成运行期查找结构。目录内重名已被编译期 CheckUniqueNames 拦截。
void CommandRegistry::Register(std::span<const CommandSpec> specs) {
  for (const CommandSpec& s : specs) {
    auto [it, inserted] = cmd_map_.emplace(s.name, CommandId{s});
    (void)it;
    CHECK(inserted) << "duplicate command: " << s.name;
  }
}

}  // namespace dfly

// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>

#include "command_layer/cmn_types.hpp"
#include "command_layer/command_id.hpp"

namespace dfly {

// 命令目录条目（纯数据）：命令的 constexpr 声明，表驱动注册的载体。
// 字段顺序即初始化列表顺序；step 仅在非默认步长时给出（如 MSET 为 2）。
struct CommandSpec {
  const char* name;
  uint32_t mask;
  int8_t first_key;
  int8_t last_key;
  CommandId::Handler handler;
  int8_t step{0};
};

// 编译期校验：命令目录内命令名不得重复。
constexpr bool CheckUniqueNames(std::span<const CommandSpec> specs) {
  for (size_t i = 0; i < specs.size(); ++i)
    for (size_t j = i + 1; j < specs.size(); ++j)
      if (std::string_view(specs[i].name) == std::string_view(specs[j].name))
        return false;
  return true;
}

// 透明 hash：同时支持 std::string 与 std::string_view，并开启 unordered_map
// 的异构查找，使 Find(std::string_view) 无需再构造临时 std::string。
// std::hash<std::string_view> 与 std::hash<std::string> 对相同字符序列
// 产生一致哈希（C++17 起标准保证），因此查找结果与原实现完全等价。
struct TransparentHash {
  using is_transparent = void;
  size_t operator()(std::string_view sv) const noexcept {
    return std::hash<std::string_view>{}(sv);
  }
};

// 命令注册表：从各模块的命令目录（constexpr CommandSpec 表）构建查找结构。
// 注册完成后的生命周期内只读，Find 即按名返回执行器的"简单工厂"。
class CommandRegistry {
 public:
  CommandRegistry();

  // 表驱动注册：追加一组命令目录，可多次调用（每个模块调一次）。
  void Register(std::span<const CommandSpec> specs);

  const CommandId* Find(std::string_view cmd) const {
    auto it = cmd_map_.find(cmd);
    return it == cmd_map_.end() ? nullptr : &it->second;
  }

  CommandId* Find(std::string_view cmd) {
    auto it = cmd_map_.find(cmd);
    return it == cmd_map_.end() ? nullptr : &it->second;
  }

  size_t size() const { return cmd_map_.size(); }

 private:
  using CmdMap = std::unordered_map<std::string, CommandId, TransparentHash,
                                    std::equal_to<>>;
  CmdMap cmd_map_;
};

}  // namespace dfly

#pragma once

#include <algorithm>
#include <ranges>
#include <span>
#include <string_view>
#include <tuple>

namespace cmn {

using Arg = std::string_view;  // 单个命令参数

using ArgSlice = std::span<const Arg>;  // 这两个是等价的

using CmdArgList = std::span<const Arg>;

}  // namespace cmn

namespace dfly {

// 命令参数中键的位置规则（Redis first/last/step 约定）。
// 本身可作为迭代器使用：按 step 步进遍历 [start, end) 的下标。
struct KeyIndex {
  KeyIndex(unsigned start = 0, unsigned end = 0, unsigned step = 1)
      : start(start), end(end), step(step) {}

  using iterator_category = std::forward_iterator_tag;
  using value_type = unsigned;
  using difference_type = std::ptrdiff_t;
  using pointer = value_type;
  using reference = value_type;

  unsigned operator*() const { return start; }
  KeyIndex& operator++() {
    start = std::min(end, start + step);
    return *this;
  }
  bool operator!=(const KeyIndex& ki) const {
    return std::tie(start, end, step) != std::tie(ki.start, ki.end, ki.step);
  }

  unsigned NumArgs() const { return (end - start + step - 1) / step; }

  // 键下标序列：[start, end) 内以 step 步进的下标。
  auto Range() const {
    unsigned s = start, st = step;  // 由ASAN报告，2026.7.30 -- 1 修改
    return std::views::iota(0u, NumArgs()) |
           std::views::transform([s, st](unsigned i) { return s + i * st; });
  }

  // 键字符串序列：将下标映射为参数中的实际键。
  auto Range(const ::cmn::ArgSlice& args) const {
    unsigned s = start, st = step;  // 由ASAN报告，2026.7.30 -- 1 修改
    return std::views::iota(0u, NumArgs()) |
           std::views::transform([s, st](unsigned i) { return s + i * st; }) |
           std::views::transform([args](unsigned idx) { return args[idx]; });
  }

 public:
  unsigned start, end, step;
};

}  // namespace dfly

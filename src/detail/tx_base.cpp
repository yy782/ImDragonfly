#include "tx_base.hpp"

#include <functional>
#include <string_view>
namespace dfly {

unsigned KeyIndex::operator*() const { return start; }

KeyIndex& KeyIndex::operator++() {
  start = std::min(end, start + step);
  return *this;
}

bool KeyIndex::operator!=(const KeyIndex& ki) const {
  return std::tie(start, end, step) != std::tie(ki.start, ki.end, ki.step);
}

LockTag::LockTag(std::string_view key) {
  str_ = key;  // 可能有问题，看源码
}

LockFp LockTag::Fingerprint() const {
  return std::hash<std::string_view>{}(str_);
}

}  // namespace dfly
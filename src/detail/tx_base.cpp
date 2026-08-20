#include "tx_base.hpp"

#include <functional>
#include <string_view>
namespace dfly {

LockTag::LockTag(std::string_view key) {
  str_ = key;  // 可能有问题，看源码
}

LockFp LockTag::Fingerprint() const {
  return std::hash<std::string_view>{}(str_);
}

}  // namespace dfly
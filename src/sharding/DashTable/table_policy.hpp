#pragma once

#include "DashTable/dash_table.hpp"


namespace dfly {

namespace detail {

using PrimeKey = CompactKey;
using PrimeValue = CompactValue;

struct PrimeTablePolicy {
  enum : uint8_t { kSlotNum = 14, kBucketNum = 56 };

  static constexpr bool kUseVersion = true;

  static uint64_t HashFn(const PrimeKey& s) {
    return s.HashCode();
  }

  static uint64_t HashFn(std::string_view u) {
    return CompactObj::HashCode(u);
  }

  static void DestroyKey(PrimeKey& cs) {
    cs.Reset();
  }

  static void DestroyValue(PrimeValue& o) {
    o.Reset();
  }

  static bool Equal(const PrimeKey& s1, std::string_view s2) {
    return s1 == s2;
  }
};


}
}
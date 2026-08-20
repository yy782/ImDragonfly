#pragma once

// ============================================================================
// table_policy.hpp — PrimeTablePolicy：DashTable 的键/值策略（重写版）
//
// 配合重写后的 dash_table.hpp 使用。契约变更点：
//   * 新增 kStashNum（段尾溢出桶数，缺省 4）；
//   * Equal 同时支持 (Key, Key) 与 (Key, string_view)；
//   * 新增恢复策略所需的序列化方法：
//     WriteKey/WriteValue/ReadKey/ReadValue —— 按 tag 保留类型
//     （INT / STR / TTL_STR / EMPTY）；ROBJ（list/hash/set/zset 等）
//     无法通用序列化，返回 false 提示上层改用专用序列化。
// ============================================================================

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include "compact_obj.hpp"
#include "dash_table.hpp"

namespace dfly {

namespace detail {

using PrimeKey = CompactKey;
using PrimeValue = CompactValue;

struct PrimeTablePolicy {
  enum : uint8_t { kSlotNum = 14, kBucketNum = 56, kStashNum = 4 };

  static uint64_t HashFn(const PrimeKey& s) { return s.HashCode(); }

  static uint64_t HashFn(std::string_view u) { return CompactObj::HashCode(u); }

  static void DestroyKey(PrimeKey& /*cs*/) {}

  static void DestroyValue(PrimeValue& /*o*/) {}

  static bool Equal(const PrimeKey& s1, std::string_view s2) {
    return s1 == s2;
  }

  static bool Equal(const PrimeKey& s1, const PrimeKey& s2) {
    return static_cast<const CompactObj&>(s1) ==
           static_cast<const CompactObj&>(s2);
  }

  // ---- 恢复策略：序列化契约 ----

  // 记录格式：[tag:u8][payload]
  //   tag 0 = INT:   [ival:i64]
  //   tag 1 = STR:   [len:u32][bytes]
  //   tag 2 = TTL:   [exp_ms:u64][len:u32][bytes]
  //   tag 3 = EMPTY: []
  // ROBJ 类型不支持通用序列化，返回 false。
 private:
  static constexpr uint8_t kRecInt = 0;
  static constexpr uint8_t kRecStr = 1;
  static constexpr uint8_t kRecTtl = 2;
  static constexpr uint8_t kRecEmpty = 3;

  template <typename S>
  static bool WriteObj(S& sink, const CompactObj& o) {
    if (o.IsRobj()) return false;
    if (o.IsEmpty()) {
      const uint8_t t = kRecEmpty;
      return sink.write(&t, 1);
    }
    if (o.IsInt()) {
      const uint8_t t = kRecInt;
      const int64_t v = o.AsInt();
      return sink.write(&t, 1) && sink.write(&v, sizeof(v));
    }
    if (o.IsTtlStr()) {
      const uint8_t t = kRecTtl;
      const auto& ts = o.AsTtl();
      const uint64_t exp = ts.exp_ms;
      const std::string_view sv = ts.view();
      const uint32_t len = static_cast<uint32_t>(sv.size());
      return sink.write(&t, 1) && sink.write(&exp, sizeof(exp)) &&
             sink.write(&len, sizeof(len)) && sink.write(sv.data(), len);
    }
    const uint8_t t = kRecStr;
    const std::string_view sv = o.AsStr();
    const uint32_t len = static_cast<uint32_t>(sv.size());
    return sink.write(&t, 1) && sink.write(&len, sizeof(len)) &&
           sink.write(sv.data(), len);
  }

  template <typename S>
  static bool ReadObj(S& src, CompactObj* o) {
    uint8_t t = 0;
    if (!src.read(&t, 1)) return false;
    switch (t) {
      case kRecEmpty:
        o->SetString(std::string_view{});
        return true;
      case kRecInt: {
        int64_t v = 0;
        if (!src.read(&v, sizeof(v))) return false;
        o->SetInt(v);
        return true;
      }
      case kRecStr: {
        uint32_t len = 0;
        if (!src.read(&len, sizeof(len))) return false;
        std::string buf(len, '\0');
        if (len != 0 && !src.read(buf.data(), len)) return false;
        o->SetString(std::move(buf));
        return true;
      }
      case kRecTtl: {
        uint64_t exp = 0;
        if (!src.read(&exp, sizeof(exp))) return false;
        uint32_t len = 0;
        if (!src.read(&len, sizeof(len))) return false;
        std::string buf(len, '\0');
        if (len != 0 && !src.read(buf.data(), len)) return false;
        o->SetTtlStr(TtlString{std::move(buf), exp});
        return true;
      }
      default:
        return false;
    }
  }

 public:
  template <typename S>
  static bool WriteKey(S& sink, const PrimeKey& k) {
    return WriteObj(sink, k);
  }
  template <typename S>
  static bool WriteValue(S& sink, const PrimeValue& v) {
    return WriteObj(sink, v);
  }
  template <typename S>
  static bool ReadKey(S& src, PrimeKey* k) {
    return ReadObj(src, k);
  }
  template <typename S>
  static bool ReadValue(S& src, PrimeValue* v) {
    return ReadObj(src, v);
  }
};

using PrimeTable = dash::DashTable<PrimeKey, PrimeValue, PrimeTablePolicy>;

}  // namespace detail
}  // namespace dfly

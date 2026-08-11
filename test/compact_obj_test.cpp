#include "sharding/DashTable/compact_obj.hpp"

#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "redis/redis_aux.hpp"

using namespace dfly;

// ============================================================================
// Empty / Default
// ============================================================================

TEST(CompactObjTest, DefaultKeyIsEmpty) {
  CompactObj obj(true);
  EXPECT_TRUE(obj.IsEmpty());
  EXPECT_EQ(obj.GetTag(), CompactObj::EMPTY);
}

TEST(CompactObjTest, DefaultValueIsEmpty) {
  CompactObj obj(false);
  EXPECT_TRUE(obj.IsEmpty());
}

// ============================================================================
// SetString / IsStr / AsStr
// ============================================================================

TEST(CompactObjTest, SetStringShort) {
  CompactObj obj(true);
  obj.SetString(std::string_view("hello"));
  EXPECT_TRUE(obj.IsStr());
  EXPECT_FALSE(obj.IsInt());
  EXPECT_EQ(obj.AsStr(), "hello");
}

TEST(CompactObjTest, SetStringLong) {
  CompactObj obj(true);
  std::string long_str(512, 'x');
  obj.SetString(std::string_view(long_str));
  EXPECT_TRUE(obj.IsStr());
  EXPECT_EQ(obj.AsStr(), long_str);
}

TEST(CompactObjTest, SetStringMove) {
  CompactObj obj(true);
  std::string s = "moved content";
  obj.SetString(std::move(s));
  EXPECT_TRUE(obj.IsStr());
  EXPECT_EQ(obj.AsStr(), "moved content");
}

TEST(CompactObjTest, SetStringOverwrite) {
  CompactObj obj(true);
  obj.SetString(std::string_view("first"));
  obj.SetString(std::string_view("second"));
  EXPECT_EQ(obj.AsStr(), "second");
}

TEST(CompactObjTest, SetEmptyString) {
  CompactObj obj(true);
  obj.SetString(std::string_view(""));
  EXPECT_TRUE(obj.IsStr());
  EXPECT_EQ(obj.AsStr(), "");
}

// ============================================================================
// SetInt / IsInt / AsInt
// ============================================================================

TEST(CompactObjTest, SetIntPositive) {
  CompactObj obj(false);
  obj.SetInt(42);
  EXPECT_TRUE(obj.IsInt());
  EXPECT_EQ(obj.AsInt(), 42);
}

TEST(CompactObjTest, SetIntNegative) {
  CompactObj obj(false);
  obj.SetInt(-100);
  EXPECT_EQ(obj.AsInt(), -100);
}

TEST(CompactObjTest, SetIntZero) {
  CompactObj obj(false);
  obj.SetInt(0);
  EXPECT_EQ(obj.AsInt(), 0);
}

TEST(CompactObjTest, SetIntLarge) {
  CompactObj obj(false);
  obj.SetInt(INT64_MAX);
  EXPECT_EQ(obj.AsInt(), INT64_MAX);
}

TEST(CompactObjTest, SetIntOverwriteString) {
  CompactObj obj(true);
  obj.SetString(std::string_view("was a string"));
  obj.SetInt(123);
  EXPECT_TRUE(obj.IsInt());
  EXPECT_EQ(obj.AsInt(), 123);
}

// ============================================================================
// Type transitions
// ============================================================================

TEST(CompactObjTest, SwitchIntToString) {
  CompactObj obj(false);
  obj.SetInt(100);
  EXPECT_TRUE(obj.IsInt());

  obj.SetString(std::string_view("now string"));
  EXPECT_TRUE(obj.IsStr());
  EXPECT_EQ(obj.AsStr(), "now string");
}

TEST(CompactObjTest, SwitchStringToInt) {
  CompactObj obj(true);
  obj.SetString(std::string_view("was string"));
  obj.SetInt(42);
  EXPECT_TRUE(obj.IsInt());
  EXPECT_EQ(obj.AsInt(), 42);
}

// ============================================================================
// ToString / GetSlice
// ============================================================================

TEST(CompactObjTest, ToStringFromInt) {
  CompactObj obj(false);
  obj.SetInt(123);
  EXPECT_EQ(obj.ToString(), "123");
}

TEST(CompactObjTest, ToStringFromStr) {
  CompactObj obj(true);
  obj.SetString(std::string_view("hello world"));
  EXPECT_EQ(obj.ToString(), "hello world");
}

TEST(CompactObjTest, ToStringEmpty) {
  CompactObj obj(true);
  EXPECT_EQ(obj.ToString(), "");
}

TEST(CompactObjTest, GetSliceFromInt) {
  CompactObj obj(false);
  obj.SetInt(999);
  std::string scratch;
  EXPECT_EQ(obj.GetSlice(&scratch), "999");
}

TEST(CompactObjTest, GetSliceFromStr) {
  CompactObj obj(true);
  obj.SetString(std::string_view("direct slice"));
  std::string scratch;
  EXPECT_EQ(obj.GetSlice(&scratch), "direct slice");
}

// ============================================================================
// Equality
// ============================================================================

TEST(CompactObjTest, EqualSameString) {
  CompactObj a(true);
  a.SetString(std::string_view("abc"));
  CompactObj b(false);
  b.SetString(std::string_view("abc"));
  EXPECT_TRUE(a == b);
}

TEST(CompactObjTest, EqualSameInt) {
  CompactObj a(false);
  a.SetInt(5);
  CompactObj b(false);
  b.SetInt(5);
  EXPECT_TRUE(a == b);
}

TEST(CompactObjTest, NotEqualDifferentTag) {
  CompactObj a(false);
  a.SetInt(0);
  CompactObj b(false);
  b.SetString(std::string_view("0"));
  EXPECT_FALSE(a == b);
}

TEST(CompactObjTest, NotEqualDifferentString) {
  CompactObj a(true);
  a.SetString(std::string_view("hello"));
  CompactObj b(true);
  b.SetString(std::string_view("world"));
  EXPECT_FALSE(a == b);
}

// ============================================================================
// Move
// ============================================================================

TEST(CompactObjTest, MoveConstructor) {
  CompactObj a(true);
  a.SetString(std::string_view("source"));
  CompactObj b(std::move(a));
  EXPECT_TRUE(a.IsEmpty());
  EXPECT_TRUE(b.IsStr());
  EXPECT_EQ(b.AsStr(), "source");
}

TEST(CompactObjTest, MoveAssignment) {
  CompactObj a(true);
  a.SetString(std::string_view("source"));
  CompactObj b(false);
  b = std::move(a);
  EXPECT_TRUE(a.IsEmpty());
  EXPECT_TRUE(b.IsStr());
  EXPECT_EQ(b.AsStr(), "source");
}

// ============================================================================
// Int Constructor
// ============================================================================

TEST(CompactObjTest, ConstructFromInt) {
  CompactObj obj(int64_t{88});
  EXPECT_TRUE(obj.IsInt());
  EXPECT_EQ(obj.AsInt(), 88);
}

// ============================================================================
// TtlStr
// ============================================================================

TEST(CompactObjTest, SetTtlStr) {
  CompactObj obj(true);
  TtlString ts;
  ts.val = "ttl_value";
  ts.exp_ms = 1700000000000ULL;
  obj.SetTtlStr(ts);
  EXPECT_TRUE(obj.IsTtlStr());
  EXPECT_EQ(obj.AsTtl().val, "ttl_value");
  EXPECT_EQ(obj.AsTtl().exp_ms, 1700000000000ULL);
}

TEST(CompactObjTest, ToStringFromTtlStr) {
  CompactObj obj(true);
  TtlString ts;
  ts.val = "ttl_string";
  ts.exp_ms = 1000;
  obj.SetTtlStr(ts);
  EXPECT_EQ(obj.ToString(), "ttl_string");
}

// ============================================================================
// ObjType
// ============================================================================

TEST(CompactObjTest, ObjTypeInt) {
  CompactObj obj(false);
  obj.SetInt(10);
  EXPECT_EQ(obj.ObjType(), OBJ_STRING);
}

TEST(CompactObjTest, ObjTypeStr) {
  CompactObj obj(true);
  obj.SetString(std::string_view("x"));
  EXPECT_EQ(obj.ObjType(), OBJ_STRING);
}

TEST(CompactObjTest, ObjTypeEmpty) {
  CompactObj obj(true);
  EXPECT_EQ(obj.ObjType(), kInvalidCompactObjType);
}

// ============================================================================
// HashCode
// ============================================================================

TEST(CompactObjTest, HashCodeSameForEqual) {
  CompactObj a(true);
  a.SetString(std::string_view("same"));
  CompactObj b(false);
  b.SetString(std::string_view("same"));
  EXPECT_EQ(a.HashCode(), b.HashCode());
}

TEST(CompactObjTest, HashCodeStatic) {
  uint64_t h1 = CompactObj::HashCode(std::string_view("hello"));
  uint64_t h2 = CompactObj::HashCode(std::string_view("hello"));
  EXPECT_EQ(h1, h2);
}

// ============================================================================
// CompactKey
// ============================================================================

TEST(CompactKeyTest, DefaultConstruct) {
  CompactKey key;
  EXPECT_TRUE(key.IsEmpty());
}

TEST(CompactKeyTest, ConstructFromStringView) {
  CompactKey key("mykey");
  EXPECT_TRUE(key.IsStr());
  EXPECT_EQ(key.AsStr(), "mykey");
}

TEST(CompactKeyTest, EqualityStringView) {
  CompactKey key("abc");
  EXPECT_TRUE(key == std::string_view("abc"));
  EXPECT_FALSE(key != std::string_view("abc"));
  EXPECT_FALSE(key == std::string_view("xyz"));
  EXPECT_TRUE(key != std::string_view("xyz"));
}

TEST(CompactKeyTest, StringViewEquality) {
  CompactKey key("hello");
  EXPECT_TRUE(std::string_view("hello") == key);
  EXPECT_FALSE(std::string_view("world") == key);
}

TEST(CompactKeyTest, AssignmentOperator) {
  CompactKey key("old");
  key = std::string_view("new_key");
  EXPECT_EQ(key.AsStr(), "new_key");
}

TEST(CompactKeyTest, ExpireTime) {
  CompactKey key("expire_key");
  EXPECT_FALSE(key.HasExpire());
  EXPECT_EQ(key.GetExpireTime(), 0u);

  key.SetExpireTime(1700000000000ULL);
  EXPECT_TRUE(key.HasExpire());
  EXPECT_EQ(key.GetExpireTime(), 1700000000000ULL);

  bool cleared = key.ClearExpireTime();
  EXPECT_TRUE(cleared);
  EXPECT_FALSE(key.HasExpire());
  EXPECT_TRUE(key.IsStr());
  EXPECT_EQ(key.AsStr(), "expire_key");
}

TEST(CompactKeyTest, ClearExpireWhenNoExpire) {
  CompactKey key("no_expire");
  bool cleared = key.ClearExpireTime();
  EXPECT_FALSE(cleared);
}

// ============================================================================
// HashCode via friend
// ============================================================================

namespace std {
template <>
struct hash<CompactKey> {
  size_t operator()(const CompactKey& k) const { return (size_t)k.HashCode(); }
};
}  // namespace std

TEST(CompactKeyTest, HashableForKey) {
  CompactKey a("apple");
  CompactKey b("apple");
  CompactKey c("banana");
  EXPECT_EQ(std::hash<CompactKey>{}(a), std::hash<CompactKey>{}(b));
  EXPECT_NE(std::hash<CompactKey>{}(a), std::hash<CompactKey>{}(c));
}

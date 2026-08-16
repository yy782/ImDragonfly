#pragma once

#include <glog/logging.h>
#include <mimalloc.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>

namespace dfly {

// 每个 shard 线程独立的 mimalloc heap（定义见 sharding/engine_shard.cpp）。
// 与 compact_obj.hpp 中的声明一致，用于长字符串的堆外分配；
// 非 shard 线程下为 nullptr，mi_heap_malloc 会自动回退到默认 heap。
extern thread_local mi_heap_t* data_heap;

// SmallString：面向 DashTable 槽位存储的紧凑字符串类型。
//
// 采用 Small Buffer Optimization（SBO）：
//   * 长度 <= kInlineCap 的短字符串完全内联存储于对象内部，零堆分配；
//   * 长度 >  kInlineCap 的长字符串通过专属分配器（data_heap）在堆上分配。
//
// 存储模式由 size_ 隐式决定：size_ <= kInlineCap 即内联，否则为堆外，
// 因此无需额外 tag 位，对象大小固定为 24 字节（8 字节 size_ + 16 字节联合体）。
class SmallString {
 public:
  // 内联容量：15 字节（与主流 std::string 的 SBO 实现一致）。
  // 内联缓冲共 16 字节 = 15 字节数据 + 1 字节尾随 NUL，保证 C 风格安全。
  static constexpr size_t kInlineCap = 15;

  // 长字符串从内联切换到堆外时的最小堆容量，避免小块频繁重分配。
  static constexpr size_t kMinHeapCap = 32;

  SmallString() = default;

  explicit SmallString(std::string_view sv) { assign(sv); }

  SmallString(const SmallString& o) { assign(o.view()); }
  SmallString& operator=(const SmallString& o) {
    if (this != &o) assign(o.view());
    return *this;
  }

  SmallString(SmallString&& o) noexcept { MoveFrom(std::move(o)); }
  SmallString& operator=(SmallString&& o) noexcept {
    if (this != &o) {
      Reset();
      MoveFrom(std::move(o));
    }
    return *this;
  }

  ~SmallString() { Reset(); }

  void assign(std::string_view sv);
  void clear() { Reset(); }

  bool empty() const { return size_ == 0; }
  size_t size() const { return size_; }
  size_t length() const { return size_; }

  // 是否内联存储（无任何堆分配）。
  bool is_inline() const { return size_ <= kInlineCap; }

  // 堆外已分配容量（不含尾随 NUL）；内联时为 0。
  size_t capacity() const { return is_inline() ? 0 : u_.heap_.cap_; }

  char* data();
  const char* data() const;

  char* c_str() { return data(); }
  const char* c_str() const { return data(); }

  std::string_view view() const { return {data(), size_}; }

  std::string to_string() const {
    std::string_view v = view();
    return std::string(v.data(), v.size());
  }

  bool operator==(std::string_view sv) const { return view() == sv; }
  bool operator==(const SmallString& o) const { return view() == o.view(); }
  bool operator!=(std::string_view sv) const { return !(*this == sv); }
  bool operator!=(const SmallString& o) const { return !(*this == o); }

  // 与 CompactObj::HashCode 保持一致的字符串哈希。
  uint64_t HashCode() const;

  void swap(SmallString& o) noexcept;

 private:
  void Reset();
  void MoveFrom(SmallString&& o) noexcept;

  // 由当前 size_ 起步，为至少容纳 need 字节计算新的堆容量（2 倍增长）。
  static size_t GrowCapacity(size_t need) {
    size_t cap = kMinHeapCap;
    while (cap < need) cap *= 2;
    return cap;
  }

  size_t size_ =
      0;  // 总长度；同时隐含存储模式（<= kInlineCap 内联，否则堆外）。

  union U {
    struct {
      char* ptr_;   // 堆外指针（含尾随 NUL）
      size_t cap_;  // 堆外已分配容量（不含尾随 NUL）
    } heap_;
    char inline_[kInlineCap + 1];  // 内联缓冲，末字节恒为 NUL

    U() : inline_{} {}
    ~U() {}
  } u_;
};

static_assert(sizeof(SmallString) == 24, "SmallString must stay 24 bytes");

inline void swap(SmallString& a, SmallString& b) noexcept { a.swap(b); }

}  // namespace dfly

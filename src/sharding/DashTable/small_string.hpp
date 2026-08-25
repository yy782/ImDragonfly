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

extern thread_local mi_heap_t* data_heap;

class SmallString {
 public:
  static constexpr size_t kInlineCap = 15;
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
  bool is_inline() const { return size_ <= kInlineCap; }
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

  uint64_t HashCode() const;

  void swap(SmallString& o) noexcept;

 private:
  void Reset();
  void MoveFrom(SmallString&& o) noexcept;

  static size_t GrowCapacity(size_t need) {
    size_t cap = kMinHeapCap;
    while (cap < need) cap *= 2;
    return cap;
  }

  size_t size_ = 0;

  union U {
    struct {
      char* ptr_;
      size_t cap_;
    } heap_;
    char inline_[kInlineCap + 1];

    U() : inline_{} {}
    ~U() {}
  } u_;
};

static_assert(sizeof(SmallString) == 24, "SmallString must stay 24 bytes");

inline void swap(SmallString& a, SmallString& b) noexcept { a.swap(b); }

}  // namespace dfly

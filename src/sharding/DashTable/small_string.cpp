#include "small_string.hpp"

#include <functional>

namespace dfly {

char* SmallString::data() { return is_inline() ? u_.inline_ : u_.heap_.ptr_; }

const char* SmallString::data() const {
  return is_inline() ? u_.inline_ : u_.heap_.ptr_;
}

void SmallString::assign(std::string_view sv) {
  const size_t n = sv.size();

  if (n <= kInlineCap) {
    // 内联路径：先释放可能持有的堆外内存。
    if (!is_inline()) {
      mi_free(u_.heap_.ptr_);
    }
    size_ = n;
    if (n != 0) memcpy(u_.inline_, sv.data(), n);
    u_.inline_[n] = '\0';
    return;
  }

  // 堆外路径：容量不足时（重新）分配，否则原地复用。
  if (is_inline() || u_.heap_.cap_ < n) {
    const size_t new_cap = GrowCapacity(n);
    if (is_inline()) {
      u_.heap_.ptr_ =
          static_cast<char*>(mi_heap_malloc(data_heap, new_cap + 1));
    } else {
      u_.heap_.ptr_ = static_cast<char*>(
          mi_heap_realloc(data_heap, u_.heap_.ptr_, new_cap + 1));
    }
    u_.heap_.cap_ = new_cap;
  }

  size_ = n;
  memcpy(u_.heap_.ptr_, sv.data(), n);
  u_.heap_.ptr_[n] = '\0';
}

uint64_t SmallString::HashCode() const {
  return std::hash<std::string_view>{}(view());
}

void SmallString::swap(SmallString& o) noexcept {
  char tmp[sizeof(SmallString)];
  memcpy(tmp, this, sizeof(SmallString));
  memcpy(this, &o, sizeof(SmallString));
  memcpy(&o, tmp, sizeof(SmallString));
}

void SmallString::Reset() {
  if (!is_inline()) {
    mi_free(u_.heap_.ptr_);
  }
  size_ = 0;
  u_.inline_[0] = '\0';
}

void SmallString::MoveFrom(SmallString&& o) noexcept {
  if (o.is_inline()) {
    size_ = o.size_;
    memcpy(u_.inline_, o.u_.inline_, o.size_ + 1);
  } else {
    size_ = o.size_;
    u_.heap_.ptr_ = o.u_.heap_.ptr_;
    u_.heap_.cap_ = o.u_.heap_.cap_;
  }

  o.size_ = 0;
  o.u_.inline_[0] = '\0';
}

}  // namespace dfly

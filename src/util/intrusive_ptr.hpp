

#pragma once

#include <atomic>
#include <cstddef>
#include <type_traits>
#include <utility>

namespace util {

template <class T>
class intrusive_ptr;

struct thread_unsafe_counter {
  using type = unsigned int;
};
struct thread_safe_counter {
  using type = std::atomic<unsigned int>;
};

namespace detail {

template <class C>
void counter_add(C& c, unsigned int n) noexcept {
  if constexpr (std::is_same_v<C, std::atomic<unsigned int>>) {
    // 调用 add_ref 的线程必然已经持有至少 1 个引用（通过拷贝构造、
    // reset(p) 或从已有 ptr 复制而来）。这意味着对象数据早已通过"上一次
    // 建立引用"时的同步（release/acquire 配对，或构造时单线程的 program
    // order）抵达本核的 cache，本核看到对象内存是完整的。
    //
    // 增加 ref_count 只是把计数从 N 变成 N+1，不读取对象数据、不建立
    // 新的 happens-before 关系。因此这里只需要"原子的算术增量"，
    // 不需要任何内存屏障。
    c.fetch_add(n, std::memory_order_relaxed);
  } else {
    c += n;
  }
}

template <class C>
bool counter_release(C& c) noexcept {
  if constexpr (std::is_same_v<C, std::atomic<unsigned int>>) {
    bool last = c.fetch_sub(1, std::memory_order_release) == 1;
    if (last) std::atomic_thread_fence(std::memory_order_acquire);
    return last;
  } else {
    return --c == 0;
  }
}

template <class C>
unsigned int counter_load(const C& c) noexcept {
  if constexpr (std::is_same_v<C, std::atomic<unsigned int>>) {
    return c.load(std::memory_order_relaxed);
  } else {
    return c;
  }
}

}  // namespace detail

template <class T, class Counter = thread_unsafe_counter>
class intrusive_ref_counter {
 public:
  using counter_type = typename Counter::type;

  unsigned int use_count() const noexcept {
    return detail::counter_load(ref_count_);
  }

  intrusive_ptr<T> intrusive_ptr_from_this() noexcept;
  intrusive_ptr<const T> intrusive_ptr_from_this() const noexcept;

 protected:
  intrusive_ref_counter() noexcept = default;
  intrusive_ref_counter(const intrusive_ref_counter&) noexcept = default;
  intrusive_ref_counter& operator=(const intrusive_ref_counter&) noexcept =
      default;
  ~intrusive_ref_counter() = default;

 private:
  // 非模板 hidden friend：形参为基类指针，允许派生类指针（T*）通过 ADL +
  // 隐式转换找到；同时函数体在类内定义，避免函数模板无法推导派生类的困境。
  friend void intrusive_ptr_add_ref(const intrusive_ref_counter* p) noexcept {
    detail::counter_add(p->ref_count_, 1);
  }
  friend void intrusive_ptr_release(const intrusive_ref_counter* p) noexcept {
    if (detail::counter_release(p->ref_count_)) {
      delete static_cast<const T*>(p);
    }
  }

  mutable counter_type ref_count_{0};
};

// ═══════════════════════════════════════════════════════════════════════════
// intrusive_ptr<T>
// ═══════════════════════════════════════════════════════════════════════════
template <class T>
class intrusive_ptr {
  template <class>
  friend class intrusive_ptr;

 public:
  using element_type = T;
  using pointer = T*;

  constexpr intrusive_ptr() noexcept = default;
  constexpr intrusive_ptr(std::nullptr_t) noexcept {}
  explicit intrusive_ptr(T* p, bool add_ref = true) noexcept : ptr_(p) {
    if (ptr_ && add_ref) intrusive_ptr_add_ref(ptr_);
  }

  intrusive_ptr(const intrusive_ptr& r) noexcept : ptr_(r.ptr_) {
    if (ptr_) intrusive_ptr_add_ref(ptr_);
  }
  intrusive_ptr(intrusive_ptr&& r) noexcept : ptr_(r.ptr_) { r.ptr_ = nullptr; }

  template <class U, std::enable_if_t<std::is_convertible_v<U*, T*>, int> = 0>
  intrusive_ptr(const intrusive_ptr<U>& r) noexcept : ptr_(r.ptr_) {
    if (ptr_) intrusive_ptr_add_ref(ptr_);
  }
  template <class U, std::enable_if_t<std::is_convertible_v<U*, T*>, int> = 0>
  intrusive_ptr(intrusive_ptr<U>&& r) noexcept : ptr_(r.ptr_) {
    r.ptr_ = nullptr;
  }

  ~intrusive_ptr() {
    if (ptr_) intrusive_ptr_release(ptr_);
  }

  intrusive_ptr& operator=(const intrusive_ptr& r) noexcept {
    if (this != &r) {
      intrusive_ptr(r).swap(*this);  // copy-and-swap，先增后减，异常安全
    }
    return *this;
  }
  intrusive_ptr& operator=(intrusive_ptr&& r) noexcept {
    if (this != &r) {
      if (ptr_) intrusive_ptr_release(ptr_);
      ptr_ = r.ptr_;
      r.ptr_ = nullptr;
    }
    return *this;
  }
  intrusive_ptr& operator=(std::nullptr_t) noexcept {
    reset();
    return *this;
  }
  template <class U, std::enable_if_t<std::is_convertible_v<U*, T*>, int> = 0>
  intrusive_ptr& operator=(const intrusive_ptr<U>& r) noexcept {
    intrusive_ptr(r).swap(*this);
    return *this;
  }
  template <class U, std::enable_if_t<std::is_convertible_v<U*, T*>, int> = 0>
  intrusive_ptr& operator=(intrusive_ptr<U>&& r) noexcept {
    intrusive_ptr(std::move(r)).swap(*this);
    return *this;
  }

  T* get() const noexcept { return ptr_; }
  T& operator*() const noexcept { return *ptr_; }
  T* operator->() const noexcept { return ptr_; }
  explicit operator bool() const noexcept { return ptr_ != nullptr; }

  void reset() noexcept {
    if (ptr_) {
      T* p = ptr_;
      ptr_ = nullptr;
      intrusive_ptr_release(p);
    }
  }
  void reset(T* p) noexcept {
    if (p != ptr_) {
      if (ptr_) intrusive_ptr_release(ptr_);
      ptr_ = p;
      if (ptr_) intrusive_ptr_add_ref(ptr_);
    }
  }

  void swap(intrusive_ptr& r) noexcept { std::swap(ptr_, r.ptr_); }

  friend bool operator==(const intrusive_ptr& a,
                         const intrusive_ptr& b) noexcept {
    return a.ptr_ == b.ptr_;
  }
  friend bool operator!=(const intrusive_ptr& a,
                         const intrusive_ptr& b) noexcept {
    return a.ptr_ != b.ptr_;
  }
  friend bool operator==(const intrusive_ptr& a, std::nullptr_t) noexcept {
    return a.ptr_ == nullptr;
  }
  friend bool operator==(std::nullptr_t, const intrusive_ptr& a) noexcept {
    return a.ptr_ == nullptr;
  }
  friend bool operator!=(const intrusive_ptr& a, std::nullptr_t) noexcept {
    return a.ptr_ != nullptr;
  }
  friend bool operator!=(std::nullptr_t, const intrusive_ptr& a) noexcept {
    return a.ptr_ != nullptr;
  }

 private:
  T* ptr_ = nullptr;
};

template <class T, class Counter>
intrusive_ptr<T>
intrusive_ref_counter<T, Counter>::intrusive_ptr_from_this() noexcept {
  return intrusive_ptr<T>(static_cast<T*>(this));
}

template <class T, class Counter>
intrusive_ptr<const T>
intrusive_ref_counter<T, Counter>::intrusive_ptr_from_this() const noexcept {
  return intrusive_ptr<const T>(static_cast<const T*>(this));
}

template <class T, class U>
bool operator<(const intrusive_ptr<T>& a, const intrusive_ptr<U>& b) noexcept {
  return a.get() < b.get();
}
template <class T, class U>
bool operator>(const intrusive_ptr<T>& a, const intrusive_ptr<U>& b) noexcept {
  return b < a;
}
template <class T, class U>
bool operator<=(const intrusive_ptr<T>& a, const intrusive_ptr<U>& b) noexcept {
  return !(b < a);
}
template <class T, class U>
bool operator>=(const intrusive_ptr<T>& a, const intrusive_ptr<U>& b) noexcept {
  return !(a < b);
}

}  // namespace util

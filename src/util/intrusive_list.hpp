

#pragma once

#include <cassert>
#include <cstddef>
#include <iterator>
#include <type_traits>

namespace util {

enum class link_mode : unsigned char {
  normal_link,  // 不做任何检查
  safe_link,  // push 前检查未链接；erase/unlink/容器析构后清空节点指针
  auto_unlink,  // 析构自动摘除（当前不支持）
};

namespace detail {

// 链表的原始节点：双向指针。list_member_hook 继承它。
template <link_mode Mode>
struct list_node_base {
  list_node_base* prev_ = nullptr;
  list_node_base* next_ = nullptr;

  bool is_linked() const noexcept { return next_ != nullptr; }
  void unlink() noexcept {
    prev_ = nullptr;
    next_ = nullptr;
  }
};

}  // namespace detail

template <link_mode Mode = link_mode::safe_link>
class list_member_hook : public detail::list_node_base<Mode> {
 public:
  bool is_linked() const noexcept {
    return detail::list_node_base<Mode>::is_linked();
  }
  void unlink() noexcept { detail::list_node_base<Mode>::unlink(); }
};

template <class H, link_mode M>
inline constexpr bool is_hook_of_v =
    std::is_base_of_v<detail::list_node_base<M>, H>;

template <class T, class Hook, Hook T::*MemberPtr,
          bool ConstantTimeSize = false>
class intrusive_list {
  static_assert(
      std::is_standard_layout_v<T>,
      "intrusive_list<T, Hook, Hook T::*MemberPtr>: T 必须是标准布局类型"
      "（与 boost::intrusive::member_hook 的前提一致）");
  static_assert(is_hook_of_v<Hook, link_mode::normal_link> ||
                    is_hook_of_v<Hook, link_mode::safe_link>,
                "Hook 必须继承自 util::list_member_hook");
  static_assert(
      !is_hook_of_v<Hook, link_mode::auto_unlink>,
      "link_mode::auto_unlink 暂不支持，请使用 safe_link 或 normal_link");

  static constexpr link_mode kMode = is_hook_of_v<Hook, link_mode::safe_link>
                                         ? link_mode::safe_link
                                         : link_mode::normal_link;
  static constexpr bool kSafe = (kMode == link_mode::safe_link);

  using node_type = detail::list_node_base<kMode>;

 public:
  using value_type = T;
  using reference = T&;
  using const_reference = const T&;
  using pointer = T*;
  using const_pointer = const T*;
  using size_type = std::size_t;

  class iterator {
    friend class const_iterator;

   public:
    using iterator_category = std::bidirectional_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = T*;
    using reference = T&;

    iterator() noexcept = default;

    reference operator*() const noexcept { return *to_value(node_); }
    pointer operator->() const noexcept { return to_value(node_); }
    iterator& operator++() noexcept {
      node_ = node_->next_;
      return *this;
    }
    iterator operator++(int) noexcept {
      iterator tmp(*this);
      ++(*this);
      return tmp;
    }
    iterator& operator--() noexcept {
      node_ = node_->prev_;
      return *this;
    }
    iterator operator--(int) noexcept {
      iterator tmp(*this);
      --(*this);
      return tmp;
    }

    friend bool operator==(const iterator& a, const iterator& b) noexcept {
      return a.node_ == b.node_;
    }
    friend bool operator!=(const iterator& a, const iterator& b) noexcept {
      return a.node_ != b.node_;
    }

   private:
    friend class intrusive_list;
    explicit iterator(node_type* node) noexcept : node_(node) {}
    node_type* node_ = nullptr;
  };

  class const_iterator {
   public:
    using iterator_category = std::bidirectional_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = const T*;
    using reference = const T&;

    const_iterator() noexcept = default;
    const_iterator(const iterator& other) noexcept : node_(other.node_) {}

    reference operator*() const noexcept { return *to_value(node_); }
    pointer operator->() const noexcept { return to_value(node_); }
    const_iterator& operator++() noexcept {
      node_ = node_->next_;
      return *this;
    }
    const_iterator operator++(int) noexcept {
      const_iterator tmp(*this);
      ++(*this);
      return tmp;
    }
    const_iterator& operator--() noexcept {
      node_ = node_->prev_;
      return *this;
    }
    const_iterator operator--(int) noexcept {
      const_iterator tmp(*this);
      --(*this);
      return tmp;
    }

    friend bool operator==(const const_iterator& a,
                           const const_iterator& b) noexcept {
      return a.node_ == b.node_;
    }
    friend bool operator!=(const const_iterator& a,
                           const const_iterator& b) noexcept {
      return a.node_ != b.node_;
    }

   private:
    friend class intrusive_list;
    explicit const_iterator(const node_type* node) noexcept : node_(node) {}
    const node_type* node_ = nullptr;
  };

  intrusive_list() noexcept { sentinel_init(); }
  intrusive_list(intrusive_list&& other) noexcept {
    sentinel_init();
    move_nodes_from(other);
  }
  intrusive_list& operator=(intrusive_list&& other) noexcept {
    if (this != &other) {
      clear();
      move_nodes_from(other);
    }
    return *this;
  }
  intrusive_list(const intrusive_list&) = delete;
  intrusive_list& operator=(const intrusive_list&) = delete;
  ~intrusive_list() { detach_all(); }

  bool empty() const noexcept { return sentinel_.next_ == &sentinel_; }

  size_type size() const noexcept {
    if constexpr (ConstantTimeSize) {
      return size_;
    } else {
      size_type n = 0;
      const node_type* cur = sentinel_.next_;
      while (cur != &sentinel_) {
        ++n;
        cur = cur->next_;
      }
      return n;
    }
  }

  void clear() noexcept {
    detach_all();
    sentinel_init();
    if constexpr (ConstantTimeSize) size_ = 0;
  }

  void push_front(T& value) noexcept { link_front(to_hook(&value)); }
  void push_back(T& value) noexcept { link_back(to_hook(&value)); }

  void pop_front() noexcept { erase(begin()); }
  void pop_back() noexcept { erase(--end()); }

  reference front() noexcept {
    assert(!empty());
    return *begin();
  }
  const_reference front() const noexcept {
    assert(!empty());
    return *begin();
  }
  reference back() noexcept {
    assert(!empty());
    return *(--end());
  }
  const_reference back() const noexcept {
    assert(!empty());
    return *(--end());
  }

  iterator begin() noexcept { return iterator(sentinel_.next_); }
  const_iterator begin() const noexcept {
    return const_iterator(sentinel_.next_);
  }
  iterator end() noexcept { return iterator(&sentinel_); }
  const_iterator end() const noexcept { return const_iterator(&sentinel_); }

  static iterator s_iterator_to(T& value) noexcept {
    return iterator(to_hook(&value));
  }
  static const_iterator s_iterator_to(const T& value) noexcept {
    return const_iterator(to_hook(&value));
  }

  iterator erase(iterator pos) noexcept {
    assert(pos.node_ != &sentinel_);
    node_type* next = pos.node_->next_;
    unlink_node(pos.node_);
    return iterator(next);
  }

  iterator erase(iterator first, iterator last) noexcept {
    while (first != last) first = erase(first);
    return last;
  }

 private:
  static Hook* to_hook(T* value) noexcept { return &(value->*MemberPtr); }
  static const Hook* to_hook(const T* value) noexcept {
    return &(value->*MemberPtr);
  }

  static constexpr std::ptrdiff_t member_offset() noexcept {
    return static_cast<std::ptrdiff_t>(
        reinterpret_cast<const char*>(&(static_cast<T*>(nullptr)->*MemberPtr)) -
        static_cast<const char*>(nullptr));
  }
  static T* to_value(node_type* node) noexcept {
    return reinterpret_cast<T*>(reinterpret_cast<char*>(node) -
                                member_offset());
  }
  static const T* to_value(const node_type* node) noexcept {
    return reinterpret_cast<const T*>(reinterpret_cast<const char*>(node) -
                                      member_offset());
  }

  void sentinel_init() noexcept {
    sentinel_.prev_ = &sentinel_;
    sentinel_.next_ = &sentinel_;
  }

  void detach_all() noexcept {
    node_type* cur = sentinel_.next_;
    while (cur != &sentinel_) {
      node_type* next = cur->next_;
      if constexpr (kSafe) cur->unlink();
      cur = next;
    }
  }

  void link_front(node_type* node) noexcept {
    if constexpr (kSafe) assert(!node->is_linked());
    node_type* head = sentinel_.next_;
    node->prev_ = &sentinel_;
    node->next_ = head;
    head->prev_ = node;
    sentinel_.next_ = node;
    if constexpr (ConstantTimeSize) ++size_;
  }
  void link_back(node_type* node) noexcept {
    if constexpr (kSafe) assert(!node->is_linked());
    node_type* tail = sentinel_.prev_;
    node->prev_ = tail;
    node->next_ = &sentinel_;
    tail->next_ = node;
    sentinel_.prev_ = node;
    if constexpr (ConstantTimeSize) ++size_;
  }
  void unlink_node(node_type* node) noexcept {
    node->prev_->next_ = node->next_;
    node->next_->prev_ = node->prev_;
    if constexpr (kSafe) node->unlink();
    if constexpr (ConstantTimeSize) --size_;
  }

  void move_nodes_from(intrusive_list& other) noexcept {
    if (!other.empty()) {
      node_type* first = other.sentinel_.next_;
      node_type* last = other.sentinel_.prev_;
      sentinel_.next_ = first;
      first->prev_ = &sentinel_;
      sentinel_.prev_ = last;
      last->next_ = &sentinel_;
      other.sentinel_init();
    }
    if constexpr (ConstantTimeSize) {
      size_ = other.size_;
      other.size_ = 0;
    }
  }

  node_type sentinel_;  // 哨兵节点，prev/next 自指表示空表
  size_type size_ = 0;
};

}  // namespace util

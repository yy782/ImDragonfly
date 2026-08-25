#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <memory_resource>
#include <new>
#include <utility>

namespace util {

template <typename T>
class spsc_queue {
 public:
  using value_type = T;

  // capacity 必须是 >= 2 的 2 的幂
  explicit spsc_queue(size_t capacity,
                      std::pmr::memory_resource* mr =
                          std::pmr::get_default_resource())
      : buffer_(nullptr),
        capacity_(capacity),
        buffer_mask_(capacity - 1),
        mr_(mr) {
    assert(capacity >= 2 && (capacity & (capacity - 1)) == 0 &&
           "spsc_queue: capacity must be a power of two and >= 2");
    buffer_ = AllocCells(capacity, mr);
  }

  spsc_queue(const spsc_queue&) = delete;
  spsc_queue& operator=(const spsc_queue&) = delete;

  ~spsc_queue() {
    // 销毁剩余元素（析构时不得有其他线程并发访问）
    size_t dq = dequeue_pos_.load(std::memory_order_relaxed);
    size_t eq = enqueue_pos_.load(std::memory_order_relaxed);
    while (dq != eq) {
      cell_t& cell = buffer_[dq & buffer_mask_];
      std::launder(reinterpret_cast<T*>(&cell.storage))->~T();
      ++dq;
    }
    if (buffer_ != nullptr) {
      mr_->deallocate(buffer_, capacity_ * sizeof(cell_t), alignof(cell_t));
      buffer_ = nullptr;
    }
  }

  template <typename U>
  bool try_enqueue(U&& data) {
    size_t eq = enqueue_pos_.load(std::memory_order_relaxed);
    size_t dq = dequeue_pos_cached_;
    if (eq - dq >= capacity_) {
      dq = dequeue_pos_.load(std::memory_order_acquire);
      if (eq - dq >= capacity_) {
        dequeue_pos_cached_ = dq;
        return false;
      }
      dequeue_pos_cached_ = dq;
    }
    cell_t* cell = &buffer_[eq & buffer_mask_];
    ::new (static_cast<void*>(&cell->storage)) T(std::forward<U>(data));
    enqueue_pos_.store(eq + 1, std::memory_order_release);
    return true;
  }

  // 仅消费者线程调用：出队，队列空时返回 false
  bool try_dequeue(T& data) {
    size_t dq = dequeue_pos_.load(std::memory_order_relaxed);
    size_t eq = enqueue_pos_cached_;
    if (eq == dq) {
      eq = enqueue_pos_.load(std::memory_order_acquire);
      if (eq == dq) {
        enqueue_pos_cached_ = eq;
        return false;
      }
      enqueue_pos_cached_ = eq;
    }
    cell_t* cell = &buffer_[dq & buffer_mask_];
    T* ptr = std::launder(reinterpret_cast<T*>(&cell->storage));
    data = std::forward<T>(*ptr);
    ptr->~T();
    dequeue_pos_.store(dq + 1, std::memory_order_release);
    return true;
  }

  // 仅消费者线程调用：查看队头元素而不出队，队列空时返回 nullptr
  T* front() {
    size_t dq = dequeue_pos_.load(std::memory_order_relaxed);
    size_t eq = enqueue_pos_cached_;
    if (eq == dq) {
      eq = enqueue_pos_.load(std::memory_order_acquire);
      if (eq == dq) {
        enqueue_pos_cached_ = eq;
        return nullptr;
      }
      enqueue_pos_cached_ = eq;
    }
    return std::launder(
        reinterpret_cast<T*>(&buffer_[dq & buffer_mask_].storage));
  }

  bool empty() const {
    size_t dq = dequeue_pos_.load(std::memory_order_relaxed);
    size_t eq = enqueue_pos_.load(std::memory_order_acquire);
    return eq == dq;
  }

  bool is_full() const {
    size_t eq = enqueue_pos_.load(std::memory_order_relaxed);
    size_t dq = dequeue_pos_.load(std::memory_order_acquire);
    return eq - dq >= capacity_;
  }

  size_t capacity() const { return capacity_; }

 private:
  struct alignas(T) cell_t {
    alignas(T) unsigned char storage[sizeof(T)];
  };

  struct alignas(64) cacheline_pad {
    unsigned char pad[64];
  };

  static cell_t* AllocCells(size_t n, std::pmr::memory_resource* mr) {
    void* p = mr->allocate(n * sizeof(cell_t), alignof(cell_t));
    return static_cast<cell_t*>(p);
  }

  cacheline_pad pad0_;
  std::atomic<size_t> enqueue_pos_{0};
  size_t dequeue_pos_cached_ = 0;
  cacheline_pad pad1_;
  std::atomic<size_t> dequeue_pos_{0};
  size_t enqueue_pos_cached_ = 0;
  cacheline_pad pad2_;
  cell_t* buffer_ = nullptr;
  size_t capacity_ = 0;
  size_t buffer_mask_ = 0;
  cacheline_pad pad3_;
  std::pmr::memory_resource* mr_ = nullptr;
};

}  // namespace util

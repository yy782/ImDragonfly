#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "detail/memory_resource.hpp"

namespace util {

template <typename T>
class mpmc_queue {
 public:
  using value_type = T;

  // capacity 必须是 >= 2 的 2 的幂
  explicit mpmc_queue(size_t capacity, PMR_NS::memory_resource* mr =
                                           PMR_NS::get_default_resource())
      : buffer_(nullptr),
        capacity_(capacity),
        buffer_mask_(capacity - 1),
        mr_(mr) {
    assert(capacity >= 2 && (capacity & (capacity - 1)) == 0 &&
           "mpmc_queue: capacity must be a power of two and >= 2");
    buffer_ = AllocCells(capacity, mr);
    for (size_t i = 0; i != capacity; ++i) {
      buffer_[i].sequence.store(i, std::memory_order_relaxed);
    }
  }

  mpmc_queue(const mpmc_queue&) = delete;
  mpmc_queue& operator=(const mpmc_queue&) = delete;

  ~mpmc_queue() {
    for (;;) {
      size_t pos = dequeue_pos_.fetch_add(1, std::memory_order_relaxed);
      cell_t& cell = buffer_[pos & buffer_mask_];
      size_t seq = cell.sequence.load(std::memory_order_acquire);
      intptr_t dif = (intptr_t)seq - (intptr_t)(pos + 1);
      if (dif < 0) {
        break;
      }
      std::launder(reinterpret_cast<T*>(&cell.storage))->~T();
    }
    if (buffer_ != nullptr) {
      mr_->deallocate(buffer_, capacity_ * sizeof(cell_t), alignof(cell_t));
      buffer_ = nullptr;
    }
  }
  template <typename U>
  bool try_enqueue(U&& data) {
    for (;;) {
      size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
      cell_t* cell = &buffer_[pos & buffer_mask_];
      size_t seq = cell->sequence.load(std::memory_order_acquire);
      intptr_t dif = (intptr_t)seq - (intptr_t)pos;
      if (dif == 0) {
        if (enqueue_pos_.compare_exchange_weak(pos, pos + 1,
                                               std::memory_order_relaxed)) {
          ::new (static_cast<void*>(&cell->storage)) T(std::forward<U>(data));
          cell->sequence.store(pos + 1, std::memory_order_release);
          return true;
        }
      } else if (dif < 0) {
        return false;
      }
    }
  }

  bool try_dequeue(T& data) {
    for (;;) {
      size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
      cell_t* cell = &buffer_[pos & buffer_mask_];
      size_t seq = cell->sequence.load(std::memory_order_acquire);
      intptr_t dif = (intptr_t)seq - (intptr_t)(pos + 1);
      if (dif == 0) {
        if (dequeue_pos_.compare_exchange_weak(pos, pos + 1,
                                               std::memory_order_relaxed)) {
          T* ptr = std::launder(reinterpret_cast<T*>(&cell->storage));
          data = std::forward<T>(*ptr);
          ptr->~T();

          cell->sequence.store(pos + buffer_mask_ + 1,
                               std::memory_order_release);
          return true;
        }
      } else if (dif < 0) {
        return false;
      }
    }
  }
  bool empty() const {
    size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
    cell_t* cell = &buffer_[pos & buffer_mask_];
    size_t seq = cell->sequence.load(std::memory_order_acquire);
    return (intptr_t)seq - (intptr_t)(pos + 1) < 0;
  }

  bool is_full() const {
    size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
    cell_t* cell = &buffer_[pos & buffer_mask_];
    intptr_t seq = cell->sequence.load(std::memory_order_relaxed);
    return (intptr_t)seq - (intptr_t)pos < 0;
  }

  size_t capacity() const { return capacity_; }

 private:
  struct alignas(std::max(alignof(T), alignof(std::atomic<size_t>))) cell_t {
    std::atomic<size_t> sequence;
    alignas(T) unsigned char storage[sizeof(T)];
  };

  struct alignas(64) cacheline_pad {
    unsigned char pad[64];
  };

  static cell_t* AllocCells(size_t n, PMR_NS::memory_resource* mr) {
    void* p = mr->allocate(n * sizeof(cell_t), alignof(cell_t));
    return static_cast<cell_t*>(p);
  }

  cacheline_pad pad0_;
  cell_t* buffer_ = nullptr;
  size_t capacity_ = 0;
  size_t buffer_mask_ = 0;
  cacheline_pad pad1_;
  std::atomic<size_t> enqueue_pos_{0};
  cacheline_pad pad2_;
  std::atomic<size_t> dequeue_pos_{0};
  cacheline_pad pad3_;
  PMR_NS::memory_resource* mr_ = nullptr;
};

}  // namespace util

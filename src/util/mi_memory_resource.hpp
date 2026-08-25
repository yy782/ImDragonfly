#pragma once

#include <glog/logging.h>
#include <mimalloc.h>

#include <cstddef>
#include <memory_resource>

namespace dfly {

class MiMemoryResource : public std::pmr::memory_resource {
 public:
  explicit MiMemoryResource(mi_heap_t* heap) : heap_(heap) {}

  mi_heap_t* heap() { return heap_; }

  size_t used() const { return used_; }

 private:
  void* do_allocate(std::size_t size, std::size_t align) final {
    void* res = mi_heap_malloc_aligned(heap_, size, align);

    if (!res) LOG(FATAL) << "Out of memory";

    size_t delta = mi_usable_size(res);

    used_ += delta;

    return res;
  }

  void do_deallocate(void* ptr, std::size_t size, std::size_t align) final {
    size_t usable = mi_usable_size(ptr);
    used_ -= usable;
    mi_free_size_aligned(ptr, size, align);
  }

  bool do_is_equal(const std::pmr::memory_resource& o) const noexcept {
    return this == &o;
  }

  mi_heap_t* heap_;
  size_t used_ = 0;
};

}  // namespace dfly

// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "mi_memory_resource.hpp"

#include <sys/mman.h>
namespace dfly{ 
//#include "base/logging.h"

void* MiMemoryResource::do_allocate(size_t size, size_t align) {

    void* res = mi_heap_malloc_aligned(heap_, size, align);

    if (!res)
        throw std::bad_alloc{};

    size_t delta = mi_usable_size(res);

    used_ += delta;

    return res;
}

void MiMemoryResource::do_deallocate(void* ptr, size_t size, size_t align) {
    size_t usable = mi_usable_size(ptr);
    used_ -= usable;
    mi_free_size_aligned(ptr, size, align);
}


}  // namespace dfly

#pragma once

#include <cassert>

#include "base/pmr/memory_resource.h"
namespace detail {
inline thread_local PMR_NS::memory_resource* tl_mr = nullptr;
}


inline void InitTLStatelessAllocMR(PMR_NS::memory_resource* mr) {
    detail::tl_mr = mr;
}

inline void CleanupStatelessAllocMR() {
  detail::tl_mr = nullptr;
}


















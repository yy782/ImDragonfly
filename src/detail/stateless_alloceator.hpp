#pragma once

#include <cassert>
#include <memory_resource>

namespace dfly {

namespace detail {
inline thread_local std::pmr::memory_resource* tl_mr = nullptr;
}

inline void InitTLStatelessAllocMR(std::pmr::memory_resource* mr) {
  detail::tl_mr = mr;
}

inline void CleanupStatelessAllocMR() { detail::tl_mr = nullptr; }
}  // namespace dfly

#include "db_table.hpp"

namespace dfly {

namespace {
constexpr unsigned kInitSegmentLog = 3;
}  // namespace

DbTable::DbTable(std::pmr::memory_resource* mr)
    : prime_(kInitSegmentLog, detail::PrimeTablePolicy{}, mr) {}

DbTable::~DbTable() = default;

size_t DbTable::MemoryUsage() const {
  const size_t lock_bytes = trans_locks.Size() * sizeof(IntentLock);
  return prime_.MemoryUsage() + lock_bytes;
}

}  // namespace dfly

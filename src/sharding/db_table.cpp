#include "db_table.hpp"

namespace dfly {

namespace {
// 键空间初始分段对数。DashTable 以 segment 为扩容/并发单位，
// 每个 segment 默认 32 个 bucket（2^5）。
constexpr unsigned kInitSegmentLog = 3;
}  // namespace

DbTable::DbTable(PMR_NS::memory_resource* mr, DbIndex index)
    : prime_(kInitSegmentLog, detail::PrimeTablePolicy{}, mr), index_(index) {}

DbTable::~DbTable() = default;

size_t DbTable::MemoryUsage() const {
  // 结构级近似：主表 + 过期索引 + 锁表。
  // 值对象的堆外内存（ROBJ / 长串缓冲）不在此列，需调用方遍历累加。
  const size_t lock_bytes = trans_locks.Size() * sizeof(IntentLock);
  return prime_.MemoryUsage() + expire_.MemoryUsage() + lock_bytes;
}

}  // namespace dfly

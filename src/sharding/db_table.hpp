#pragma once

#include <unordered_map>
#include <vector>

#include "DashTable/compact_obj.hpp"
#include "DashTable/dash_table.hpp"
#include "DashTable/table_policy.hpp"
#include "detail/common_types.hpp"
#include "detail/intent_lock.hpp"
#include "util/intrusive_ptr.hpp"

namespace dfly {

using PrimeKey = detail::PrimeKey;
using PrimeValue = detail::PrimeValue;
using PrimeTable =
    dash::DashTable<PrimeKey, PrimeValue, detail::PrimeTablePolicy>;
using PrimeIterator = PrimeTable::iterator;
using PrimeConstIterator = PrimeTable::const_iterator;

inline bool IsValid(PrimeIterator it) { return !it.is_done(); }
inline bool IsValid(PrimeConstIterator it) { return !it.is_done(); }

class WatchedContext {
 public:
  void Notify() {}

  bool operator==(const WatchedContext&) { return false; }
};

class LockTable {
 public:
  IntentLock& Acquire(LockFp fp) {
    auto [it, inserted] = locks_.try_emplace(fp);
    (void)inserted;
    return it->second;
  }

  void Release(LockFp fp, IntentLock::Mode mode) {
    auto it = locks_.find(fp);
    if (it == locks_.end()) return;
    it->second.Release(mode);
    if (it->second.IsFree()) locks_.erase(it);
  }

  void RemoveIfUnused(LockFp fp) {
    auto it = locks_.find(fp);
    if (it != locks_.end() && it->second.IsFree()) locks_.erase(it);
  }
  bool Find(LockFp fp, IntentLock::Mode mode) const {
    auto it = locks_.find(fp);
    return it != locks_.end() && it->second.Check(mode);
  }

  size_t Size() const { return locks_.size(); }

 private:
  std::unordered_map<LockFp, IntentLock> locks_;
};

struct DbTable
    : util::intrusive_ref_counter<DbTable, util::thread_unsafe_counter> {
  explicit DbTable(std::pmr::memory_resource* mr);
  ~DbTable();

  PrimeTable& prime() { return prime_; }
  const PrimeTable& prime() const { return prime_; }

  size_t MemoryUsage() const;

  PrimeTable prime_;

  LockTable trans_locks;
  std::unordered_map<LockFp, std::vector<WatchedContext>> watched_keys_;
};

using DbTableArray = std::vector<util::intrusive_ptr<DbTable>>;

}  // namespace dfly

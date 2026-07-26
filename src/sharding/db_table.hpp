// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once
#include <absl/container/flat_hash_map.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "DashTable/compact_obj.hpp"
#include "DashTable/dash_table.hpp"
#include "DashTable/table_policy.hpp"
#include "detail/common.hpp"
#include "detail/intent_lock.hpp"
#include "detail/tx_base.hpp"
namespace dfly {
using PrimeKey = detail::PrimeKey;
using PrimeValue = detail::PrimeValue;

using PrimeTable = DashTable<PrimeKey, PrimeValue, detail::PrimeTablePolicy>;
using PrimeIterator = PrimeTable::iterator;
using PrimeConstIterator = PrimeTable::const_iterator;
inline bool IsValid(PrimeIterator it) { return !it.is_done(); }

inline bool IsValid(PrimeConstIterator it) { return !it.is_done(); }
using DbIndex = uint16_t;
class ConnectionContext;

class LockTable {
 public:
  size_t Size() const { return locks_.size(); }

  bool Acquire(LockFp fp, IntentLock::Mode mode) {
    return locks_[fp].Acquire(mode);  // 这里可能哈希冲突
  }

  void Release(LockFp fp, IntentLock::Mode mode) {
    auto it = locks_.find(fp);
    assert(it != locks_.end());
    it->second.Release(mode);
    if (it->second.IsFree()) locks_.erase(it);
  }

  auto begin() const { return locks_.cbegin(); }

  auto end() const { return locks_.cend(); }

 private:
  absl::flat_hash_map<LockFp, IntentLock> locks_;
};

struct DbTable
    : boost::intrusive_ref_counter<DbTable, boost::thread_unsafe_counter> {
  explicit DbTable(PMR_NS::memory_resource* mr,
                   DbIndex index);  // explicit多余???
  ~DbTable();

  PrimeTable& prime() { return prime_; }
  const PrimeTable& prime() const { return prime_; }

  DbIndex index() const { return index_; }

  PrimeTable prime_;
  DbIndex index_;
  LockTable trans_locks;
};
using DbTableArray = std::vector<boost::intrusive_ptr<DbTable>>;
}  // namespace dfly
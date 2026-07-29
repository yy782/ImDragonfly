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

// ═══════════════════════════════════════════════════════════════════════
// 基于 fingerprint 的全局锁表，key 为 KeyLockArgs 的 hash，不含事务 ID。
//
// 每个 fp 只维护一对全局引用计数 (cnt_[SHARED], cnt_[EXCLUSIVE])，
// 不按事务区分所有权。Release 只做计数递减，无法校验"这个锁是我加的吗"。
//
// 当前依赖 Acquire/Release 的 +1/-1 对称性保证正确性，没有额外的归属信息
// 做防御。以下场景依赖于这一约束，修改时需格外注意：
//
//   1. IntentLock::Acquire 采用"先增后判"策略 —— 即使返回 false，对应 mode
//      的计数器也已递增。因此调用方在失败后必须以相同的 Mode 调用 Release
//      回滚，否则其他事务的锁计数会被错误修改。
//
//   2. DbSlice::Acquire 逐 fp 加锁，若中途失败则回滚已成功的 fp（含失败的那
//      个，因上述"先增后判"）。回滚时对所有 fp 调用同 mode 的 Release，完全
//      依赖计数器能退回到 Acquire 前状态，无法区分"自己的锁"和"别人的锁"。
//
//   3. 同一个 fp 可能对应不同 key（hash 碰撞），T1 锁 keyA、T2 锁 keyB 可能
//      共享同一组 refcount。T2 回滚 Release 时，如果 mode 或次数不匹配，T1
//      的锁会被悄悄破坏，且无任何检测手段。
//
// 风险：如果将来 Acquire/Release 逻辑不对称（如加超时、锁升级、条件释放），
//       回滚路径会静默破坏其他事务的锁状态，破坏隔离性。
// ═══════════════════════════════════════════════════════════════════════
class LockTable {
 public:
  size_t Size() const { return locks_.size(); }

  bool Acquire(LockFp fp, IntentLock::Mode mode) {
    return locks_[fp].Acquire(mode);  // 这里可能哈希冲突
  }

  void Release(LockFp fp, IntentLock::Mode mode) {
    auto it = locks_.find(fp);
    // assert(it != locks_.end());
    if (it == locks_.end()) {
      return;
    }
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
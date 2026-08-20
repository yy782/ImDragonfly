// DbTable —— 单个数据库（DB）的键空间。
//
// 职责边界：只承载"这个库的数据长什么样、怎么读写"，不掺命令/事务语义。
//   * prime_         主存储（DashTable，键值唯一真源）
//   * expire_        过期索引（伴生结构，见 expire_index.hpp）
//   * trans_locks    VVL 锁模块的行级锁表。冻结：不参与本次重写，
//                    待事务层重写时一并重构。
//   * watched_keys_  WATCH 登记表：连接层经 WatchedKeySink 接收脏通知
//   * version_       键空间修改计数：WATCH / 乐观并发 / 快照的脏检测
//
// 依赖方向（单向）：ShardStorage -> DbTable -> DashTable。

#pragma once

#include <unordered_map>
#include <vector>

#include "DashTable/compact_obj.hpp"
#include "DashTable/dash_table.hpp"
#include "DashTable/table_policy.hpp"
#include "detail/common.hpp"
#include "detail/intent_lock.hpp"
#include "detail/tx_base.hpp"
#include "expire_index.hpp"
#include "util/intrusive_ptr.hpp"

namespace dfly {

using PrimeKey = detail::PrimeKey;
using PrimeValue = detail::PrimeValue;
using PrimeTable = DashTable<PrimeKey, PrimeValue, detail::PrimeTablePolicy>;
using PrimeIterator = PrimeTable::iterator;
using PrimeConstIterator = PrimeTable::const_iterator;

inline bool IsValid(PrimeIterator it) { return !it.is_done(); }
inline bool IsValid(PrimeConstIterator it) { return !it.is_done(); }

// WATCH 通知目标。连接层实现此接口（上层重写时接入），在键变脏时收到
// 回调。与 VVL 锁模块解耦：事务层重写时无需改 DbTable。
class WatchedKeySink {
 public:
  virtual ~WatchedKeySink() = default;
  virtual void MarkKeyDirty(std::string_view key) = 0;
};

// WATCH 登记条目：登记时的版本快照 + 到期时刻。
struct WatchedKeyEntry {
  WatchedKeySink* sink = nullptr;
  uint64_t version = 0;    // 登记时的键空间版本快照（DbTable::version_）
  uint64_t expire_ms = 0;  // 登记到期时刻；0 表示永不过期
};

// VVL 锁模块（冻结）：行级锁表，键指纹 -> 意图锁。
// 语义保持不变；事务层重写时再重构（与 Transaction 的锁协调一起）。
class LockTable {
 public:
  // 获取（不存在则创建）指纹对应的意图锁，返回其引用。调用方负责 Acquire。
  IntentLock& GetOrCreate(LockFp fp) {
    auto [it, inserted] = locks_.try_emplace(fp);
    (void)inserted;
    return it->second;
  }

  // 释放意图锁；锁空闲则移除表项。
  void Release(LockFp fp, IntentLock::Mode mode) {
    auto it = locks_.find(fp);
    if (it == locks_.end()) return;
    it->second.Release(mode);
    if (it->second.IsFree()) locks_.erase(it);
  }

  // 无竞争者（空闲）时移除表项。
  void RemoveIfUnused(LockFp fp) {
    auto it = locks_.find(fp);
    if (it != locks_.end() && it->second.IsFree()) locks_.erase(it);
  }

  // 指纹当前是否已被以 mode 持有（SHARED：存在共享者；EXCLUSIVE：被独占）。
  bool Find(LockFp fp, IntentLock::Mode mode) const {
    auto it = locks_.find(fp);
    return it != locks_.end() && it->second.Check(mode);
  }

  size_t Size() const { return locks_.size(); }

 private:
  std::unordered_map<LockFp, IntentLock> locks_;
};

struct DbTable : util::intrusive_ref_counter<DbTable, util::thread_unsafe_counter> {
  explicit DbTable(PMR_NS::memory_resource* mr, DbIndex index);
  ~DbTable();

  PrimeTable& prime() { return prime_; }
  const PrimeTable& prime() const { return prime_; }
  ExpireIndex& expire_index() { return expire_; }
  const ExpireIndex& expire_index() const { return expire_; }
  DbIndex index() const { return index_; }

  // 键空间修改计数：插入 / 覆盖 / 删除时自增（TTL 变更不计入）。
  // WATCH 以"登记时快照 vs 当前版本"判脏；事务/快照以其做乐观并发控制。
  void MarkChanged() { ++version_; }
  uint64_t version() const { return version_; }

  // 近似内存：表结构 + 过期索引 + 锁表。值对象的堆外内存
  // （ROBJ / 长串缓冲）需调用方在遍历时另行累加。
  size_t MemoryUsage() const;

  PrimeTable prime_;
  ExpireIndex expire_;
  DbIndex index_;
  LockTable trans_locks;  // VVL 锁模块（冻结）
  uint64_t version_ = 0;
  std::unordered_map<std::string, std::vector<WatchedKeyEntry>> watched_keys_;
};

using DbTableArray = std::vector<util::intrusive_ptr<DbTable>>;

}  // namespace dfly

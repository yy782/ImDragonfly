// ShardStorage —— 单分片（shard）的键空间存储。
//
// 与旧 DbSlice 的本质区别（这是重设计，不是换皮）：
//
//  1. 面向命令语义的原语，而非"裸迭代器 + 手动通知"。
//     旧版把迭代器 + AutoUpdater 泄漏给命令层：命令层要处理迭代器失效
//     （Launder）、要记得触发脏通知（Run）、要拆 add_expire/remove_expire。
//     本版：读写都以 key 为参数，存储层内部完成 查找/惰性过期/类型无关校验，
//     命令层拿到的只是一个只读视图，不存在迭代器失效问题。
//
//  2. 值 + TTL 一体提交。
//     Upsert / Mutate 一次调用完成：值写入 + TTL 同步 + 版本自增 + WATCH
//     脏检测 + 过期索引登记。命令层没有任何"后续收尾"义务。
//
//  3. 版本号驱动全自动脏检测。
//     DbTable::version_ 单调自增（插入/覆盖/删除/过期删除），WATCH 登记
//     版本快照，写路径自动比对并通知 WatchedKeySink，无需上层参与。
//     仅 TTL 变更不计版本（与 Redis 一致：EXPIRE 不触发 WATCH）。
//
//  4. 过期统一由过期索引驱动。
//     惰性过期内嵌在读取路径（读到期键即删），后台 PurgeExpired 按堆序
//     批量清理，O(到期数·log n)，替代旧版全表扫描。
//
//  5. 锁收敛为 Acquire / Release 两条原语（VVL 锁模块，LockTable 冻结）。
//
// 依赖方向（单向）：命令层/事务层 -> ShardStorage -> DbTable -> DashTable。

#pragma once

#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "DashTable/compact_obj.hpp"
#include "db_table.hpp"
#include "detail/op_status.hpp"
#include "detail/tx_base.hpp"
#include "util/expected.hpp"

namespace dfly {

// OpResult = expected<T, OpStatus>：成功携带值 / 失败携带状态码。
template <typename T> using OpResult = util::expected<T, OpStatus>;

class EngineShard;

class ShardStorage {
 public:
  using Key = std::string_view;
  using TimeMs = uint64_t;  // 绝对毫秒（epoch）

  ShardStorage(ShardId index, EngineShard* owner);
  ~ShardStorage();

  ShardStorage(const ShardStorage&) = delete;
  void operator=(const ShardStorage&) = delete;

  ShardId shard_id() const { return shard_id_; }

  // ---------- 读 ----------

  // 键值视图。有效期：分片事件循环内、当前命令执行期（表不会被并发修改）。
  struct ValueView {
    const PrimeValue* value = nullptr;
    bool valid() const { return value != nullptr; }
    CompactObjType obj_type() const { return value->ObjType(); }
    bool IsType(CompactObjType t) const {
      return value != nullptr && value->ObjType() == t;
    }
  };

  // 查找键（自动惰性过期：已过期键被删除并计入版本）。
  // 不存在 / 已过期 -> KEY_NOTFOUND。
  OpResult<ValueView> Find(const DbContext& cntx, Key key) const;

  bool Exists(const DbContext& cntx, Key key) const;

  // ---------- 写（值 + TTL 一体提交） ----------

  // 插入或整体覆盖。true = 新建键，false = 覆盖既有键。
  // ttl_at == 0 表示无 TTL（同时清除已有 TTL）。
  // 一次调用完成：值写入 + TTL 同步 + 版本自增 + WATCH 脏检测。
  OpResult<bool> Upsert(const DbContext& cntx, Key key, PrimeValue value,
                        TimeMs ttl_at);

  // 原地修改（INCR 等读-改-写场景，避免大值整体拷贝）。
  // mutate 在键值上执行；new_ttl 为空表示 TTL 保持不变。
  // 键不存在时自动新建（值初始为默认构造，交给 mutate 填充）。
  // true = 新建键。
  OpResult<bool> Mutate(const DbContext& cntx, Key key,
                        std::function<void(PrimeValue*)> mutate,
                        std::optional<TimeMs> new_ttl = std::nullopt);

  // 删除。true = 删除了存在的键（过期键不计入，与 DEL 语义一致）。
  OpResult<bool> Delete(const DbContext& cntx, Key key);

  // ---------- TTL（绝对毫秒） ----------

  // 设置（>0）/ 清除（0）到期时刻。键不存在 -> KEY_NOTFOUND。
  // 仅 TTL 变更不触发 WATCH（与 Redis 一致）。
  OpResult<void> SetTtl(const DbContext& cntx, Key key, TimeMs ttl_at);

  // 剩余到期时刻；0 = 无 TTL；键不存在 -> KEY_NOTFOUND。
  OpResult<TimeMs> ExpireTime(const DbContext& cntx, Key key) const;

  // ---------- 过期清理（后台任务 / 命令触发） ----------

  size_t PurgeExpired(TimeMs now_ms);                // 全部库
  size_t PurgeExpired(DbIndex db_ind, TimeMs now_ms);  // 单库

  // ---------- 数据库管理 ----------

  bool IsDbValid(DbIndex id) const {
    return id < db_arr_.size() && bool(db_arr_[id]);
  }
  size_t DbCount() const { return db_arr_.size(); }
  void EnsureDb(DbIndex id);

  DbTable* GetDb(DbIndex id) {
    return IsDbValid(id) ? db_arr_[id].get() : nullptr;
  }
  const DbTable* GetDb(DbIndex id) const {
    return IsDbValid(id) ? db_arr_[id].get() : nullptr;
  }

  size_t Size(DbIndex dbid) const;
  size_t MemoryUsage(DbIndex dbid) const;

  // SCAN 语义遍历：回调收到 (key, value) 的引用；遍历中修改表安全。
  template <typename Cb>
  void Traverse(DbIndex dbid, Cb&& cb);

  // ---------- WATCH（自动脏检测） ----------

  // 登记：键被修改 / 删除 / 过期删除时通知 sink。until_ms=0 永不过期。
  void Watch(const DbContext& cntx, Key key, WatchedKeySink* sink,
             TimeMs until_ms = 0);
  void Unwatch(WatchedKeySink* sink, Key key);
  void UnwatchAll(WatchedKeySink* sink);

  // ---------- 锁（VVL 锁模块，冻结） ----------

  // 按指纹集合获取意图锁。false = 与相反模式的持有者冲突（计数已累加，
  // 调用方负责在失败路径回滚已获取的锁）。
  bool Acquire(IntentLock::Mode mode, const KeyLockArgs& lock_args);
  void Release(IntentLock::Mode mode, const KeyLockArgs& lock_args);

 private:
  // 读路径查找：自动惰性过期（到期键删除并通知 WATCH 后视为不存在）。
  OpResult<PrimeIterator> Locate(const DbContext& cntx, Key key) const;

  // 惰性过期：到期则删除（版本自增 + WATCH 通知），返回是否删除。
  bool LazyExpire(DbTable& db, const PrimeIterator& it, Key key, TimeMs now_ms);

  // 值变更提交：版本自增 + WATCH 脏检测。
  void CommitWrite(DbTable& db, const PrimeIterator& it, Key key);

  // 删除并提交（版本自增 + WATCH 脏检测）。
  void EraseAndNotify(DbTable& db, const PrimeIterator& it, Key key);

  // TTL 同步到过期索引（登记 / 清除），不 bump 版本。
  void SyncTtl(DbTable& db, const PrimeIterator& it, TimeMs ttl_at);

  // 键变脏通知：比对登记版本，触发后清理登记（与键空间解耦，按 key 查表）。
  void NotifyWatchers(DbTable& db, Key key);

  ShardId shard_id_;
  EngineShard* owner_;
  DbTableArray db_arr_;
};

// ---------- 模板成员实现 ----------

template <typename Cb>
void ShardStorage::Traverse(DbIndex dbid, Cb&& cb) {
  if (!IsDbValid(dbid)) return;
  auto& db = *db_arr_[dbid];
  PrimeTable::Cursor cursor;
  do {
    cursor = db.prime_.Traverse(
        cursor, [&](PrimeTable::iterator it) { cb(it->first, it->second); });
  } while (cursor);
}

}  // namespace dfly

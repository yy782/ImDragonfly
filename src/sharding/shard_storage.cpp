#include "shard_storage.hpp"

#include <glog/logging.h>

#include <utility>

#include "engine_shard.hpp"
#include "util/Time.hpp"

namespace dfly {

ShardStorage::ShardStorage(ShardId index, EngineShard* owner)
    : shard_id_(index), owner_(owner) {
  db_arr_.resize(1);
  EnsureDb(0);
}

ShardStorage::~ShardStorage() = default;

// ---------- 读 ----------

OpResult<PrimeIterator> ShardStorage::Locate(const Context& cntx,
                                             Key key) const {
  if (!IsDbValid(cntx.GetDbIndex())) return OpStatus::KEY_NOTFOUND;

  DbTable& db = *db_arr_[cntx.GetDbIndex()];
  PrimeIterator it = db.prime_.Find(key);
  if (!IsValid(it)) return OpStatus::KEY_NOTFOUND;

  // 惰性过期：到期键就地删除并通知，对上层表现为"不存在"。
  if (LazyExpire(db, it, key, cntx.GetTimeNowMs()))
    return OpStatus::KEY_NOTFOUND;

  return it;
}

OpResult<ShardStorage::ValueView> ShardStorage::Find(const Context& cntx,
                                                     Key key) const {
  auto it = Locate(cntx, key);
  if (!it.ok()) return it.status();
  return ValueView{&it->second};
}

bool ShardStorage::Exists(const Context& cntx, Key key) const {
  return Find(cntx, key).ok();
}

// ---------- 写 ----------

OpResult<bool> ShardStorage::Upsert(const Context& cntx, Key key,
                                    PrimeValue value, TimeMs ttl_at) {
  EnsureDb(cntx.GetDbIndex());
  DbTable& db = *db_arr_[cntx.GetDbIndex()];

  PrimeIterator it = db.prime_.Find(key);
  bool is_new = !IsValid(it);
  if (is_new) {
    it = db.prime_.InsertNew(key, PrimeValue{});
  } else if (LazyExpire(db, it, key, cntx.GetTimeNowMs())) {
    // 键已过期：删除后按新建处理（SET 覆盖过期键的语义）。
    it = db.prime_.InsertNew(key, PrimeValue{});
    is_new = true;
  }

  it->second = std::move(value);
  SyncTtl(db, it, ttl_at);
  CommitWrite(db, it, key);  // 版本自增 + WATCH 脏检测
  return is_new;
}

OpResult<bool> ShardStorage::Mutate(const Context& cntx, Key key,
                                    std::function<void(PrimeValue*)> mutate,
                                    std::optional<TimeMs> new_ttl) {
  EnsureDb(cntx.GetDbIndex());
  DbTable& db = *db_arr_[cntx.GetDbIndex()];

  PrimeIterator it = db.prime_.Find(key);
  bool is_new = !IsValid(it);
  if (is_new) {
    it = db.prime_.InsertNew(key, PrimeValue{});
  } else if (LazyExpire(db, it, key, cntx.GetTimeNowMs())) {
    it = db.prime_.InsertNew(key, PrimeValue{});
    is_new = true;
  }

  mutate(&it->second);
  if (new_ttl.has_value()) SyncTtl(db, it, *new_ttl);
  CommitWrite(db, it, key);
  return is_new;
}

OpResult<bool> ShardStorage::Delete(const Context& cntx, Key key) {
  if (!IsDbValid(cntx.GetDbIndex())) return false;

  DbTable& db = *db_arr_[cntx.GetDbIndex()];
  PrimeIterator it = db.prime_.Find(key);
  if (!IsValid(it)) return false;

  // 已过期：清理并计为"未删除"（DEL 语义）。
  if (LazyExpire(db, it, key, cntx.GetTimeNowMs())) return false;

  EraseAndNotify(db, it, key);
  return true;
}

// ---------- TTL ----------

OpResult<void> ShardStorage::SetTtl(const Context& cntx, Key key,
                                    TimeMs ttl_at) {
  if (!IsDbValid(cntx.GetDbIndex())) return OpStatus::KEY_NOTFOUND;

  DbTable& db = *db_arr_[cntx.GetDbIndex()];
  PrimeIterator it = db.prime_.Find(key);
  if (!IsValid(it)) return OpStatus::KEY_NOTFOUND;
  if (LazyExpire(db, it, key, cntx.GetTimeNowMs()))
    return OpStatus::KEY_NOTFOUND;

  SyncTtl(db, it, ttl_at);  // 不 bump 版本、不通知（EXPIRE 不触发 WATCH）
  return OpStatus::OK;
}

OpResult<ShardStorage::TimeMs> ShardStorage::ExpireTime(const Context& cntx,
                                                        Key key) const {
  auto it = Locate(cntx, key);
  if (!it.ok()) return it.status();

  const PrimeKey& k = it->first;
  return k.HasExpire() ? uint64_t(k.GetExpireTime()) : 0;
}

// ---------- 过期清理 ----------

size_t ShardStorage::PurgeExpired(TimeMs now_ms) {
  size_t total = 0;
  for (DbIndex i = 0; i < db_arr_.size(); ++i) {
    total += PurgeExpired(i, now_ms);
  }
  return total;
}

size_t ShardStorage::PurgeExpired(DbIndex db_ind, TimeMs now_ms) {
  if (!IsDbValid(db_ind)) return 0;
  DbTable& db = *db_arr_[db_ind];

  return db.expire_index().PopExpired(now_ms, [&](const ExpireIndex::Entry& e) {
    // 延迟验证：只有"键仍在 + TTL 仍是该记录"才是有效到期，其余是残留。
    PrimeIterator it = db.prime_.FindByHash(e.hash, e.key);
    if (!IsValid(it)) return false;            // 残留：键已删
    if (!it->first.HasExpire()) return false;  // 残留：TTL 已移除
    if (uint64_t(it->first.GetExpireTime()) != e.exp_ms)
      return false;                            // 残留：TTL 已覆盖

    db.prime_.Erase(it);
    db.MarkChanged();
    NotifyWatchers(db, e.key);  // 过期删除也是键空间变更，WATCH 置脏
    return true;
  });
}

// ---------- 数据库管理 ----------

void ShardStorage::EnsureDb(DbIndex id) {
  if (IsDbValid(id)) return;
  if (db_arr_.size() <= id) db_arr_.resize(id + 1);
  db_arr_[id] =
      util::intrusive_ptr<DbTable>(new DbTable(owner_->memory_resource(), id));
}

size_t ShardStorage::Size(DbIndex dbid) const {
  const DbTable* db = GetDb(dbid);
  return db ? db->prime_.Size() : 0;
}

size_t ShardStorage::MemoryUsage(DbIndex dbid) const {
  const DbTable* db = GetDb(dbid);
  return db ? db->MemoryUsage() : 0;
}

// ---------- WATCH ----------

void ShardStorage::Watch(const Context& cntx, Key key, WatchedKeySink* sink,
                         TimeMs until_ms) {
  EnsureDb(cntx.GetDbIndex());
  DbTable& db = *db_arr_[cntx.GetDbIndex()];
  db.watched_keys_[std::string(key)].push_back(
      WatchedKeyEntry{sink, db.version(), until_ms});
}

void ShardStorage::Unwatch(WatchedKeySink* sink, Key key) {
  for (auto& db : db_arr_) {
    if (!db) continue;
    auto it = db->watched_keys_.find(std::string(key));
    if (it == db->watched_keys_.end()) continue;
    std::erase_if(it->second,
                  [&](const WatchedKeyEntry& e) { return e.sink == sink; });
    if (it->second.empty()) db->watched_keys_.erase(it);
  }
}

void ShardStorage::UnwatchAll(WatchedKeySink* sink) {
  for (auto& db : db_arr_) {
    if (!db) continue;
    for (auto it = db->watched_keys_.begin(); it != db->watched_keys_.end();) {
      std::erase_if(it->second,
                    [&](const WatchedKeyEntry& e) { return e.sink == sink; });
      if (it->second.empty()) {
        it = db->watched_keys_.erase(it);
      } else {
        ++it;
      }
    }
  }
}

void ShardStorage::NotifyWatchers(DbTable& db, Key key) {
  if (db.watched_keys_.empty()) return;
  auto it = db.watched_keys_.find(std::string(key));
  if (it == db.watched_keys_.end()) return;

  const TimeMs now = util::GetCurrentTimeMs();
  const uint64_t cur_version = db.version();
  auto& vec = it->second;

  // 先通知（不修改容器），再统一清理已触发 / 已过期的登记。
  for (const WatchedKeyEntry& e : vec) {
    if (e.expire_ms != 0 && now >= e.expire_ms) continue;  // 登记已过期
    if (cur_version != e.version) e.sink->MarkKeyDirty(key);
  }
  std::erase_if(vec, [&](const WatchedKeyEntry& e) {
    return (e.expire_ms != 0 && now >= e.expire_ms) || cur_version != e.version;
  });
  if (vec.empty()) db.watched_keys_.erase(it);
}

// ---------- 锁（VVL 锁模块，冻结） ----------

bool ShardStorage::Acquire(IntentLock::Mode mode,
                           const KeyLockArgs& lock_args) {
  if (!IsDbValid(lock_args.db_index)) return true;
  DbTable& db = *db_arr_[lock_args.db_index];

  bool res = true;
  for (LockFp fp : lock_args.fps) {
    IntentLock& il = db.trans_locks.GetOrCreate(fp);
    if (!il.Acquire(mode)) {
      // 与相反模式的持有者冲突。计数已累加（等待者视角），调用方负责
      // 在失败路径回滚已获取的锁（Release）。
      res = false;
    }
  }
  return res;
}

void ShardStorage::Release(IntentLock::Mode mode,
                           const KeyLockArgs& lock_args) {
  if (!IsDbValid(lock_args.db_index)) return;
  DbTable& db = *db_arr_[lock_args.db_index];
  for (LockFp fp : lock_args.fps) {
    db.trans_locks.Release(fp, mode);
  }
}

}  // namespace dfly

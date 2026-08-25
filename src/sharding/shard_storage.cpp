#include "shard_storage.hpp"

#include <glog/logging.h>

#include <utility>

#include "shard.hpp"

namespace dfly {

ShardStorage::ShardStorage(ShardId index, Shard* owner)
    : shard_id_(index), owner_(owner) {
  db_arr_.resize(1);
  EnsureDb(0);
}

ShardStorage::~ShardStorage() = default;

OpResult<PrimeIterator> ShardStorage::Locate(const DbContext& cntx,
                                             Key key) const {
  if (!IsDbValid(cntx.GetDbIndex()))
    return util::make_unexpected(
        OpStatus::KEY_NOTFOUND);  // 不应该是KEY_NOTFOUND

  DbTable& db = *db_arr_[cntx.GetDbIndex()];
  PrimeIterator it = db.prime_.Find(key);
  if (!IsValid(it)) return util::make_unexpected(OpStatus::KEY_NOTFOUND);

  if (LazyExpire(db, it, key, cntx.GetTimeNowMs()))
    return util::make_unexpected(OpStatus::KEY_NOTFOUND);

  return it;
}

OpResult<PrimeIterator> ShardStorage::Find(const DbContext& cntx,
                                           Key key) const {
  return Locate(cntx, key);
}

bool ShardStorage::Exists(const DbContext& cntx, Key key) const {
  return Find(cntx, key).has_value();
}

OpResult<ShardStorage::WriteIterator> ShardStorage::FindOrInsert(
    const DbContext& cntx, Key key) {
  EnsureDb(cntx.GetDbIndex());
  DbTable& db = *db_arr_[cntx.GetDbIndex()];

  PrimeIterator it = db.prime_.Find(key);
  if (IsValid(it)) {
    if (LazyExpire(db, it, key, cntx.GetTimeNowMs())) {
      it = db.prime_.InsertNew(key, PrimeValue{});
      return WriteIterator(this, &db, std::move(it), true);
    }
    return WriteIterator(this, &db, std::move(it), false);
  }
  it = db.prime_.InsertNew(key, PrimeValue{});
  return WriteIterator(this, &db, std::move(it), true);
}

OpResult<bool> ShardStorage::Upsert(const DbContext& cntx, Key key,
                                    PrimeValue value, TimeMs ttl_at) {
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

  it->second = std::move(value);
  SyncTtl(it, ttl_at);
  return is_new;
}

OpResult<bool> ShardStorage::Mutate(const DbContext& cntx, Key key,
                                    util::FunctionRef<void(PrimeValue*)> mutate,
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
  if (new_ttl.has_value()) SyncTtl(it, *new_ttl);
  return is_new;
}

OpResult<bool> ShardStorage::Delete(const DbContext& cntx, Key key) {
  DbTable& db = *db_arr_[cntx.GetDbIndex()];
  PrimeIterator it = db.prime_.Find(key);
  if (!IsValid(it)) return false;
  EraseAndNotify(db, it, key);
  return true;
}

OpResult<void> ShardStorage::SetTtl(const DbContext& cntx, Key key,
                                    TimeMs ttl_at) {
  if (!IsDbValid(cntx.GetDbIndex()))
    return util::make_unexpected(OpStatus::KEY_NOTFOUND);

  DbTable& db = *db_arr_[cntx.GetDbIndex()];
  PrimeIterator it = db.prime_.Find(key);
  if (!IsValid(it)) return util::make_unexpected(OpStatus::KEY_NOTFOUND);
  if (LazyExpire(db, it, key, cntx.GetTimeNowMs()))
    return util::make_unexpected(OpStatus::KEY_NOTFOUND);

  SyncTtl(it, ttl_at);
  return {};
}

OpResult<ShardStorage::TimeMs> ShardStorage::ExpireTime(const DbContext& cntx,
                                                        Key key) const {
  auto it = Locate(cntx, key);
  if (!it.has_value()) return util::make_unexpected(it.error());

  const PrimeKey& k = it.value()->first;
  return k.HasExpire() ? uint64_t(k.GetExpireTime()) : 0;
}

void ShardStorage::EnsureDb(DbIndex id) {
  if (IsDbValid(id)) return;
  if (db_arr_.size() <= id) db_arr_.resize(id + 1);
  db_arr_[id] =
      util::intrusive_ptr<DbTable>(new DbTable(owner_->memory_resource()));
}

size_t ShardStorage::MemoryUsage(DbIndex dbid) const {
  DCHECK(IsDbValid(dbid));
  const DbTable* db = db_arr_[dbid].get();
  DCHECK(db);
  return db->MemoryUsage();
}

void ShardStorage::Watch(const DbContext& cntx, Key key,
                         const WatchedContext& observer) {
  DbTable& db = *db_arr_[cntx.GetDbIndex()];
  db.watched_keys_[KeyFingerprint(key)].push_back(observer);
}

void ShardStorage::Unwatch(const WatchedContext& observer, Key key) {
  for (auto& db : db_arr_) {
    if (!db) continue;
    auto it = db->watched_keys_.find(KeyFingerprint(key));
    if (it == db->watched_keys_.end()) continue;
    std::erase_if(it->second, [&](WatchedContext& e) { return e == observer; });
    if (it->second.empty()) db->watched_keys_.erase(it);
  }
}

void ShardStorage::NotifyWatchers(DbTable& db, Key key) const {
  if (db.watched_keys_.empty()) return;
  auto it = db.watched_keys_.find(KeyFingerprint(key));
  if (it == db.watched_keys_.end()) return;

  for (WatchedContext& ctx : it->second) ctx.Notify();
}

bool ShardStorage::LazyExpire(DbTable& db, const PrimeIterator& it, Key key,
                              TimeMs now_ms) const {
  const PrimeKey& k = it->first;
  if (!k.HasExpire() || uint64_t(k.GetExpireTime()) > uint64_t(now_ms))
    return false;

  db.prime_.Erase(it);
  NotifyWatchers(db, key);
  return true;
}

void ShardStorage::EraseAndNotify(DbTable& db, const PrimeIterator& it,
                                  Key key) {
  db.prime_.Erase(it);
  NotifyWatchers(db, key);
}

void ShardStorage::SyncTtl(PrimeIterator& it, TimeMs ttl_at) {
  PrimeKey& k = const_cast<PrimeKey&>(it->first);
  if (ttl_at != 0) {
    k.SetExpireTime(ttl_at);
  } else if (k.HasExpire()) {
    k.ClearExpireTime();
  }
}

bool ShardStorage::Acquire(IntentLock::Mode mode,
                           const KeyLockArgs& lock_args) {
  DCHECK(IsDbValid(lock_args.db_index));
  DbTable& db = *db_arr_[lock_args.db_index];

  bool res = true;
  for (LockFp fp : lock_args.fps) {
    IntentLock& il = db.trans_locks.Acquire(fp);
    if (!il.Acquire(mode)) {
      res &= false;
    }
  }
  return res;
}

void ShardStorage::Release(IntentLock::Mode mode,
                           const KeyLockArgs& lock_args) {
  DCHECK(IsDbValid(lock_args.db_index));
  DbTable& db = *db_arr_[lock_args.db_index];
  for (LockFp fp : lock_args.fps) {
    db.trans_locks.Release(fp, mode);
  }
}

}  // namespace dfly

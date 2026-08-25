#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "DashTable/compact_obj.hpp"
#include "db_table.hpp"
#include "detail/common_types.hpp"
#include "detail/op_status.hpp"
#include "util/expected.hpp"
#include "util/function.hpp"

namespace dfly {

template <typename T>
using OpResult = util::expected<T, OpStatus>;

class Shard;

class ShardStorage {
 public:
  using Key = std::string_view;
  using TimeMs = uint64_t;

  ShardStorage(ShardId index, Shard* owner);
  ~ShardStorage();

  ShardStorage(const ShardStorage&) = delete;
  void operator=(const ShardStorage&) = delete;

  ShardId shard_id() const { return shard_id_; }

  OpResult<PrimeIterator> Find(const DbContext& cntx, Key key) const;

  bool Exists(const DbContext& cntx, Key key) const;

  class WriteIterator {
   public:
    WriteIterator() = default;
    WriteIterator(ShardStorage* owner, DbTable* db, PrimeIterator it,
                  bool is_new)
        : owner_(owner), db_(db), it_(std::move(it)), is_new_(is_new) {}

    WriteIterator(const WriteIterator&) = delete;
    WriteIterator& operator=(const WriteIterator&) = delete;

    WriteIterator(WriteIterator&& o) noexcept
        : owner_(o.owner_),
          db_(o.db_),
          it_(std::move(o.it_)),
          is_new_(o.is_new_) {
      o.owner_ = nullptr;
    }

    WriteIterator& operator=(WriteIterator&& o) noexcept {
      if (this != &o) {
        Post();
        owner_ = o.owner_;
        db_ = o.db_;
        it_ = std::move(o.it_);
        is_new_ = o.is_new_;
        o.owner_ = nullptr;
      }
      return *this;
    }

    ~WriteIterator() { Post(); }

    bool is_new() const { return is_new_; }

    PrimeValue& value() { return it_->second; }

    void set_ttl(TimeMs ttl_at) { owner_->SyncTtl(it_, ttl_at); }

    void erase() {
      if (it_.is_done()) return;
      owner_->EraseAndNotify(*db_, it_, it_->first.GetSlice());
      it_ = PrimeIterator{};
    }

   private:
    void Post() {
      if (!owner_ || it_.is_done()) return;
      owner_->NotifyWatchers(*db_, it_->first.GetSlice());
      owner_ = nullptr;
    }

    ShardStorage* owner_ = nullptr;
    DbTable* db_ = nullptr;
    PrimeIterator it_;
    bool is_new_ = false;
  };

  OpResult<WriteIterator> FindOrInsert(const DbContext& cntx, Key key);

  // 插入或整体覆盖。true = 新建键，false = 覆盖既有键。
  OpResult<bool> Upsert(const DbContext& cntx, Key key, PrimeValue value,
                        TimeMs ttl_at);

  OpResult<bool> Mutate(const DbContext& cntx, Key key,
                        util::FunctionRef<void(PrimeValue*)> mutate,
                        std::optional<TimeMs> new_ttl = std::nullopt);

  OpResult<bool> Delete(const DbContext& cntx, Key key);

  OpResult<void> SetTtl(const DbContext& cntx, Key key, TimeMs ttl_at);

  OpResult<TimeMs> ExpireTime(const DbContext& cntx, Key key) const;

  bool IsDbValid(DbIndex id) const {
    return id < db_arr_.size() && bool(db_arr_[id]);
  }
  size_t DbCount() const { return db_arr_.size(); }
  void EnsureDb(DbIndex id);

  size_t MemoryUsage(DbIndex dbid) const;

  template <typename Cb>
  void Traverse(DbIndex dbid, Cb&& cb);

  void Watch(const DbContext& cntx, Key key, const WatchedContext& observer);
  void Unwatch(const WatchedContext& observer, Key key);

  bool Acquire(IntentLock::Mode mode, const KeyLockContext& lock_args);
  void Release(IntentLock::Mode mode, const KeyLockContext& lock_args);

 private:
  OpResult<PrimeIterator> Locate(const DbContext& cntx, Key key) const;
  bool LazyExpire(DbTable& db, const PrimeIterator& it, Key key,
                  TimeMs now_ms) const;

  void EraseAndNotify(DbTable& db, const PrimeIterator& it, Key key);

  void SyncTtl(PrimeIterator& it, TimeMs ttl_at);
  void NotifyWatchers(DbTable& db, Key key) const;

  ShardId shard_id_;
  Shard* owner_;
  DbTableArray db_arr_;
};

template <typename Cb>
void ShardStorage::Traverse(DbIndex dbid, Cb&& cb) {
  if (!IsDbValid(dbid)) return;
  auto& db = *db_arr_[dbid];
  PrimeTable::Cursor cursor = 0;
  do {
    cursor = db.prime_.Traverse(
        cursor, [&](PrimeTable::iterator it) { cb(it->first, it->second); });
  } while (cursor);
}

}  // namespace dfly

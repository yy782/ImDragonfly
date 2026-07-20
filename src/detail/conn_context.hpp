// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include <absl/container/flat_hash_set.h>
#include "detail/common.hpp"
#include "cppcoro/task.hpp"
#include <atomic>
#include "sharding/engine_shard_set.hpp"
#include "sharding/db_slice.hpp"
#include <assert.h>
namespace dfly {

class Connection;
class Transaction;


class ConnectionContext{
public:
    ConnectionContext() = default;
    ConnectionContext& operator=(const ConnectionContext& o) {
        owner_ = o.owner_;
        ns_ = o.ns_;
        index_ = o.index_;
        assert(watch_keys_.empty());
        assert(!watched_dirty_);
        assert(watched_dirty_ver_ == 0);
        assert(watch_keys_.empty());
        return *this;
    }
    ~ConnectionContext();
    RedisSessionPtr owner() const { return owner_; }
    RedisSessionPtr& owner() { return owner_; }
    
    template<typename Cb>
    void AddWatchKey(std::string_view key, Cb&& cb);

    void ClearWatchKeys() {
        watch_keys_.clear();
        ++watched_dirty_ver_;
        watched_dirty_ = false;
    }
    uint64_t GetWatchedDirtyVer() const {
        return watched_dirty_ver_.load(std::memory_order_relaxed);
    }
    const absl::flat_hash_set<std::string_view>& GetWatchKeys() const { return watch_keys_; }
    bool HasWatchKeys() const { return !watch_keys_.empty(); }
    bool IsDirty() const { return watched_dirty_.load(std::memory_order_relaxed); }
    bool SetDirty(uint64_t watched_dirty_ver) {
        if (watched_dirty_ver != watched_dirty_ver_.load(std::memory_order_relaxed)) return false;
        watched_dirty_.store(true, std::memory_order_relaxed); 
        return true;
    }
    const Namespace* GetNamespace() const { return ns_; }
    DbIndex GetDbIndex() const { return index_; }


private:
    friend class RedisSession;
    ConnectionContext(RedisSessionPtr owner, Namespace* ns, DbIndex index) : owner_(owner), ns_(ns),index_(index) {}
    RedisSessionPtr owner_;
    Namespace* ns_; 
    DbIndex index_;
    absl::flat_hash_set<std::string_view> watch_keys_;
    std::atomic<uint64_t> watched_dirty_ver_ = 0;
    std::atomic<bool> watched_dirty_ = false;

    
    
};

class CommandId;

class  CommandContext{
public:
    CommandContext() = default;

    CommandContext(Transaction* transaction, const CommandId* cid) : 
    transaction_(transaction),
    cid_(cid) 
    {

    }
    const CommandId* cid() const {
      return cid_;
    }
    Transaction* tx() const { 
        return transaction_;
    }
private:
    Transaction* transaction_;
    const CommandId* cid_;
};

template<typename Cb>
void ConnectionContext::AddWatchKey(std::string_view key, Cb&& cb) 
{ 
    watch_keys_.insert(key);
    ShardId shard_id = Shard(key, shard_set->size());
    shard_set->Add(shard_id, [this, key, shard_id, cb = std::move(cb)](){
        ns_->GetDbSlice(shard_id).RegisterWatchedKey(key, this);
        cb();
    });
}




}  // namespace dfly


// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once
#include "detail/common_types.hpp"
#include "command_layer/cmn_types.hpp"
#include "sharding/namespaces.hpp"
#include <span>
#include <iostream>
namespace dfly {




class DbContext {
public:
    DbContext() = default;
    DbContext(const Namespace* ns, DbIndex index, uint64_t time_now_ms)
        : ns_(ns), db_index_(index), time_now_ms_(time_now_ms) {}
    DbContext(const DbContext& o) noexcept {
        *this = o;
    }
    DbContext& operator=(const DbContext& o) noexcept {
        ns_ = o.ns_;
        db_index_ = o.db_index_;
        time_now_ms_ = o.time_now_ms_;
        return *this;
    }
    DbSlice& GetDbSlice(ShardId shard_id) const {
        return ns_->GetDbSlice(shard_id);        
    }
    const Namespace* GetNamespace() const {
        return ns_;
    }
    DbIndex GetDbIndex() const {
        return db_index_;
    }
    uint64_t GetTimeNowMs() const {
        return time_now_ms_;
    }
private:
    const Namespace* ns_; 
    DbIndex db_index_;
    uint64_t time_now_ms_;
 
};


}  // namespace dfly
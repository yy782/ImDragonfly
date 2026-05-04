// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "namespaces.hpp"
#include "engine_shard_set.hpp"
#include "db_slice.hpp"
#include "synchronization.hpp"


namespace dfly {


Namespace::Namespace() {
    shard_db_slices_.resize(shard_set->size());
    shard_set->RunBriefInParallel([&](EngineShard* es) { // 并行执行
        ShardId sid = es->shard_id();
        shard_db_slices_[sid] = std::make_unique<DbSlice>(sid, false, es);
    });
}

DbSlice& Namespace::GetCurrentDbSlice() {
    EngineShard* es = EngineShard::tlocal();
    return GetDbSlice(es->shard_id());
}

DbSlice& Namespace::GetDbSlice(ShardId sid) {
    return *shard_db_slices_[sid];
}
Namespaces::Namespaces() {
    default_namespace_ = &GetOrInsert("");
}

Namespaces::~Namespaces() {
    Clear();
}

void Namespaces::Clear() {
    std::unique_lock<std::shared_mutex> lock(rw_mutex);

    default_namespace_ = nullptr;

    if (namespaces_.empty()) {
        return;
    }

    shard_set->RunBriefInParallel([&](EngineShard* es) {
        for (auto& ns : namespaces_) {
            ns.second.shard_db_slices_[es->shard_id()].reset();
        }
    });

    namespaces_.clear();
}

Namespace& Namespaces::GetDefaultNamespace() const {
    return *default_namespace_;
}

Namespace& Namespaces::GetOrInsert(std::string_view ns) {
    std::string nns=std::string(ns);                // not same
    {
        // Try to look up under a shared lock
        std::shared_lock<std::shared_mutex> lock(rw_mutex);
        auto it = namespaces_.find(nns);            
        if (it != namespaces_.end()) {
            return it->second;
        }
    }

    {
        // Key was not found, so we create create it under unique lock
        std::unique_lock<std::shared_mutex> lock(rw_mutex);
        return namespaces_[nns];
    }
}

}  // namespace dfly

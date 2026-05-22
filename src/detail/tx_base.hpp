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




struct DbContext {
    Namespace* ns_ = nullptr; 
    DbIndex db_index_ = 0;
    uint64_t time_now_ms_ = 0;
    DbSlice& GetDbSlice(ShardId shard_id) const{
        return ns_->GetDbSlice(shard_id);        
    }  
};


}  // namespace dfly
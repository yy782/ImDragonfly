#pragma once
#include "common_types.hpp"

#include "sharding/namespaces.hpp"

namespace dfly {

struct DbContext {
    Namespace* ns = nullptr; 
    DbIndex db_index = 0;
    // uint64_t time_now_ms = 0;
    DbSlice& GetDbSlice(ShardId shard_id) const{
        return ns->GetDbSlice(shard_id);        
    }


    
};

}  // namespace dfly
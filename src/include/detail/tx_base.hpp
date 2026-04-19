#pragma once
#include "common_types.hpp"
namespace dfly {

struct DbContext {
    DbIndex db_index = 0;
    uint64_t time_now_ms = 0;
    DbSlice& GetDbSlice(ShardId shard_id) const;
};

}  // namespace dfly

#include "common_types.hpp"

struct DbContext {
    DbIndex db_index = 0;
    uint64_t time_now_ms = 0;
    DbSlice& GetDbSlice(ShardId shard_id) const;
};

#include "transaction.hpp"

namespace dfly{

OpArgs Transaction::GetOpArgs(EngineShard* shard) const {
    return OpArgs{shard, this, GetDbContext()};
}


ShardArgs Transaction::GetShardArgs(ShardId sid) const {
    return ShardArgs{full_args_, std::span(args_slices_)}; 
}
    
}
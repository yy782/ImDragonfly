
#include "transaction.hpp"

namespace dfly{

OpArgs Transaction::GetOpArgs(EngineShard* shard) const {
    return OpArgs{shard, this, GetDbContext()};
}



    
}
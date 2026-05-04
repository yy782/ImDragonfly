#include "tx_base.hpp"
#include "sharding/engine_shard.hpp"

namespace dfly {

DbSlice& OpArgs::GetDbSlice() const {
    return db_cntx_.GetDbSlice(shard_->shard_id());
}
}
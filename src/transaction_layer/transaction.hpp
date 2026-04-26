
#pragma once

#include "tx_base.hpp"
#include "common_types.hpp"

#include <functional>

namespace dfly{


class CommandId;
class Namespace;


class Transaction{
public:
    using RunnableType = std::function<RunnableResult(Transaction* t, EngineShard*)>; // not same , using ref will well

    OpArgs Transaction::GetOpArgs(EngineShard* shard) const;
    
    DbContext GetDbContext() const {
        return DbContext{namespace_, db_index_, time_now_ms_};
    }   

private:
    const CommandId* cid_ = nullptr;
    Namespace* namespace_ = nullptr;
    DbIndex db_index_ = 0;
    uint64_t timw_now_ms_ = 0;

    ShardId unique_shard_id_ = kInvalidSid;


};






}
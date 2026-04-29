
#pragma once

#include "tx_base.hpp"
#include "common_types.hpp"
#include "utils/function_ref.hpp"
#include "engine_shard_set.hpp"
#include "cmn_types.hpp"
#include "tx_base.hpp"

namespace dfly{


class CommandId;
class Namespace;


class Transaction{
public:
    using RunnableType = utils::FunctionRef<RunnableResult(Transaction* t, EngineShard*)>; 

    OpArgs GetOpArgs(EngineShard* shard) const;
    ShardArgs GetShardArgs(ShardId sid) const;    
    DbContext GetDbContext() const {
        return DbContext{namespace_, db_index_, time_now_ms_};
    }   

    DbSlice& Transaction::GetDbSlice(ShardId shard_id) const {
        return namespace_->GetDbSlice(shard_id);
    }

    void Execute(std::coroutine_handle<Coro> handle, RunnableType cb) // not same 
    {
        shard_set->Add(unique_shard_id_, [this]()mutable{
            cb(this, GetDbSlice(unique_shard_id_).shard_owner());
            handle.resume();
        });
    }


private:
    // const CommandId* cid_ = nullptr;
    Namespace* namespace_ = nullptr;
    DbIndex db_index_ = 0;
    uint64_t time_now_ms_ = 0;

    ShardId unique_shard_id_ = kInvalidSid;

    std::vector<IndexSlice> args_slices_;
    facade::CmdArgList full_args_;

};






}
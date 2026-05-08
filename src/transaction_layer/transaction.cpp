
#include "transaction.hpp"

#include "sharding/engine_shard_set.hpp"
#include "command_layer/parsed_command.hpp"
#include "sharding/engine_shard_set.hpp"
namespace dfly{


Transaction::Transaction(const CommandId* cid) :
cid_(cid)
{
    args_slices_.reserve(Shard->size());
}



void Transaction::InitByArgs(Namespace* ns, DbIndex index, cmn::CmdArgList args){
    ns_ = ns;
    db_cntx_ = DbContext(ns, index, util::GetCurrentTimeMs()); 
    full_args_ = args;

    auto key = args[0]; 
    auto val = args[1];
    args_slices_[Shard(key)].push_back({0, 1}); 
}




OpArgs Transaction::GetOpArgs(EngineShard* shard) const {
    return OpArgs{shard, this, GetDbContext()};
}


ShardArgs Transaction::GetShardArgs(ShardId sid) const {

    (void)sid;
    return ShardArgs{full_args_, std::span(args_slices_)}; 
}
    

void Transaction::Scheduling(std::coroutine_handle<> handle, RunnableType cb) // not same 
{
    auto [shard_id, arg_indices] = args_slices_[0];
    
    shard_set->Add(shard_id, [this, cb = std::move(cb), handle, shard_id]()mutable{
        cb(this, GetDbSlice(shard_id).shard_owner());
        handle.resume();
    });
}








}
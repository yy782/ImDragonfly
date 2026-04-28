
#include "transaction.hpp"

#include "engine_shard_set.hpp"
#include "parsed_command.hpp"

namespace dfly{


Transaction::Transaction(CommandId* cid) :
cid_(cid)
{

}



OpStatus Transaction::InitByArgs(Namespace* ns, DbIndex index, cmn::CmdArgList args){
    db_cntx_ = DbContext(ns, db_index, GetCurrentTimeMs()); 
    full_args_ = args;
}


OpArgs Transaction::GetOpArgs(EngineShard* shard) const {
    return OpArgs{shard, this, GetDbContext()};
}


ShardArgs Transaction::GetShardArgs(ShardId sid) const {
    return ShardArgs{full_args_, std::span(args_slices_)}; 
}
    

void Transaction::Scheduling(std::coroutine_handle<Coro> handle, RunnableType cb) // not same 
{
    shard_set->Add(unique_shard_id_, [this]()mutable{
        cb(this, GetDbSlice(unique_shard_id_).shard_owner());
        handle.resume();
    });
}








}
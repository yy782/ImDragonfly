
#include "transaction.hpp"

#include "engine_shard_set.hpp"
#include "parsed_command.hpp"

namespace dfly{


Transaction::Transaction(CommandId* cid) :
cid_(cid)
{

}



void Transaction::InitByArgs(Namespace* ns, DbIndex index, cmn::CmdArgList args){
    ns_ = ns;
    db_cntx_ = DbContext(ns, db_index, GetCurrentTimeMs()); 
    full_args_ = args;
}

auto Transaction::Execute() {
    
}


OpArgs Transaction::GetOpArgs(EngineShard* shard) const {
    return OpArgs{shard, this, GetDbContext()};
}


ShardArgs Transaction::GetShardArgs(ShardId sid) const {
    return ShardArgs{full_args_, std::span(args_slices_)}; 
}
    

void Transaction::Scheduling(std::coroutine_handle<> handle, RunnableType cb) // not same 
{
    shard_set->Add(unique_shard_id_, [this]()mutable{
        cb(this, GetDbSlice(unique_shard_id_).shard_owner());
        handle.resume();
    });
}








}
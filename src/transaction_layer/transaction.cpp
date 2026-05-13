
#include "transaction.hpp"

#include "sharding/engine_shard_set.hpp"
#include "command_layer/parsed_command.hpp"
#include "sharding/engine_shard_set.hpp"
#include "sharding/db_slice.hpp"
#include "command_layer/cmn_types.hpp"
namespace dfly{


Transaction::Transaction(const CommandId* cid) :
cid_(cid)
{
    
}



void Transaction::InitByArgs(Namespace* ns, DbIndex index, ::cmn::CmdArgList args){
    ns_ = ns;
    db_cntx_ = DbContext(ns, index, util::GetCurrentTimeMs()); 
    full_args_ = args;
    auto key = args.size() > 1 ? args[1] : "";
    unique_shard_id_ = Shard(key); 
}




OpArgs Transaction::GetOpArgs(EngineShard* shard) const {
    return OpArgs{shard, this, GetDbContext()};
}


ShardArgs Transaction::GetShardArgs(ShardId sid) const {

    (void)sid;

    std::vector<IndexSlice> slice;
    slice.emplace_back(0, 1);
    return ShardArgs{full_args_, std::span(slice)}; // 不确定
}
    

void Transaction::Scheduling(std::coroutine_handle<> handle, RunnableType cb) // not same 
{   
    shard_set->Add(unique_shard_id_, [this, cb = std::move(cb), handle]()mutable{
        cb(this, GetDbSlice(unique_shard_id_).shard_owner());
        handle.resume();
    });
}

}
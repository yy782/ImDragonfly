
#pragma once

#include "tx_base.hpp"
#include "common_types.hpp"
#include "utils/function_ref.hpp"

#include "cmn_types.hpp"


namespace dfly{


class CommandId;
class Namespace;


class Transaction{
public:
    using RunnableType = utils::FunctionRef<RunnableResult(Transaction* t, EngineShard*)>; 

    explicit Transaction(const CommandId* cid);

    OpStatus InitByArgs(Namespace* ns, DbIndex index, cmn::CmdArgList args);



    OpArgs GetOpArgs(EngineShard* shard) const;

    ShardArgs GetShardArgs(ShardId sid) const;

    DbContext& GetDbContext() const {
        return db_cntx_;
    }   

    DbSlice& GetDbSlice(ShardId shard_id) const {
        return namespace_->GetDbSlice(shard_id);
    }



    
    void Scheduling(std::coroutine_handle<Coro> handle, RunnableType cb); // not same


private:
    const CommandId* cid_ = nullptr;

    DbContext db_cntx_;

    cmn::CmdArgList full_args_;
    std::vector<IndexSlice> args_slices_; // IndexSlice from tx_base.hpp, 处理full_args_的分片事务
    

};






}

#pragma once
#include <cstdint>
#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <span> 
#include <utility>
#include "detail/tx_base.hpp"
#include "detail/common_types.hpp"
#include "util/function.hpp"
#include "command_layer/command_registry.hpp"
#include "command_layer/cmn_types.hpp"
#include "sharding/op_status.hpp"

namespace dfly{


class CommandId;
class Namespace;


class Transaction{
public:

    using RunnableResult = facade::OpStatus;
    using RunnableType = util::FunctionRef<RunnableResult(Transaction* t, EngineShard*)>; 

    explicit Transaction(const CommandId* cid);

    void InitByArgs(Namespace* ns, DbIndex index, cmn::CmdArgList args);

    auto Execute();



    OpArgs GetOpArgs(EngineShard* shard) const;

    ShardArgs GetShardArgs(ShardId sid) const;

    const DbContext& GetDbContext() const {
        return db_cntx_;
    }   
    DbContext& GetDbContext() {
        return db_cntx_;
    }  
    const DbSlice& GetDbSlice(ShardId shard_id) const {
        return ns_->GetDbSlice(shard_id);
    }
    DbSlice& GetDbSlice(ShardId shard_id) {
        return ns_->GetDbSlice(shard_id);
    }    



    
    void Scheduling(std::coroutine_handle<> handle, RunnableType cb); // not same


private:
    const CommandId* cid_ = nullptr;

    DbContext db_cntx_;

    cmn::CmdArgList full_args_;
    std::vector<IndexSlice> args_slices_; // IndexSlice from tx_base.hpp, 处理full_args_的分片事务

    Namespace* ns_ = nullptr; // 事务所属的命名空间
    

};






}
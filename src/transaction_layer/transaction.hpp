
#pragma once
#include <cstdint>
#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <span> 
#include <utility>
#include <coroutine>
#include "detail/tx_base.hpp"
#include "detail/common_types.hpp"
#include "util/function.hpp"
#include "command_layer/command_registry.hpp"
#include "command_layer/cmn_types.hpp"
#include "sharding/op_status.hpp"

namespace dfly{
using ::cmn::CmdArgList;

class CommandId;
class Namespace;


class Transaction{
public:

    using RunnableType = util::FunctionRef<void(Transaction*, EngineShard*)>;
 

    explicit Transaction(const CommandId* cid);

    void InitByArgs(Namespace* ns, DbIndex index, CmdArgList args);




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
    DbIndex& GetDbIndex() {
        return db_cntx_.db_index_;
    }


    
    void Scheduling(std::coroutine_handle<> handle, RunnableType cb); // not same


private:
    const CommandId* cid_ = nullptr;

    DbContext db_cntx_;

    CmdArgList full_args_;
    ShardId unique_shard_id_;

    Namespace* ns_ = nullptr; // 事务所属的命名空间
    

};






}
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

#include "util/synchronization.hpp"

namespace dfly{
using ::cmn::CmdArgList;
using ::facade::OpStatus;
class CommandId;
class Namespace;


class Transaction{
public:

    using RunnableType = util::FunctionRef<void(Transaction*, EngineShard*)>;

    enum CoordinatorState : uint8_t {
        COORD_SCHED = 1, // 0b001 - 已调度
        COORD_CONCLUDING = 1 << 1,  // Whether its the last hop of a transaction// 0b010 - 正在结束（最后一跳）
        COORD_CANCELLED = 1 << 2,// 0b100 - 已取消（阻塞事务超时或连接关闭）
    }; 

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

    bool IsScheduled() const {
        return coordinator_state_ & COORD_SCHED;
    }
    
    void Scheduling(std::coroutine_handle<> handle, RunnableType cb); // not same


private:

    void InitTxTime();
    void InitBase(Namespace* ns, DbIndex dbid, CmdArgList args);
    void InitGlobal();
    void InitByKeys(const KeyIndex& key_index);

    OpResult<KeyIndex> DetermineKeys(const CommandId* cid, CmdArgList args);

    void ScheduleInternal();
    const CommandId* cid_ = nullptr;

    DbContext db_cntx_;

    CmdArgList full_args_;
    std::vector<IndexSlice> args_slices_; // IndexSlice from tx_base.hpp, 处理full_args_的分片事务

    Namespace* ns_ = nullptr; // 事务所属的命名空间

    bool global_ = false; // 是否是全局事务
    uint32_t unique_shard_cnt_{0};  
    uint8_t coordinator_state_ = 0; // 事务协调者状态，使用位掩码表示是否已调度、正在结束（最后一跳）或已取消（阻塞事务超时或连接关闭）




    util::EmbeddedBlockingCounter run_barrier_{0};
};




}

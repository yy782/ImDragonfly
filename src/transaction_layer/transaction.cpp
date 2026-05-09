#include "transaction.hpp"

#include "sharding/engine_shard_set.hpp"
#include "command_layer/parsed_command.hpp"
#include "sharding/engine_shard_set.hpp"
#include "detail/common_types.hpp"
namespace dfly{


Transaction::Transaction(const CommandId* cid) :
cid_(cid)
{
    args_slices_.reserve(Shard->size());
    InitTxTime();

}




OpStatus Transaction::InitByArgs(Namespace* ns, DbIndex index, ::cmn::CmdArgList args){
    InitBase(ns, index, args);


    if ((cid_->opt_mask() & CO::GLOBAL_TRANS) > 0) {
        InitGlobal();
        return OpStatus::OK;
    }


    if ((cid_->opt_mask() & CO::NO_KEY_TRANSACTIONAL) > 0) {
        EnableAllShards(); // 只支持SHUTDOWN
        return OpStatus::OK;
    }

    OpResult<KeyIndex> key_index = DetermineKeys(cid_, args);
    if (!key_index)
        return key_index.status();

    InitByKeys(*key_index);
    return OpStatus::OK; 
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
    coordinator_state_ = (coordinator_state_ | COORD_CONCLUDING);

    assert(coordinator_state_ & COORD_SCHED);




    // shard_set->Add(shard_id, [this, cb = std::move(cb), handle, shard_id]()mutable{
    //     cb(this, GetDbSlice(shard_id).shard_owner());
    //     handle.resume();
    //});
}




void Transaction::InitTxTime(){
    db_cntx_.time_now_ms_ = ::util::GetCurrentTimeMs();
}

void Transaction::InitBase(Namespace* ns, DbIndex dbid, CmdArgList args) {
    global_ = false;
    db_cntx_.db_index_ = dbid;
    full_args_ = args;
    local_result_ = OpStatus::OK;

    if (IsScheduled()) {
        // TODO check
    } else {
        ns_ = ns;
    }
}


void Transaction::InitGlobal() {
    global_ = true;
    EnableAllShards();
}


void Transaction::EnableAllShards() {
    unique_shard_cnt_ = shard_set->size();
}




OpResult<KeyIndex> Transaction::DetermineKeys(const CommandId* cid, CmdArgList args) {
    if (cid->opt_mask() & (CO::GLOBAL_TRANS | CO::NO_KEY_TRANSACTIONAL))
        return KeyIndex{};

    int num_custom_keys = -1;
    unsigned start = 0, end = 0, step = 0;
    if (cid->first_key_pos() > 0) {
        start = cid->first_key_pos() - 1;
        int8_t last = cid->last_key_pos();
        end = last > 0 ? last : (int(args.size()) + last + 1);
        step = cid->interleaved_step() ? cid->interleaved_step() : 1;
    }

    return {KeyIndex{start, end, step};}
}


void Transaction::InitByKeys(const KeyIndex& key_index) {
    if ((key_index.end_ - key_index.start_) == 0)
        return;
    args_slices_.resize(shard_set->size());

    unique_shard_cnt_ = 0;
    for (unsigned i : key_index.Range()) {
        std::string_view key = full_args_[i];
        ShardId sid = Shard(key);
        
        if (args_slices_[sid].empty()) {
            ++unique_shard_cnt_;
        }
        args_slices_[sid].push_back(i);
    }



}




void Transaction::ScheduleInternal() {
    while (true) {
        run_barrier_.Start(unique_shard_cnt_);

        
    }
}




}

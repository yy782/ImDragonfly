#include <set>
#include <unordered_set>
#include <iostream>
#include "transaction.hpp"

#include "sharding/engine_shard_set.hpp"
#include "sharding/db_slice.hpp"
#include "command_layer/cmn_types.hpp"

namespace dfly{

using facade::kInvalidKeysStart;
using facade::kInvalidKeysNum;
using facade::kInvalidKeysOffset;

Transaction::Transaction(const CommandId* cid) : cid_(cid) {
    Slices_.resize(shard_set->size());
    for (size_t i = 0; i < Slices_.size(); ++i) {
        Slices_[i].unique_shard_id = i;
        Slices_[i].tx = this;
    }
}
Transaction::~Transaction() {

}
void Transaction::InitByArgs(ConnectionContext* conn_cntx, CmdArgList args){
    conn_cntx_ = conn_cntx;
    db_cntx_ = DbContext(conn_cntx_->GetNamespace(), conn_cntx_->GetDbIndex(), util::GetCurrentTimeMs()); 
    cmd_cntx_ = CommandContext(conn_cntx_, this, cid_);
    InitKeys(args);
}
void Transaction::InitKeys(::cmn::CmdArgList args) {
    full_args_ = args;
    std::set<size_t> shards;
    for (size_t i = cid_->keys_start(); i < args.size(); i += cid_->keys_offset()) {
        std::string_view key = args[i];
        ShardId sid = Shard(key, shard_set->size());
        Slices_[sid].keyIds.push_back(i);
        Slices_[sid].lists = full_args_;
        shards.insert(sid); 
        ++key_num_;
    }
    shard_id_cnt_ = shards.size();

}
    

void Transaction::Scheduling(std::coroutine_handle<> handle, RunnableType cb) {   
    auto done_cnt_ptr = std::make_shared<std::atomic<uint32_t>>(shard_id_cnt_);
    for (const auto& slice : Slices_) {
        if (slice.keyIds.empty()) continue;
        shard_set->Add(slice.unique_shard_id, [this, cb = std::move(cb), handle, &slice, done_cnt_ptr] () mutable {
            cb(this, GetDbSlice(slice.unique_shard_id).shard_owner());
            --(*done_cnt_ptr);
            if ((*done_cnt_ptr) == 0) {
                handle.resume();
            }
        });
    }
}

void Transaction::QueueCommand(const CommandId* cid, CmdArgList args) {
    std::vector<std::string> VecArgs(args.begin(), args.end());
    std::vector<std::string_view> ViewArgs = std::vector<std::string_view>(VecArgs.begin(), VecArgs.end());
    queued_commands_.push_back({cid, std::move(VecArgs), std::move(ViewArgs)});
}


}
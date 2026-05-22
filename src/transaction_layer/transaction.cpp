#include <set>
#include "transaction.hpp"

#include "sharding/engine_shard_set.hpp"

#include "sharding/engine_shard_set.hpp"
#include "sharding/db_slice.hpp"
#include "command_layer/cmn_types.hpp"
namespace dfly{

using facade::kInvalidKeysStart;
using facade::kInvalidKeysNum;
using facade::kInvalidKeysOffset;

Transaction::Transaction(const CommandId* cid) :
cid_(cid)
{
    Slices_.resize(shard_set->size());
    for (size_t i = 0; i < Slices_.size(); ++i) {
        Slices_[i].unique_shard_id = i;
        Slices_[i].tx = this;
    }
}
void Transaction::InitByArgs(Namespace* ns, DbIndex index, ::cmn::CmdArgList args){
    ns_ = ns;
    db_cntx_ = DbContext(ns, index, util::GetCurrentTimeMs()); 
    InitKeys(args);
}
void Transaction::InitKeys(::cmn::CmdArgList args) {
    full_args_ = args;
    std::set<size_t> shards;
    for (size_t i = cid_->keys_start(); i < args.size(); i += cid_->keys_offset()) {
        std::string_view key = args[i];
        ShardId sid = Shard(key);
        Slices_[sid].keyIds.push_back(i);
        Slices_[sid].lists = full_args_;
        shards.insert(sid); 
        ++key_num_;
    }
    shard_id_cnt_ = shards.size();

}
    

void Transaction::Scheduling(std::coroutine_handle<> handle, RunnableType cb) // not same 
{   
    auto done_cnt_ptr = std::make_shared<std::atomic<uint32_t>>(shard_id_cnt_);
    for (const auto& slice : Slices_) {
        if (slice.keyIds.empty()) continue;
        shard_set->Add(slice.unique_shard_id, [this, cb = std::move(cb), handle, &slice, done_cnt_ptr]()mutable{
            cb(this, GetDbSlice(slice.unique_shard_id).shard_owner());
            --(*done_cnt_ptr);
            if ((*done_cnt_ptr) == 0) {
                handle.resume();
            }
        });
    }
}





}
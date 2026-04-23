
#pragma once

#include <thread>
#include <vector>
#include <functional>
#include "sharding/engine_shard_set.hpp"
#include "sharding/cluster_support.hpp"
#include "network/facade/redis_parser.hpp"
#include "network/facade/reply_builder.hpp"
#include "sharding/namespaces.hpp"
#include "detail/tx_base.hpp"
#include "sharding/db_slice.hpp"
namespace dfly{

class Common{
public:

    Common(const DbContext& db_context) : db_context_(db_context) {}

    std::string HandleSet(const std::string& key, const std::string& value) {
        ShardId sid = KeySlot(std::string_view(key));

        shard_set->Await(sid, [&]() {
            auto* shard = EngineShard::tlocal();

            db_context_.GetDbSlice(shard->shard_id())
                                            .AddNew(db_context_, std::string_view(key), 
                                            CompactValue(std::string_view(value)), 0);
        });
        
        return BuildString("OK");
    }

    std::string HandleGet(const std::string& key) {
        ShardId sid = KeySlot(std::string_view(key));
        
        OpResult<DbSlice::ItAndUpdater> result;
        
        shard_set->Await(sid, [&]() {
            auto* shard = EngineShard::tlocal();
            result=db_context_.GetDbSlice(shard->shard_id())
                                            .AddOrFind(db_context_, std::string_view(key),  
                                            std::nullopt);
        });
        
        
        auto res=result.value();
        return BuildBulkString(res.it_->second.ToString());

    }

    std::string HandleDel(const std::vector<std::string>& args) {
        std::atomic<int> deleted{0};
        
        for (size_t i = 1; i < args.size(); i++) {
            auto& key = args[i];
            ShardId sid = KeySlot(std::string_view(key));
            shard_set->Await(sid, [&, key = args[i]]() {
                auto* shard = EngineShard::tlocal();
                auto it=db_context_.GetDbSlice(shard->shard_id())
                                    .AddOrFind(db_context_, std::string_view(key),  std::nullopt)
                                    .value().it_;                    
                db_context_.GetDbSlice(shard->shard_id()).Del(db_context_, it);
                ++deleted;
                                
            });
        }
        return BuildInteger(deleted.load());
    }

private:
    DbContext db_context_;


};







}
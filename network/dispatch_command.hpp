


#include <thread>
#include <vector>
#include <functional>
#include "EngineShardSet.hpp"
#include "cluster_support.hpp"
#include "facade/redis_parser.hpp"
#include "facade/reply_builder.hpp"
#include "src/include/namespaces.hpp"


namespace dfly{

class Common{
public:

    Common(const DbContext& db_context) : db_context_(db_context) {}

    std::string HandleSet(const std::string& key, const std::string& value) {
        ShardId sid = KeySlot(std::string_view(key));
        
        // 使用 BlockingCounter 等待完成
        util::fb2::BlockingCounter bc(1);

        
        shard_set->Add(sid, [&]() {
            auto* shard = EngineShard::tlocal();

            db_context_.GetDbSlice(shard->shard_id())
                                            .AddNew(db_cntext_, key, value, 0);
            bc.Dec();
        });
        
        bc.Wait();
        return BuildString("OK");
    }

    std::string HandleGet(const std::string& key) {
        ShardId sid = KeySlot(std::string_view(key));
        
        std::optional<std::string> result;
        util::fb2::BlockingCounter bc(1);
        
        shard_set->Add(sid, [&]() {
            auto* shard = EngineShard::tlocal();
            db_context_.GetDbSlice(shard->shard_id())
                                            .AddOrFind(db_cntext_, key,  std::nullopt);
            bc.Dec();
        });
        
        bc.Wait();
        
        if (result.has_value()) {
            return BuildBulkString(*result);
        } else {
            return BuildNull();
        }
    }

    std::string HandleDel(const std::vector<std::string>& args) {
        std::atomic<int> deleted{0};
        util::fb2::BlockingCounter bc(args.size() - 1);
        
        for (size_t i = 1; i < args.size(); i++) {
            ShardId sid = KeySlot(std::string_view(key));
            shard_set->Add(sid, [&, key = args[i]]() {
                auto* shard = EngineShard::tlocal();
                auto it=db_context_.GetDbSlice(shard->shard_id())
                                    .AddOrFind(db_cntext_, key,  std::nullopt)
                                    .value().it_;                    
                db_context_.GetDbSlice(shard->shard_id()).Del(db_context_, it);
                ++deleted;
                                   
                bc.Dec();
            });
        }
        
        bc.Wait();
        return BuildInteger(deleted.load());
    }

private:
    DbContext db_cntext_;


};







}
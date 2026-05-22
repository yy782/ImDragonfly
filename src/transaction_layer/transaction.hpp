
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
class RedisSession;

class Transaction{
public:

    using RunnableType = util::FunctionRef<void(Transaction*, EngineShard*)>;
 

    explicit Transaction(const CommandId* cid);

    void InitByArgs(Namespace* ns, DbIndex index, CmdArgList args);
    void InitKeys(::cmn::CmdArgList args);



    // OpArgs GetOpArgs(EngineShard* shard) const;

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
    uint32_t GetKeyNum() {
        return key_num_;
    }

    
    void Scheduling(std::coroutine_handle<> handle, RunnableType cb); // not same


    RedisSession*& debug_owner() {return owner_;}

    struct Slice{
        ShardId unique_shard_id;
        Transaction* tx;
        std::vector<uint32_t> keyIds;
        CmdArgList lists;
        DbSlice& GetDbSlice() {
            return tx->GetDbSlice(unique_shard_id);
        }
        DbContext& GetDbContext() {
            return tx->GetDbContext();
        } 
        const DbSlice& GetDbSlice() const {
            return tx->GetDbSlice(unique_shard_id);
        }
        const DbContext& GetDbContext() const {
            return tx->GetDbContext();
        } 
        struct Iterator {
            using ArgsIndexPair = std::pair<std::string_view, uint32_t>;
            using iterator_category = std::input_iterator_tag;
            using value_type = ArgsIndexPair;
            using difference_type = ptrdiff_t;
            using pointer = value_type*;
            using reference = value_type&;

            
            ArgsIndexPair pa;
            const Slice* slice;
            Iterator(std::string_view key, uint32_t keyId, const Slice* sl) : pa(key, keyId), slice(sl) {}
            bool operator==(const Iterator& o) const {
            return pa == o.pa;
            }

            bool operator!=(const Iterator& o) const {
                return !(*this == o);
            }
            ArgsIndexPair& operator*() {
                return pa;
            }            
            ArgsIndexPair* operator->() {
                return &pa;
            }
            Iterator& operator++() {
                if (pa.second + 1 >= slice->keyIds.size()) {
                    *this = slice->cend();
                    return *this;
                }else {
                    uint32_t nextId = slice->keyIds[pa.second + 1];
                    std::string_view key = slice->lists[nextId];
                    pa = ArgsIndexPair(key, nextId);
                    return *this;                    
                }
            }      
        };

        Iterator cbegin() const {
            if (keyIds.empty()) {
                return cend();
            }else {
               uint32_t keyId = keyIds[0];
               std::string_view key = lists[keyId]; 
               return Iterator(key, keyId, this);
            }
        }

        Iterator cend() const {
            uint32_t keyId = lists.size();
            std::string_view key;
            return Iterator(key, keyId, this);
        }

        Iterator begin() const {
            return cbegin();
        }

        Iterator end() const {
            return cend();
        }   

    };

    Slice& GetSlice(ShardId id) {
        assert(id < Slices_.size());
        return Slices_[id];
    }
private:


    
    const CommandId* cid_;
    CmdArgList full_args_;
    Namespace* ns_; // 事务所属的命名空间
    




    DbContext db_cntx_= {};
    std::vector<Slice> Slices_;
    ShardId shard_id_cnt_ = 0;
    uint32_t key_num_ = 0;



    // For DEBUG
    RedisSession* owner_;

};


}




















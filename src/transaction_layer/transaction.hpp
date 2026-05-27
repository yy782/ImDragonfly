
#pragma once
#include <cstdint>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <span> 
#include <utility>
#include <coroutine>
#include <variant>
#include <gtest/gtest.h>
#include "detail/tx_base.hpp"
#include "detail/common_types.hpp"
#include "util/function.hpp"
#include "command_layer/command_registry.hpp"
#include "command_layer/cmn_types.hpp"
#include "detail/conn_context.hpp"
#include "sharding/op_status.hpp"

namespace dfly{
using ::cmn::CmdArgList;

class CommandId;
class Namespace;
class RedisSession;
class EngineShard;

class Transaction{
public:

    enum class State {
        IDLE,           // 空闲状态，未开启事务
        MULTI,          // MULTI 已执行，正在收集命令
        EXEC           // EXEC 已执行，正在执行事务
    };

    struct QueuedCommand {
        const CommandId* cid;
        std::vector<std::string> args;
        std::vector<std::string_view> ViewArgs;
        CmdArgList GetCmdArgList() const {
            CmdArgList argsList(ViewArgs);
            return argsList;
        }
    };

    using RunnableType = util::FunctionRef<void(Transaction*, EngineShard*)>;

    explicit Transaction(const CommandId* cid);
    ~Transaction();
    void InitByArgs(ConnectionContext* conn_cntx, CmdArgList args);


    const DbContext& GetDbContext() const {
        return db_cntx_;
    }   
    DbContext& GetDbContext() {
        return db_cntx_;
    }  
    const DbSlice& GetDbSlice(ShardId shard_id) const {
        return db_cntx_.GetDbSlice(shard_id);
    }
    ConnectionContext* GetConnectionContext() {
        return conn_cntx_;
    }

    DbSlice& GetDbSlice(ShardId shard_id) {
        return db_cntx_.GetDbSlice(shard_id);   
    }
    CommandContext& GetCommandContext() {
        return cmd_cntx_;
    }
    const CommandContext& GetCommandContext() const {
        return cmd_cntx_;
    }    
    
    DbIndex GetDbIndex() {
        return db_cntx_.GetDbIndex();    
    }
    uint32_t GetKeyNum() {
        return key_num_;
    }


    
    void Scheduling(std::coroutine_handle<> handle, RunnableType cb); // not same

    State GetState() const { return state_; }
    void SetState(State new_state) { state_ = new_state; }

    void QueueCommand(const CommandId* cid, CmdArgList args);
    const std::vector<QueuedCommand>& GetQueuedCommands() const { return queued_commands_; }
    void ClearQueuedCommands() { queued_commands_.clear(); }

    template<typename Cb>
    void AddWatchKey(std::string_view key, Cb&& cb) { return conn_cntx_->AddWatchKey(key, std::move(cb)); }

    auto ClearWatchKeys() { conn_cntx_->ClearWatchKeys(); }

    const std::unordered_set<std::string_view>& GetWatchKeys() const { return conn_cntx_->GetWatchKeys(); }
    bool HasWatchKeys() const { return conn_cntx_->HasWatchKeys(); }
    
    bool IsDirty() const { return conn_cntx_->IsDirty(); }

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
            uint32_t idx;
            Iterator(std::string_view key, uint32_t keyId, const Slice* sl, uint32_t id) : pa(key, keyId), slice(sl), idx(id) {}
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
                if (idx + 1 >= slice->keyIds.size()) {
                    *this = slice->cend();
                    return *this;
                }else {
                    uint32_t nextId = slice->keyIds[++idx];
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
               return Iterator(key, keyId, this, 0);
            }
        }

        Iterator cend() const {
            uint32_t keyId = lists.size();
            std::string_view key;
            return Iterator(key, keyId, this, keyIds.size());
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
    template<typename... Args>
    Transaction*& CreateSubTransaction(Args&&... args) {
        SubTransactions_.emplace_back(new Transaction(std::forward<Args>(args)...));
        SubTransactions_.back()->setFatherTransaction(this);
        return SubTransactions_.back(); // 可能会扩容失效
    }
    void ClearSubTransaction() {
        for (auto& t : SubTransactions_) {
            delete t;
        }
        SubTransactions_.clear();
    }
    bool collectMultiRes(const std::string& s) {
        MultiRes_.push_back(s);
        return MultiReady();
    }
    void ClearMultiRes() {
        MultiRes_.clear();
    }
    std::vector<std::string> SwapOrClearMultiRes(std::vector<std::string> vec = {}) {
        MultiRes_.swap(vec);
        return vec;
    }
    bool MultiReady() {
        EXPECT_TRUE(MultiRes_.size() <= queued_commands_.size());
        return MultiRes_.size() == queued_commands_.size();
    }



    void startMulti() { // 这里没有清空状态
        SetState(Transaction::State::MULTI);
    }
    void FinishOrDiscardMulti() {
        SetState(Transaction::State::IDLE);
        ClearQueuedCommands();
        ClearWatchKeys();
        ClearMultiRes();
        ClearSubTransaction();
    }

    void setFatherTransaction(Transaction* fa) {
        fa_ = fa;
    }

private:
    void InitKeys(::cmn::CmdArgList args);
    const CommandId* cid_;
    CmdArgList full_args_;

    DbContext db_cntx_;
    CommandContext cmd_cntx_;
    std::vector<Slice> Slices_;
    ShardId shard_id_cnt_ = 0;
    uint32_t key_num_ = 0;

    State state_ = State::IDLE;
    std::vector<QueuedCommand> queued_commands_;
    std::vector<std::string> MultiRes_;
    std::vector<Transaction*> SubTransactions_; 
    Transaction* fa_;

    ConnectionContext* conn_cntx_;

};


}




















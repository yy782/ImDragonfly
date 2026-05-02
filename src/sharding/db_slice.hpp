#pragma once

#include "db_table.hpp"
#include "op_status.hpp"
#include "string_or_view.hpp"
#include "tx_base.hpp"


namespace dfly{
using namespace cmn;
class EngineShard;
class DbSlice 
{
public:    
    template <typename T> 
    class IteratorT {
    public:
        IteratorT() = default;
        IteratorT(T it, StringOrView key) : 
        it_(it), 
        key_(std::move(key)) 
        {}
        
        // 核心方法：在访问前自动清洁迭代器
        const T& GetInnerIt() const {
            LaunderIfNeeded();
            return it_;
        }
        
        // 重载操作符，自动清洁
        auto operator->() const {
            return GetInnerIt().operator->();
        }
        
        bool is_done() const {
            return GetInnerIt().is_done();
        }
        
        std::string_view key() const {
            return key_.view();
        }
        
    private:
        void LaunderIfNeeded() const;  // 清洁逻辑
        
        mutable T it_;                          // 底层迭代器
        StringOrView key_;                      // 对应的 key（用于重新查找）
    };

    using Iterator = IteratorT<PrimeIterator>;
    using ConstIterator = IteratorT<PrimeConstIterator>; 
    using Context = DbContext;   
    struct ItAndUpdater 
    {
        Iterator it_;
        bool is_new_ = false;
    };


    DbSlice(uint32_t index, bool cache_mode, EngineShard* owner);
    ~DbSlice();

    DbSlice(const DbSlice&) = delete;
    void operator=(const DbSlice&) = delete;


    EngineShard* shard_owner() const {
        return owner_;
    }

    ShardId shard_id() const {
        return shard_id_;
    }

    void PerformDeletionAtomic(const Iterator& del_it, DbTable* table); // 实际的删除函数

    ItAndUpdater FindMutable(const Context& cntx, std::string_view key); // Iterator it：指向 key 的迭代器（可修改）
    ConstIterator FindReadOnly(const Context& cntx, std::string_view key) const; // 查找 key，返回只读迭代器
    OpResult<ItAndUpdater> AddOrFind(const Context& cntx, std::string_view key, 
                                    std::optional<unsigned> req_obj_type); // 如果 key 存在就返回它，不存在就创建空值并返回。
    OpResult<ItAndUpdater> AddOrUpdate(const Context& cntx, std::string_view key, PrimeValue obj,
                                        uint64_t expire_at_ms);
    OpResult<ItAndUpdater> AddNew(const Context& cntx, std::string_view key, PrimeValue obj,
                                    uint64_t expire_at_ms);

    void Del(Context cntx, Iterator it, DbTable* db_table = nullptr);
    void DelMutable(Context cntx, ItAndUpdater it_updater); // 通过 FindMutable 找到 key 后删除
    bool IsDbValid(DbIndex id) const { return id < db_arr_.size() && bool(db_arr_[id]); } 


    facade::OpResult<void> UpdateExpire(const Context& cntx, Iterator prime_it,
                                        int64_t sec);    
    void AddExpire(DbIndex db_ind, const Iterator& main_it, uint64_t at);

    bool RemoveExpire(DbIndex db_ind, const Iterator& main_it);
    Iterator ExpireIfNeeded(const Context& cntx, Iterator it) const;
    PrimeIterator ExpireIfNeeded(const Context& cntx, PrimeIterator it) const;    
    void ExpireAllIfNeeded();




private:
    enum class UpdateStatsMode : uint8_t {
        kReadStats,
        kMutableStats,
    };
    OpResult<ItAndUpdater> AddOrFindInternal(const Context& cntx, std::string_view key,
                                            std::optional<unsigned> req_obj_type); // 获取或创建一个 key

    OpResult<PrimeIterator> FindInternal(const Context& cntx, std::string_view key,
                                        std::optional<unsigned> req_obj_type,
                                        UpdateStatsMode stats_mode) const;
    OpResult<ItAndUpdater> FindMutableInternal(const Context& cntx, std::string_view key,
                                             std::optional<unsigned> req_obj_type);
    OpResult<ItAndUpdater> AddOrUpdateInternal(const Context& cntx,
                                                            std::string_view key, PrimeValue obj,
                                                            uint64_t expire_at_ms);         
                                                            
                                                            


    void CreateDb(DbIndex index);


    ShardId shard_id_;
    EngineShard* owner_;
    DbTableArray db_arr_;
};


template <typename T> 
void DbSlice::IteratorT<T>::LaunderIfNeeded() const {
     //  如果底层迭代器本身已无效，直接返回
    if (!IsValid(it_)) {
        return;
    }

    if (!it_.IsOccupied() || it_->first != key_.view()) {
        //  迭代器已失效，根据原 key 重新查找
        it_ = it_.owner().Find(key_.view());
    }
\
}
}  // namespace dfly

#include "db_table.hpp"
#include "op_status.hpp"
#include "detail/string_or_view.hpp"
#include "detail/tx_base.hpp"

#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"
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
        fiber_epoch_(util::fb2::FiberSwitchEpoch()), key_(std::move(key)) 
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
        mutable uint64_t fiber_epoch_ = 0;     // 上次清洁时的纤程切换计数
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

    void PerformDeletionAtomic(const Iterator& del_it, DbTable* table, bool async = false); // 实际的删除函数

    ItAndUpdater FindMutable(const Context& cntx, std::string_view key); // Iterator it：指向 key 的迭代器（可修改）
    ConstIterator FindReadOnly(const Context& cntx, std::string_view key) const; // 查找 key，返回只读迭代器
    OpResult<ItAndUpdater> AddOrFind(const Context& cntx, std::string_view key, 
                                    std::optional<unsigned> req_obj_type); // 如果 key 存在就返回它，不存在就创建空值并返回。
    OpResult<ItAndUpdater> AddOrUpdate(const Context& cntx, std::string_view key, PrimeValue obj,
                                        uint64_t expire_at_ms);
    OpResult<ItAndUpdater> AddNew(const Context& cntx, std::string_view key, PrimeValue obj,
                                    uint64_t expire_at_ms);

  void Del(Context cntx, Iterator it, DbTable* db_table = nullptr, bool async = false);
  void DelMutable(Context cntx, ItAndUpdater it_updater); // 通过 FindMutable 找到 key 后删除
  bool IsDbValid(DbIndex id) const { return id < db_arr_.size() && bool(db_arr_[id]); } 
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
                                                            uint64_t expire_at_ms,
                                                            bool force_update);         
                                                            
                                                            


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

    uint64_t current_epoch = util::fb2::FiberSwitchEpoch();//  获取当前的“纤程切换纪元”

    if (current_epoch != fiber_epoch_) {
        if (!it_.IsOccupied() || it_->first != key_.view()) {
            //  迭代器已失效，根据原 key 重新查找
            it_ = it_.owner().Find(key_.view());
        }
        fiber_epoch_ = current_epoch;
    }
}
}  // namespace dfly

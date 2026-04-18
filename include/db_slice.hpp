#include "DbTable.hpp"
#include "op_status.hpp"
#include "string_or_view.hpp"

using namespace cmn;
class EngineShard;
class DbSlice 
{
    
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
    struct ItAndUpdater 
    {
        Iterator it;
        bool is_new = false;
    };
    DbSlice(const DbSlice&) = delete;
    void operator=(const DbSlice&) = delete;
    ItAndUpdater FindMutable(std::string_view key); // Iterator it：指向 key 的迭代器（可修改）
    ConstIterator FindReadOnly(std::string_view key) const; // 查找 key，返回只读迭代器
    OpResult<ItAndUpdater> AddOrFind(std::string_view key, 
                                    std::optional<unsigned> req_obj_type); // 如果 key 存在就返回它，不存在就创建空值并返回。
    OpResult<ItAndUpdater> AddOrUpdate(std::string_view key, PrimeValue obj,
                                        uint64_t expire_at_ms);
    OpResult<ItAndUpdater> AddNew(std::string_view key, PrimeValue obj,
                                    uint64_t expire_at_ms);

  void Del(Iterator it, DbTable* db_table = nullptr, bool async = false);
  void DelMutable(ItAndUpdater it_updater); // 通过 FindMutable 找到 key 后删除
private:
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

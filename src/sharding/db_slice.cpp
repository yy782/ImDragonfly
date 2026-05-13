#include "db_slice.hpp"
#include <optional>
#include "engine_shard.hpp"
#include "util/Time.hpp"
namespace dfly{ 


DbSlice::DbSlice(uint32_t index, bool cache_mode, EngineShard* owner)
    : shard_id_(index),
      owner_(owner) {

    (void)cache_mode;

    db_arr_.emplace_back();
    CreateDb(0);
}

DbSlice::~DbSlice() {
    // we do not need this code but it's easier to debug in case we encounter
    // memory allocation bugs during delete operations.

    for (auto& db : db_arr_) {
        if (!db)
            continue;
        db.reset();
    }

    // AsyncDeleter::Shutdown();
}



DbSlice::ItAndUpdater DbSlice::FindMutable(const Context& cntx, std::string_view key) {
  return std::move(FindMutableInternal(cntx, key, std::nullopt).value());
}
DbSlice::ConstIterator DbSlice::FindReadOnly(const Context& cntx, std::string_view key) const {

    

    auto res = FindInternal(cntx, key, std::nullopt, UpdateStatsMode::kReadStats);
    return {*res, StringOrView::FromView(key)};
}
OpResult<DbSlice::ItAndUpdater> DbSlice::FindMutableInternal(const Context& cntx, std::string_view key,
                                                             std::optional<unsigned> req_obj_type) {
    auto res = FindInternal(cntx, key, req_obj_type, UpdateStatsMode::kMutableStats);
    if (!res.ok()) {
        return res.status();
    }

    auto it = Iterator(*res, StringOrView::FromView(key));
    
    // PreUpdate() might have caused a deletion of `it`
    if (res->IsOccupied()) {
        // 有效，继续
        return {{it}};
    } else {
        return OpStatus::KEY_NOTFOUND;
    }
}
auto DbSlice::FindInternal(const Context& cntx, std::string_view key, std::optional<unsigned> req_obj_type,
                           UpdateStatsMode stats_mode) const -> OpResult<PrimeIterator> {

    std::cout << this->shard_id_  << "Find" << std::endl;

    if (!IsDbValid(cntx.db_index_)) {  // Can it even happen?
        return OpStatus::KEY_NOTFOUND;
    }

    auto& db = *db_arr_[cntx.db_index_];
    PrimeIterator it = db.prime_.Find(key);
    int miss_weight = (stats_mode == UpdateStatsMode::kReadStats);

    if (!IsValid(it)) {
        return OpStatus::KEY_NOTFOUND;
    }

    if (req_obj_type.has_value() && it->second.ObjType() != req_obj_type.value()) {
        return OpStatus::WRONG_TYPE;
    }
    auto& pv = it->second;


    (void)pv;
    (void)miss_weight;

    return it;
}


facade::OpResult<DbSlice::ItAndUpdater> DbSlice::AddOrFind(const Context& cntx, std::string_view key,
                                                   std::optional<unsigned> req_obj_type) {

  

  return AddOrFindInternal(cntx, key, req_obj_type);
}
facade::OpResult<DbSlice::ItAndUpdater> DbSlice::AddOrUpdate(const Context& cntx, std::string_view key,
                                                     PrimeValue obj, uint64_t expire_at_ms) {
    return AddOrUpdateInternal(cntx, key, std::move(obj), expire_at_ms);
}

facade::OpResult<DbSlice::ItAndUpdater> DbSlice::AddNew(const Context& cntx, std::string_view key,
                                                PrimeValue obj, uint64_t expire_at_ms) {
    auto op_result = AddOrUpdateInternal(cntx, key, std::move(obj), expire_at_ms);
    auto& res = *op_result;
    return DbSlice::ItAndUpdater{.it_ = res.it_};
}

facade::OpResult<DbSlice::ItAndUpdater> DbSlice::AddOrFindInternal(const Context& cntx, std::string_view key,
                                                           std::optional<unsigned> req_obj_type) {

    DbTable& db = *db_arr_[cntx.db_index_];
    auto res = FindInternal(cntx, key, req_obj_type, UpdateStatsMode::kMutableStats);

    if (res.ok()) {
        Iterator it(*res, StringOrView::FromView(key));
        if (res->IsOccupied()) {
            return ItAndUpdater{.it_ = it, .is_new_ = false};
        } else {
            res = OpStatus::KEY_NOTFOUND;
        }
    } else if (res == OpStatus::WRONG_TYPE) {
        return OpStatus::WRONG_TYPE;
    }
    auto status = res.status();
    PrimeIterator it;
    try {
        std::cout << this->shard_id_ << "InsertNew" << std::endl;
        it = db.prime_.InsertNew(key, PrimeValue{});
    } catch (std::bad_alloc& e) {
        return OpStatus::WRONG_TYPE; // 这里的错误类型不太准确，但我们没有更合适的选项了
    }

    (void)status;

    return ItAndUpdater{
        .it_ = Iterator(it, StringOrView::FromView(key)),
        .is_new_ = true};
}


facade::OpResult<DbSlice::ItAndUpdater> DbSlice::AddOrUpdateInternal(const Context& cntx,
                                                             std::string_view key, PrimeValue obj,
                                                             uint64_t expire_at_ms) {
    auto op_result = AddOrFind(cntx, key, std::nullopt);
    
    if(op_result.status() != OpStatus::OK)
    {
        return op_result.status();
    }


    auto& res = *op_result;
    if (!res.is_new_ ) // not same 
        return op_result;

    auto& it = res.it_;
    it->second = std::move(obj);

    if (expire_at_ms) {
        AddExpire(cntx.db_index_, it, expire_at_ms);
    } else {
        RemoveExpire(cntx.db_index_, it);
    }

    return op_result;
}


void DbSlice::Del(Context cntx, Iterator it, DbTable* db_table) {
    DbTable* table = db_table ? db_table : db_arr_[cntx.db_index_].get();
    // auto obj_type = it->second.ObjType();


    PerformDeletionAtomic(it, table); // 执行实际删除
}

void DbSlice::DelMutable(Context cntx, ItAndUpdater it_updater) {
    Del(cntx, it_updater.it_);
}

void DbSlice::PerformDeletionAtomic(const Iterator& del_it, DbTable* table) {
    table->prime_.Erase(del_it.GetInnerIt()); // 执行实际删除
}



void DbSlice::CreateDb(DbIndex db_ind) {
    auto& db = db_arr_[db_ind];
    if (!db) {
        db.reset(new DbTable{owner_->memory_resource(), db_ind});
    }
}


facade::OpResult<void> DbSlice::UpdateExpire(const Context& cntx, Iterator main_it,
                                    int64_t sec){
    (void)cntx;
    main_it->first.SetExpireTime(sec);                                    

   return {OpStatus::OK};                                     
}

void DbSlice::AddExpire(DbIndex db_ind, const Iterator& main_it, uint64_t at) {
    
    (void)db_ind;

    main_it->first.SetExpireTime(at);
}

bool DbSlice::RemoveExpire(DbIndex db_ind, const Iterator& main_it) {

    (void)db_ind;

    if (!main_it->first.HasExpire())
        return false;

    main_it->first.ClearExpireTime();

    return true;
}













DbSlice::Iterator DbSlice::ExpireIfNeeded(const Context& cntx, Iterator it) const {
    return Iterator::FromPrime(ExpireIfNeeded(cntx, it.GetInnerIt()));
}

PrimeIterator DbSlice::ExpireIfNeeded(const Context& cntx, PrimeIterator it) const {
    (void)cntx;
    if (!it->first.HasExpire()) {
        return it;
    }

    int64_t expire_time = it->first.GetExpireTime();

    if (int64_t(cntx.time_now_ms_) < expire_time ) {
        return it;
    }

    std::string scratch;
    std::string_view key = it->first.GetSlice(&scratch);

    auto& db = db_arr_[cntx.db_index_];
    const_cast<DbSlice*>(this)->PerformDeletionAtomic(Iterator(it, StringOrView::FromView(key)),
                                                        db.get());

    return PrimeIterator{};
}

void DbSlice::ExpireAllIfNeeded() {

    for (DbIndex db_index = 0; db_index < db_arr_.size(); db_index++) {
        if (!db_arr_[db_index])
            continue;
        auto& db = *db_arr_[db_index];

        auto cb = [&](PrimeTable::iterator prime_it) {
            if (prime_it->first.HasExpire()) {
                ExpireIfNeeded(Context{nullptr, db_index, util::GetCurrentTimeMs()}, prime_it);
            }
        };

        PrimeTable::Cursor cursor;
        do {
            cursor = db.prime_.Traverse(cursor, cb);
        } while (cursor);
    }
}









}  // namespace dfly




































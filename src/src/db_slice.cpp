#include "db_slice.hpp"
#include <optional>
namespace dfly{ 
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
    if (!IsDbValid(cntx.db_index)) {  // Can it even happen?
        return OpStatus::KEY_NOTFOUND;
    }

    auto& db = *db_arr_[cntx.db_index];
    PrimeIterator it = db.prime_.Find(key);
    int miss_weight = (stats_mode == UpdateStatsMode::kReadStats);

    if (!IsValid(it)) {
        return OpStatus::KEY_NOTFOUND;
    }

    if (req_obj_type.has_value() && it->second.ObjType() != req_obj_type.value()) {
        return OpStatus::WRONG_TYPE;
    }
    auto& pv = it->second;
    return it;
}


OpResult<DbSlice::ItAndUpdater> DbSlice::AddOrFind(const Context& cntx, std::string_view key,
                                                   std::optional<unsigned> req_obj_type) {
  return AddOrFindInternal(cntx, key, req_obj_type);
}
OpResult<DbSlice::ItAndUpdater> DbSlice::AddOrUpdate(const Context& cntx, std::string_view key,
                                                     PrimeValue obj, uint64_t expire_at_ms) {
    return AddOrUpdateInternal(cntx, key, std::move(obj), expire_at_ms, true);
}

OpResult<DbSlice::ItAndUpdater> DbSlice::AddNew(const Context& cntx, std::string_view key,
                                                PrimeValue obj, uint64_t expire_at_ms) {
    auto op_result = AddOrUpdateInternal(cntx, key, std::move(obj), expire_at_ms, false);
    auto& res = *op_result;
    return DbSlice::ItAndUpdater{.it = res.it};
}

OpResult<DbSlice::ItAndUpdater> DbSlice::AddOrFindInternal(const Context& cntx, std::string_view key,
                                                           std::optional<unsigned> req_obj_type) {

    DbTable& db = *db_arr_[cntx.db_index];
    auto res = FindInternal(cntx, key, req_obj_type, UpdateStatsMode::kMutableStats);

    if (res.ok()) {
        Iterator it(*res, StringOrView::FromView(key));
        if (res->IsOccupied()) {
            return ItAndUpdater{.it = it, .is_new = false};
        } else {
        res = OpStatus::KEY_NOTFOUND;
        }
    } else if (res == OpStatus::WRONG_TYPE) {
        return OpStatus::WRONG_TYPE;
    }
    auto status = res.status();
    PrimeIterator it;
    try {
        it = db.prime_.InsertNew(key, PrimeValue{});
    } catch (std::bad_alloc& e) {
        return OpStatus::OUT_OF_MEMORY;
    }
    return ItAndUpdater{
        .it = Iterator(it, StringOrView::FromView(key)),
        .is_new = true};
}


OpResult<DbSlice::ItAndUpdater> DbSlice::AddOrUpdateInternal(const Context& cntx,
                                                             std::string_view key, PrimeValue obj,
                                                             uint64_t expire_at_ms,
                                                             bool force_update) {
    auto op_result = AddOrFind(cntx, key, std::nullopt);
    
    if(op_result.status() != OpStatus::OK)
    {
        return op_result.status();
    }


    auto& res = *op_result;
    if (!res.is_new && !force_update) 
        return op_result;

    auto& it = res.it;
    it->second = std::move(obj);
    return op_result;
}


void DbSlice::Del(Context cntx, Iterator it, DbTable* db_table, bool async) {
    DbTable* table = db_table ? db_table : db_arr_[cntx.db_index].get();
    auto obj_type = it->second.ObjType();


    PerformDeletionAtomic(it, table, async); // 执行实际删除
}

void DbSlice::DelMutable(Context cntx, ItAndUpdater it_updater) {
    Del(cntx, it_updater.it);
}

void DbSlice::PerformDeletionAtomic(const Iterator& del_it, DbTable* table, bool async) {
    util::FiberAtomicGuard guard; // 确保删除操作在纤程中是原子的
    table->prime_.Erase(del_it.GetInnerIt()); // 执行实际删除
}

}  // namespace dfly




































#include "db_slice.hpp"
#include <optional>
#include "engine_shard.hpp"
#include "Time.hpp"
#include "detail/conn_context.hpp"
#include <assert.h>
#include <exception>
namespace dfly{ 


DbSlice::DbSlice(uint32_t index, bool cache_mode, EngineShard* owner)
    : shard_id_(index),
      owner_(owner) {

    (void)cache_mode;

    db_arr_.emplace_back();
    CreateDb(0);
}

DbSlice::~DbSlice() {
    assert(std::uncaught_exceptions() == 0);
    for (auto& db : db_arr_) {
        if (!db)
            continue;
        db.reset();
    }
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

    if (res->IsOccupied()) {
        return {{it, AutoUpdater(cntx.GetDbIndex(), key, it, this)}};
    } else {
        return OpStatus::KEY_NOTFOUND;
    }
}
auto DbSlice::FindInternal(const Context& cntx, std::string_view key, std::optional<unsigned> req_obj_type,
                           UpdateStatsMode stats_mode) const -> OpResult<PrimeIterator> {
    (void)stats_mode;
    if (!IsDbValid(cntx.GetDbIndex())) {
        return OpStatus::KEY_NOTFOUND;
    }

    auto& db = *db_arr_[cntx.GetDbIndex()];
    PrimeIterator it = db.prime_.Find(key);

    
    if (!IsValid(it)) {
        return OpStatus::KEY_NOTFOUND;
    }

    if (req_obj_type.has_value() && it->second.ObjType() != req_obj_type.value()) {
        return OpStatus::WRONG_TYPE;
    }


    it = ExpireIfNeeded(cntx, it);
    if (!IsValid(it)) {
        return OpStatus::KEY_NOTFOUND;
    }

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
    return DbSlice::ItAndUpdater{res.it, AutoUpdater(cntx.GetDbIndex(), key, res.it, this), true}; // 注意一下
}

facade::OpResult<DbSlice::ItAndUpdater> DbSlice::AddOrFindInternal(const Context& cntx, std::string_view key,
                                                           std::optional<unsigned> req_obj_type) {
                                                        
    DbTable& db = *db_arr_[cntx.GetDbIndex()];
    auto res = FindInternal(cntx, key, req_obj_type, UpdateStatsMode::kMutableStats);

    if (res.ok()) {
        Iterator it(*res, StringOrView::FromView(key));
        if (res->IsOccupied()) {
            return ItAndUpdater{it, AutoUpdater(cntx.GetDbIndex(), key, it, this), false};
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
        return OpStatus::WRONG_TYPE; 
    }

    (void)status;

    return ItAndUpdater{
        Iterator(it, StringOrView::FromView(key)),
        AutoUpdater(cntx.GetDbIndex(), key, Iterator(it, StringOrView::FromView(key)), this), true};
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
    if (!res.is_new ) // not same 
        return op_result;

    auto& it = res.it;
    it->second = std::move(obj);

    if (expire_at_ms) {
        AddExpire(cntx.GetDbIndex(), it, expire_at_ms);
    } else {
        RemoveExpire(cntx.GetDbIndex(), it);
    }

    return op_result;
}


void DbSlice::Del(Context cntx, Iterator it, DbTable* db_table) {

    DbTable* table = db_table ? db_table : db_arr_[cntx.GetDbIndex()].get();
    PerformDeletionAtomic(it, table); // 执行实际删除
}

void DbSlice::DelMutable(Context cntx, ItAndUpdater it_updater) {
    it_updater.post_updater.Run();
    Del(cntx, it_updater.it);
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

    if (!it->first.HasExpire()) {
        return it;
    }

    int64_t expire_time = it->first.GetExpireTime();

    if (int64_t(cntx.GetTimeNowMs() / 1000) < expire_time) {
        return it;
    }

    std::string scratch;
    std::string_view key = it->first.GetSlice(&scratch);

    auto& db = db_arr_[cntx.GetDbIndex()];
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
                ExpireIfNeeded(Context(nullptr, db_index, base::GetCurrentTimeMs()), prime_it);
            }
        };

        PrimeTable::Cursor cursor;
        do {
            cursor = db.prime_.Traverse(cursor, cb);
        } while (cursor);
    }
}


void DbSlice::RegisterWatchedKey(std::string_view key,
                                 ConnectionContext* conn_cntx) {
    db_arr_[conn_cntx->GetDbIndex()]->watched_keys_[std::string(key)].emplace_back(conn_cntx);
}

void DbSlice::PostUpdate(DbIndex db_ind, std::string_view key) {
    // auto& db = *db_arr_[db_ind];
    // auto& watched_keys = db.watched_keys_;
    // if (!watched_keys.empty()) {
    //     if (auto wit = watched_keys.find(std::string(key)); wit != watched_keys.end()) {
    //         for (auto& key_cntx : wit->second)
    //         {
    //             if (key_cntx.isExpired()) {
    //                 continue;
    //             }
    //             if (!key_cntx.conn_context->SetDirty(key_cntx.key_version)) {
    //                 // 这里开启了新的一轮事务，清除key_cntx的所有数据 TODO,
    //             }
    //         }
    //         watched_keys.erase(wit);
    //     }
    // }
}

void DbSlice::UnregisterWatchedKeys(ConnectionContext* conn_cntx, const std::vector<std::string_view>& keys) {
    auto& db = *db_arr_[conn_cntx->GetDbIndex()];
    for (const auto& key : keys) {
        if (auto wit = db.watched_keys_.find(std::string(key)); wit != db.watched_keys_.end()) { // 这里需要优化
            auto& vec = wit->second;
            for (auto& key_cntx : vec) {
                if (key_cntx.conn_context != conn_cntx) continue; // 这里是同步删除，一定是安全的
                std::erase(wit->second, key_cntx);
            }
            if (wit->second.empty()) {
                db.watched_keys_.erase(wit);
            }            
        }
    }
}

bool DbSlice::Acquire(IntentLock::Mode mode, const KeyLockArgs& lock_args) {
    if (lock_args.fps.empty()) {  
        return true;
    }

    auto& lt = db_arr_[lock_args.db_index]->trans_locks;
    bool all_locked = true;
    int i = 0;
    for (; i < lock_args.fps.size(); i++) {
        if (!lt.Acquire(lock_args.fps[i], mode)) {
            all_locked = false;
            break;
        }
    }
    if (!all_locked) {
        for (;i > 0; i--) {
            lt.Release(lock_args.fps[i - 1], mode);
        }        
    }
    return all_locked;
}

void DbSlice::Release(IntentLock::Mode mode, const KeyLockArgs& lock_args) {
    if (lock_args.fps.empty()) {
        return;
    }
    auto& lt = db_arr_[lock_args.db_index]->trans_locks;
    for (LockFp fp : lock_args.fps) {
       lt.Release(fp, mode);
    }
}

void DbSlice::AutoUpdater::Cancel() {
    fields_ = {}; 
}
DbSlice::AutoUpdater::AutoUpdater(AutoUpdater&& o) noexcept {
    *this = std::move(o);
}
DbSlice::AutoUpdater& DbSlice::AutoUpdater::operator=(AutoUpdater&& o) noexcept {
    Run();
    fields_ = o.fields_;
    o.Cancel();
    return *this;
}


DbSlice::AutoUpdater::~AutoUpdater() {
    Run();
}


void DbSlice::AutoUpdater::Run() {
    if (fields_.db_slice == nullptr) {
        return;
    }

    fields_.db_slice->PostUpdate(fields_.db_ind, fields_.key);
    Cancel();
}

DbSlice::AutoUpdater::AutoUpdater(DbIndex db_ind, std::string_view key, const Iterator& it,
                                  DbSlice* db_slice)
    : fields_{db_slice, db_ind, it, key} {
}


}  // namespace dfly




































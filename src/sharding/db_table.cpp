#include "db_table.hpp"
#include "detail/conn_context.hpp"
namespace dfly{


unsigned kInitSegmentLog = 3;


DbTable::DbTable(PMR_NS::memory_resource* mr, DbIndex db_index)
    : prime_(kInitSegmentLog, detail::PrimeTablePolicy{}, mr),
      index_(db_index) {

}
DbTable::WatchedKeyContext::WatchedKeyContext(ConnectionContext* o_conn_context)  :  
    conn_context(o_conn_context),
    sess(o_conn_context->owner()),
    key_version(o_conn_context->GetWatchedDirtyVer())
{

}

bool DbTable::WatchedKeyContext::operator==(const WatchedKeyContext& o) const noexcept {
    if (key_version != o.key_version) {
        return false;
    }
    if (auto ptr = sess.lock()) {
        return ptr.get() == o.sess.lock().get();
    }else {
        return false;
    }
}
bool DbTable::WatchedKeyContext::isExpired() const noexcept {
    return sess.lock() == nullptr;
}


DbTable::~DbTable() {

}    

}
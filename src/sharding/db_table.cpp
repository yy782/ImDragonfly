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
    key_version(o_conn_context->GetWatchedDirtyVer())
{

}


DbTable::~DbTable() {

}    

}
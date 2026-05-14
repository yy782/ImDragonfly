#include "db_table.hpp"

namespace dfly{


unsigned kInitSegmentLog = 3;


DbTable::DbTable(PMR_NS::memory_resource* mr, DbIndex db_index)
    : prime_(kInitSegmentLog, detail::PrimeTablePolicy{}, mr),
      index_(db_index) {

}

DbTable::~DbTable() {

}    

}
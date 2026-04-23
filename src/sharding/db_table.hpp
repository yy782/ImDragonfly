#pragma once
#include "DashTable/dash_table.hpp"
#include "DashTable/compact_obj.hpp"
#include "DashTable/table_policy.hpp"

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
namespace dfly{
using PrimeKey = detail::PrimeKey;
using PrimeValue = detail::PrimeValue;

using PrimeTable = DashTable<PrimeKey, PrimeValue, detail::PrimeTablePolicy>;
using PrimeIterator = PrimeTable::iterator;
using PrimeConstIterator = PrimeTable::const_iterator;
inline bool IsValid(PrimeIterator it) {
    return !it.is_done();
}

inline bool IsValid(PrimeConstIterator it) {
    return !it.is_done();
}
using DbIndex = uint16_t;
//uint32_t thread_index;


struct DbTable : 
    boost::intrusive_ref_counter<DbTable, boost::thread_unsafe_counter> 
{

PrimeTable prime_;
DbIndex index_;
// uint32_t thread_index_;
  explicit DbTable(PMR_NS::memory_resource* mr, DbIndex index); // explicit多余???
  ~DbTable();

void Clear();
size_t table_memory();
};
using DbTableArray = std::vector<boost::intrusive_ptr<DbTable>>;
}  // namespace dfly
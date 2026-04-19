#pragma once
#include "DashTable/dash_table.hpp"
#include "DashTable/CompactObj.hpp"

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
namespace dfly{
using PrimeKey = detail::PrimeKey;
using PrimeValue = detail::PrimeValue;

using PrimeTable = DashTable<PrimeKey, PrimeValue>;
using PrimeIterator = PrimeTable::iterator;
using PrimeConstIterator = PrimeTable::const_iterator;
inline bool IsValid(PrimeIterator it) {
  return !it.is_done();
}

inline bool IsValid(PrimeConstIterator it) {
  return !it.is_done();
}
using DbIndex = uint16_t;
uint32_t thread_index;


struct DbTable : 
    boost::intrusive_ref_counter<DbTable, boost::thread_unsafe_counter> 
{

PrimeTable prime_;
void Clear();
size_t table_memory();
};
using DbTableArray = std::vector<boost::intrusive_ptr<DbTable>>;
}  // namespace dfly
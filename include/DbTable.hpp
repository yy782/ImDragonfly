#include "DashTable/dash_table.hpp"
#include "DashTable/CompactObj.hpp"

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

using PrimeKey = detail::PrimeKey;
using PrimeValue = detail::PrimeValue;

using PrimeTable = DashTable<PrimeKey, PrimeValue>;
using PrimeIterator = PrimeTable::iterator;
using PrimeConstIterator = PrimeTable::const_iterator;

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
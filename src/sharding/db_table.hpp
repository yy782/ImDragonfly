// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once
#include "DashTable/dash_table.hpp"
#include "DashTable/compact_obj.hpp"
#include "DashTable/table_policy.hpp"

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <unordered_map>
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
class ConnectionContext;

struct DbTable : 
    boost::intrusive_ref_counter<DbTable, boost::thread_unsafe_counter> 
{
    explicit DbTable(PMR_NS::memory_resource* mr, DbIndex index); // explicit多余???
    ~DbTable();

    PrimeTable& prime() { return prime_; }
    const PrimeTable& prime() const { return prime_; }

    DbIndex index() const { return index_; } 

    class WatchedKeyContext {
    public:

        WatchedKeyContext(ConnectionContext* o_conn_context);
        bool operator==(const WatchedKeyContext& o) noexcept {
            return conn_context == o.conn_context && key_version == o.key_version;
        }
    private:
        friend class DbSlice;
        ConnectionContext* conn_context;
        uint64_t key_version;        
    };
    PrimeTable prime_;
    DbIndex index_;
    std::unordered_map<std::string_view, std::vector<WatchedKeyContext>> watched_keys_;
};
using DbTableArray = std::vector<boost::intrusive_ptr<DbTable>>;
}  // namespace dfly
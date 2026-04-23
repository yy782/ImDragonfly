#pragma once 
#include "engine_shard_set.hpp"
#include "common_types.hpp"
namespace dfly{
inline SlotId KeySlot(std::string_view key){
    size_t hash = 0x811c9dc5;
    for (char c : key) {
        hash ^= c;
        hash *= 0x01000193;
    }
    return hash % shard_set->size();
}    
}
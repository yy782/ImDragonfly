#pragma once // common.hpp
#include <string_view>
#include <memory>
#include <functional>
#include "common_types.hpp"
namespace dfly {
class Namespaces;

inline Namespaces* namespaces = nullptr;

class RedisSession;
using RedisSessionPtr = std::shared_ptr<RedisSession>;
using RedisSessionWeakPtr = std::weak_ptr<RedisSession>;
inline ShardId Shard(std::string_view key, ssize_t shard_set_size){
    size_t hash = std::hash<std::string_view>{}(key);
    return hash % shard_set_size;   
}


}
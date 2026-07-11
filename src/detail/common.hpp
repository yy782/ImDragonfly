#pragma once // common.hpp
#include <string_view>
#include <memory>
#include <functional>
#include <immintrin.h>
#include "common_types.hpp"
namespace dfly {
class Namespaces;

inline Namespaces* namespaces = nullptr;

class RedisSession;
using RedisSessionPtr = std::shared_ptr<RedisSession>;
using RedisSessionWeakPtr = std::weak_ptr<RedisSession>;
inline ShardId Shard(std::string_view key, ssize_t shard_set_size) {

    const char* data = key.data();
    size_t len = key.size();
    size_t hash = 0x9e3779b97f4a7c15ULL;
    
    // 每次处理 32 字节
    size_t i = 0;
    if (len >= 32) {
        __m256i vec = _mm256_setzero_si256();
        for (; i + 32 <= len; i += 32) {
            __m256i chunk = _mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(data + i)
            );
            vec = _mm256_xor_si256(vec, chunk);
        }
        // 混合结果
        alignas(32) uint64_t buffer[4];
        _mm256_store_si256(reinterpret_cast<__m256i*>(buffer), vec);
        hash ^= buffer[0] ^ buffer[1] ^ buffer[2] ^ buffer[3];
    }
    
    // 处理剩余字节
    for (; i < len; ++i) {
        hash ^= data[i] + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
    }

    return hash & (shard_set_size - 1);   
}


}
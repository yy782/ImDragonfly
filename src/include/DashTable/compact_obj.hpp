// simplified_compact_obj.h
#pragma once

#include <cstring>
#include <optional>
#include <string>
#include <string_view>

namespace dfly{

namespace detail
{
class RobjWrapper;
}








using CompactObjType = unsigned;

constexpr CompactObjType kInvalidCompactObjType = std::numeric_limits<CompactObjType>::max();


class CompactObj{
    static constexpr unsigned kInlineLen = 16;

    void operator=(const CompactObj&) = delete;
    CompactObj(const CompactObj&) = delete;
protected:
    enum TagEnum : uint8_t {
        INT_TAG = 17,
        SMALL_TAG = 18, // 小字符串
        ROBJ_TAG = 19, // Redis 对象（list/hash/set） 
    };    
    enum EncodingEnum : uint8_t;
public:
    struct StrEncoding;
    using MemoryResource = PMR_NS::memory_resource;  
    
    explicit CompactObj(bool is_key)
        : is_key_{is_key}, taglen_{0} {  // default - empty string
    }

    CompactObj(std::string_view str, bool is_key) : CompactObj(is_key) {
        SetString(str);
    }

    CompactObj(CompactObj&& cs) noexcept : CompactObj(cs.is_key_) {
        operator=(std::move(cs));
    };    

    ~CompactObj();

    CompactObj& operator=(CompactObj&& o) noexcept; 

    uint64_t HashCode() const;
    static uint64_t HashCode(std::string_view str);

    void Reset();


    void SetString(std::string_view str);

    CompactObjType ObjType() const;

protected:
    void SetMeta(uint8_t taglen);

    union U {
        std::string str_;
        U():str_(){}
        ~U(){}
    }u_;

    const bool is_key_ : 1; 
    uint8_t taglen_ : 5;       
};

struct CompactKey : public CompactObj {
    CompactKey() : CompactObj(true) {
    }

    explicit CompactKey(std::string_view str) : CompactObj{str, true} {
    }

    bool HasExpire() const ;


    void SetExpireTime(uint64_t abs_ms);


    bool ClearExpireTime();

    uint64_t GetExpireTime() const;

    CompactKey& operator=(std::string_view sv) noexcept {
        SetString(sv);
        return *this;
    }

    bool operator==(std::string_view sl) const;

    bool operator!=(std::string_view sl) const {
        return !(*this == sl);
    }

    friend bool operator==(std::string_view sl, const CompactKey& o) {
        return o.operator==(sl);
    }
};

struct CompactValue : public CompactObj {
    CompactValue() : CompactObj(false) {
    }

    explicit CompactValue(std::string_view str) : CompactObj{str, false} {
    }
};


#include "redis/redis_aux.hpp"

void CompactObj::SetString(std::string_view str) {
    u_.str_=str;
}

CompactObj& CompactObj::operator=(CompactObj&& o) noexcept {
    SetMeta(o.taglen_);

    u_.str_=o.u_.str_;

    return *this;
}

void CompactObj::SetMeta(uint8_t taglen) {
    taglen_ = taglen;
}

uint64_t CompactObj::HashCode() const {
    return std::hash<std::string>{}(u_.str_); // !!!!!!!!!!!!!!!!!!!
}

uint64_t CompactObj::HashCode(std::string_view str) {
  return std::hash<std::string_view>{}(str);
}

void CompactObj::Reset() {
    taglen_ = 0;
}


CompactObjType CompactObj::ObjType() const {
    return OBJ_STRING;
}



}
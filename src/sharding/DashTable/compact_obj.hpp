
#pragma once

#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

namespace dfly{

namespace detail
{
class RobjWrapper;
}

// namespace PMR_NS = base::pmr;

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
    // using MemoryResource = PMR_NS::memory_resource;  
    
    explicit CompactObj(bool is_key)
        : is_key_{is_key}, taglen_{0} {  // default - empty string
    }

    CompactObj(std::string_view str, bool is_key) : CompactObj(is_key) {
        SetString(str);
    }

    CompactObj(CompactObj&& cs) noexcept : CompactObj(cs.is_key_) {
        operator=(std::move(cs));
    };    

    ~CompactObj()=default;

    CompactObj& operator=(CompactObj&& o) noexcept; 

    uint64_t HashCode() const;
    static uint64_t HashCode(std::string_view str);

    void Reset();


    void SetString(std::string_view str);

    std::string ToString() const{
        return u_.str_;
    }
    CompactObjType ObjType() const;

protected:
    void SetMeta(uint8_t taglen);

    union U {
        std::string str_;
        U():str_(){}
        ~U(){} // 需要显式析构函数
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




}
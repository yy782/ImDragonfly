
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



using CompactObjType = unsigned;

constexpr CompactObjType kInvalidCompactObjType = std::numeric_limits<CompactObjType>::max();


class CompactObj{
    static constexpr unsigned kInlineLen = 16;

    void operator=(const CompactObj&) = delete;
    CompactObj(const CompactObj&) = delete;
protected:
    enum TagEnum : uint8_t {
        INT_TAG = 17,
        STR_TAG = 18, // 字符串
        ROBJ_TAG = 19, // Redis 对象（list/hash/set） 
        SDS_TTL_TAG = 24, 
    };
public:
    
    explicit CompactObj(bool is_key)
        : is_key_{is_key}, taglen_{0} {  
    }
    CompactObj(std::string_view str, bool is_key) : CompactObj(is_key) {
        SetString(str);
    }

    CompactObj(CompactObj&& cs) noexcept : CompactObj(cs.is_key_) {
        operator=(std::move(cs));
    };    

    ~CompactObj()=default;

    CompactObj& operator=(CompactObj&& o) noexcept; 
    bool operator==(const CompactObj& o) noexcept;
    uint64_t HashCode() const;
    static uint64_t HashCode(std::string_view str);

    void Reset();


    void SetString(std::string_view str);

    std::string ToString() const{
        return u_.str_;
    }
    CompactObjType ObjType() const;

    std::string_view GetSlice(std::string* scratch) const {

        if (taglen_ == STR_TAG) {
            *scratch = std::string_view(*scratch);
            return *scratch;
        }

        if (taglen_ == SDS_TTL_TAG) {
            return u_.str_ttl_.view();
        }

        return std::string_view{};
    }


protected:
    void SetMeta(uint8_t taglen);

    struct TtlString {
        std::string str_;   
        uint64_t exp_ms_;  

        std::string_view view() const {
            return std::string_view(str_);
        }
    };

    union U {
        std::string str_;

        TtlString str_ttl_;

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

    bool HasExpire() const{
    return taglen_ == SDS_TTL_TAG;
  }

    void SetExpireTime(uint64_t abs_ms);


    bool ClearExpireTime();

    uint64_t GetExpireTime() const;

    CompactKey& operator=(std::string_view sv) noexcept {
        SetString(sv);
        return *this;
    }

    bool operator==(std::string_view sl) const{
        return u_.str_==std::string(sl);
    }

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
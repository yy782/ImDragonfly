
#pragma once

#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <glog/logging.h>
namespace dfly{





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
        TTL_STR_TAG = 24,  // 过期字符串
    };
public:
    struct TtlString {
        std::string str_;   
        uint64_t exp_ms_;  

        std::string_view view() const {
            return std::string_view(str_);
        }

        bool operator==(const TtlString& o) const{
            return str_ == o.str_ && exp_ms_ == o.exp_ms_;
        }
    }; 

    explicit CompactObj(bool is_key)
        : is_key_{is_key}, tag_{0} {  
    }
    CompactObj(std::string_view str, bool is_key) : CompactObj(is_key) {
        SetString(str);
    }
    CompactObj(int64_t val) : CompactObj(false) {
        SetInt(val);
    } 
    CompactObj(const TtlString& s) :CompactObj(true) {
        SetTtlStr(s);
    }
    CompactObj(CompactObj&& cs) noexcept : CompactObj(cs.is_key_) {
        operator=(std::move(cs));
    };    
    ~CompactObj() {
        Reset();
    }
    CompactObj& operator=(CompactObj&& o) noexcept {
        switch (o.tag_)
        {
        case 0:
            break;
        case INT_TAG:
            SetInt(o.u_.ival_);
            break;
        case STR_TAG:
            SetString(o.u_.str_);
            break;
        case TTL_STR_TAG:
            SetTtlStr(o.u_.ttl_str_);
            break;
        default:
            LOG(FATAL) << "Invalid tag: " << o.tag_; // 插入默认值会没有标签
            break;
        }

        return *this;
    }
    bool operator==(const CompactObj& o) noexcept{
        if (tag_ != o.tag_) return false;
        switch (tag_)
        {
        case 0:
            return true; // 有这种情况吗，调试可以记录一下
        case INT_TAG:
            return o.u_.ival_ == u_.ival_;
        case STR_TAG:
            return o.u_.str_ == u_.str_;
        case TTL_STR_TAG:
            return o.u_.ttl_str_ == u_.ttl_str_;
        default:
            LOG(FATAL) << "Invalid tag: " << o.tag_;
            return false;
        }        
    }

    uint64_t HashCode() const;
    static uint64_t HashCode(std::string_view str);

    void Reset() {
        switch (tag_)
        {
            using std::string;
            case 0:
                break;
            case INT_TAG:
                break;
            case STR_TAG:
                u_.str_.~string(); // 
                break;
            case TTL_STR_TAG:
                u_.ttl_str_.~TtlString();
                break;
            default:
                LOG(FATAL) << "Invalid tag: " << tag_;
                break;
        }
        tag_ = 0;
    }


    void SetString(std::string_view str) {
        u_.str_ = str;
        SetMeta(STR_TAG);
    }
    void SetInt(int64_t val) {
        u_.ival_ = val;
        SetMeta(INT_TAG);
    }
    void SetTtlStr(const TtlString& s) {
        u_.ttl_str_ = std::move(s);
    }


    std::string ToString() const {
        switch (tag_)
        {
        case INT_TAG:
            return std::to_string(u_.ival_);
        case STR_TAG:
            return u_.str_;
        case TTL_STR_TAG:
            return u_.ttl_str_.str_;
        default:
            LOG(FATAL) << "Invalid tag: " << tag_;
            return {};
        }         
    }
    CompactObjType ObjType() const;

    std::string_view GetSlice(std::string* scratch) const { 
        switch (tag_)
        {
        case INT_TAG:
            *scratch = std::to_string(u_.ival_);
            return *scratch;
        case STR_TAG:
            return u_.str_;
        case TTL_STR_TAG:
            return u_.ttl_str_.str_;
        default:
            LOG(FATAL) << "Invalid tag: " << tag_;
            return {};
        } 

    }


protected:
    void SetMeta(uint8_t tag) {
        tag_ = tag;
    }



    union U {
        std::string str_; // string不是POD类型，不能强制对齐， 如果使用ssd字符串会好些

        TtlString ttl_str_;

        int64_t ival_;

        U():str_(){}
        ~U(){} // 需要显式析构函数
    }u_;

    const bool is_key_ : 1; 
    uint8_t tag_ : 5;   
    
    

};

struct CompactKey : public CompactObj {
    CompactKey() : CompactObj(true) {
    }

    explicit CompactKey(std::string_view str) : CompactObj{str, true} {
    }

    bool HasExpire() const{
    return tag_ == TTL_STR_TAG;
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
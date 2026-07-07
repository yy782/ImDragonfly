
#pragma once

#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <glog/logging.h>
#include <mimalloc.h>
#include "redis/redis_aux.hpp"
namespace dfly{




using CompactObjType = unsigned;

constexpr CompactObjType kInvalidCompactObjType = std::numeric_limits<CompactObjType>::max();


class RobjWrapper{
public:
    RobjWrapper() = default;
    RobjWrapper(CompactObjType type, void* obj_inner)
        : type_(type), obj_inner_(obj_inner) {}
    ~RobjWrapper();
    RobjWrapper(const RobjWrapper&) = delete;
    RobjWrapper& operator=(const RobjWrapper&) = delete;
    RobjWrapper(RobjWrapper&& o) noexcept {
        type_ = o.type_;
        obj_inner_ = o.obj_inner_;
        o.reset();
    }
    RobjWrapper& operator=(RobjWrapper&& o) noexcept {
        if (this != &o) {
            this->~RobjWrapper();
            type_ = o.type_;
            obj_inner_ = o.obj_inner_;
            o.reset();
        }
        return *this;
    }
    CompactObjType type() const { return type_; }
    void* obj_inner() const { return obj_inner_; }
    bool operator==(const RobjWrapper& o) const;
    void reset() {
        type_ = kInvalidCompactObjType;
        obj_inner_ = nullptr;
    }
private:
    CompactObjType type_;
    void* obj_inner_;
};

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

    CompactObj(CompactObjType type, void* obj_inner) :CompactObj(true) {
        SetRobj(type, obj_inner);
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
            SetString(std::move(o.u_.str_));
            break;
        case TTL_STR_TAG:
            SetTtlStr(std::move(o.u_.ttl_str_));
            break;
        case ROBJ_TAG:
            u_.r_obj_ = std::move(o.u_.r_obj_);
            SetMeta(ROBJ_TAG);
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
        case ROBJ_TAG:
            return o.u_.r_obj_ == u_.r_obj_;
        default:
            LOG(FATAL) << "Invalid tag: " << o.tag_;
            return false;
        }        
    }

    uint64_t HashCode() const;
    static uint64_t HashCode(std::string_view str);
    void SetString(std::string_view str) {
            u_.str_ = str;
            SetMeta(STR_TAG);
        
    }
    void SetInt(int64_t val) {
        u_.ival_ = val;
        SetMeta(INT_TAG);
    }
    void SetTtlStr(const TtlString& s) {
        u_.ttl_str_ = s;
        SetMeta(TTL_STR_TAG);
    }
    void SetRobj(CompactObjType type, void* obj_inner) {
        u_.r_obj_ = {type, obj_inner};
        SetMeta(ROBJ_TAG);
    }
    
    RobjWrapper& GetRobjWrapper() {
        return u_.r_obj_;
    }
    
    const RobjWrapper& GetRobjWrapper() const {
        return u_.r_obj_;
    }
    
    uint8_t GetTag() const {
        return tag_;
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
        case 0:
            return {};
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
        case 0:
            return {};
        default:
            LOG(FATAL) << "Invalid tag: " << tag_;
            return {};
        } 

    }


protected:
    void SetMeta(uint8_t tag) {
        tag_ = tag;
    }

    void Reset() {
        switch (tag_)
        {
            using std::string;
            case 0:
                break;
            case INT_TAG:
                break;
            case STR_TAG:
                u_.str_ = ""; 
                break;
            case TTL_STR_TAG:
                u_.ttl_str_.~TtlString();
                break;
            case ROBJ_TAG:
                u_.r_obj_.~RobjWrapper();
                break;
            default:
                LOG(FATAL) << "Invalid tag: " << tag_;
                break;
        }
        tag_ = 0;
    }

    union U {
        std::string str_; // string不是POD类型，不能强制对齐， 如果使用ssd字符串会好些

        TtlString ttl_str_;

        int64_t ival_;
        RobjWrapper r_obj_;

        U():str_(){}
        ~U(){
        } // 需要显式析构函数
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

    static CompactValue MakeList() {
        CompactValue obj;
        obj.SetRobj(OBJ_LIST, new (mi_malloc(sizeof(ListObject))) ListObject{});
        return obj;
    }

    ListObject* GetList() {
        if (tag_ != ROBJ_TAG || ObjType() != OBJ_LIST) {
            return nullptr;
        }
        return static_cast<ListObject*>(GetRobjWrapper().obj_inner());
    }

    const ListObject* GetList() const {
        if (tag_ != ROBJ_TAG || ObjType() != OBJ_LIST) {
            return nullptr;
        }
        return static_cast<const ListObject*>(GetRobjWrapper().obj_inner());
    }

    static CompactValue MakeHash() {
        CompactValue obj;
        obj.SetRobj(OBJ_HASH, new (mi_malloc(sizeof(HashObject))) HashObject{});
        return obj;
    }

    HashObject* GetHash() {
        if (tag_ != ROBJ_TAG || ObjType() != OBJ_HASH) {
            return nullptr;
        }
        return static_cast<HashObject*>(GetRobjWrapper().obj_inner());
    }

    const HashObject* GetHash() const {
        if (tag_ != ROBJ_TAG || ObjType() != OBJ_HASH) {
            return nullptr;
        }
        return static_cast<const HashObject*>(GetRobjWrapper().obj_inner());
    }

    static CompactValue MakeSet() {
        CompactValue obj;
        obj.SetRobj(OBJ_SET, new (mi_malloc(sizeof(SetObject))) SetObject{});
        return obj;
    }

    SetObject* GetSet() {
        if (tag_ != ROBJ_TAG || ObjType() != OBJ_SET) {
            return nullptr;
        }
        return static_cast<SetObject*>(GetRobjWrapper().obj_inner());
    }

    const SetObject* GetSet() const {
        if (tag_ != ROBJ_TAG || ObjType() != OBJ_SET) {
            return nullptr;
        }
        return static_cast<const SetObject*>(GetRobjWrapper().obj_inner());
    }

    static CompactValue MakeZSet() {
        CompactValue obj;
        obj.SetRobj(OBJ_ZSET, new (mi_malloc(sizeof(ZSetObject))) ZSetObject{});
        return obj;
    }

    ZSetObject* GetZSet() {
        if (tag_ != ROBJ_TAG || ObjType() != OBJ_ZSET) {
            return nullptr;
        }
        return static_cast<ZSetObject*>(GetRobjWrapper().obj_inner());
    }

    const ZSetObject* GetZSet() const {
        if (tag_ != ROBJ_TAG || ObjType() != OBJ_ZSET) {
            return nullptr;
        }
        return static_cast<const ZSetObject*>(GetRobjWrapper().obj_inner());
    }
};




}
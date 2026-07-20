#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <glog/logging.h>
#include <mimalloc.h>
#include "redis/redis_aux.hpp"

namespace dfly {

using CompactObjType = unsigned;
constexpr CompactObjType kInvalidCompactObjType = std::numeric_limits<CompactObjType>::max();

// ──── Tagged Union core ──────────────────────────────────────────────────────
// Union U holds all possible value representations.
// Lifetime is managed explicitly by CompactObj via placement new / destroy.
struct TtlString {
    std::string val;
    uint64_t exp_ms;

    std::string_view view() const { return std::string_view(val); }
    bool operator==(const TtlString& o) const {
        return val == o.val && exp_ms == o.exp_ms;
    }
};
struct Robj {
    void* ptr;
    CompactObjType type;
};

union CompactU {
    int64_t ival_;
    std::string str_;
    TtlString ttl_;
    Robj robj_;
    CompactU() : ival_{} {}
    ~CompactU() {}  // non-trivial members require manual destruction
};

// ──── CompactObj ─────────────────────────────────────────────────────────────

class CompactObj {
    void operator=(const CompactObj&) = delete;
    CompactObj(const CompactObj&) = delete;
public:
    enum Tag : uint8_t {
        EMPTY       = 0,
        INT_TAG     = 1,
        STR_TAG     = 2,
        ROBJ_TAG    = 3,
        TTL_STR_TAG = 4,
    };
    explicit CompactObj(bool is_key)
        : is_key_{is_key}, tag_{EMPTY} {}
    CompactObj(std::string_view str, bool is_key) : CompactObj(is_key) {
        SetString(str);
    }
    explicit CompactObj(int64_t val) : CompactObj(false) {
        SetInt(val);
    }
    explicit CompactObj(const TtlString& ts) : CompactObj(true) {
        SetTtlStr(ts);
    }
    CompactObj(CompactObjType type, void* obj_inner) : CompactObj(true) {
        SetRobj(type, obj_inner);
    }
    CompactObj(CompactObj&& o) noexcept : CompactObj(o.is_key_) {
        MoveFrom(std::move(o));
    }
    ~CompactObj() { Destroy(); }
    CompactObj& operator=(CompactObj&& o) noexcept {
        if (this != &o) {
            Destroy();
            MoveFrom(std::move(o));
        }
        return *this;
    }
    bool operator==(const CompactObj& o) const noexcept {
        if (tag_ != o.tag_) return false;
        switch (tag_) {
        case EMPTY:       return true;
        case INT_TAG:     return u_.ival_ == o.u_.ival_;
        case STR_TAG:     return u_.str_ == o.u_.str_;
        case TTL_STR_TAG: return u_.ttl_ == o.u_.ttl_;
        case ROBJ_TAG:    return RobjCmp(o);
        default:
            LOG(FATAL) << "Invalid tag: " << int(tag_);
            return false;
        }
    }
    uint64_t HashCode() const;
    static uint64_t HashCode(std::string_view str);
    CompactObjType ObjType() const {
        switch (tag_) {
        case EMPTY:       return kInvalidCompactObjType;
        case INT_TAG:
        case STR_TAG:
        case TTL_STR_TAG: return OBJ_STRING;
        case ROBJ_TAG:    return u_.robj_.type;
        }
        LOG(FATAL) << "TBD " << int(tag_);
        return kInvalidCompactObjType;
    }
    void SetString(std::string&& str) {
        Destroy();
        new (&u_.str_) std::string(std::move(str));
        tag_ = STR_TAG;
    }
    void SetString(std::string_view str) {
        Destroy();
        new (&u_.str_) std::string(str);
        tag_ = STR_TAG;
    }
    void SetInt(int64_t val) {
        Destroy();
        u_.ival_ = val;
        tag_ = INT_TAG;
    }
    void SetTtlStr(const TtlString& ts) {
        Destroy();
        new (&u_.ttl_) TtlString(ts);
        tag_ = TTL_STR_TAG;
    }
    void SetRobj(CompactObjType type, void* obj_inner) {
        Destroy();
        u_.robj_.type = type;
        u_.robj_.ptr  = obj_inner;
        tag_ = ROBJ_TAG;
    }
    Tag  GetTag()      const { return tag_; }
    bool IsEmpty()     const { return tag_ == EMPTY; }
    bool IsInt()       const { return tag_ == INT_TAG; }
    bool IsStr()       const { return tag_ == STR_TAG; }
    bool IsRobj()      const { return tag_ == ROBJ_TAG; }
    bool IsTtlStr()    const { return tag_ == TTL_STR_TAG; }
    int64_t       AsInt()  const { DCHECK(IsInt());  return u_.ival_; }
    const std::string& AsStr()  const { DCHECK(IsStr());  return u_.str_; }
    const TtlString&   AsTtl()  const { DCHECK(IsTtlStr()); return u_.ttl_; }
    void*        RobjPtr()  const { DCHECK(IsRobj()); return u_.robj_.ptr; }
    CompactObjType RobjType() const { DCHECK(IsRobj()); return u_.robj_.type; }
    std::string ToString() const {
        switch (tag_) {
        case INT_TAG:     return std::to_string(u_.ival_);
        case STR_TAG:     return u_.str_;
        case TTL_STR_TAG: return u_.ttl_.val;
        case EMPTY:       return {};
        default:
            LOG(FATAL) << "Invalid tag: " << int(tag_);
            return {};
        }
    }
    std::string_view GetSlice(std::string* scratch) const {
        switch (tag_) {
        case INT_TAG:
            *scratch = std::to_string(u_.ival_);
            return *scratch;
        case STR_TAG:     return u_.str_;
        case TTL_STR_TAG: return u_.ttl_.val;
        case EMPTY:       return {};
        default:
            LOG(FATAL) << "Invalid tag: " << int(tag_);
            return {};
        }
    }
protected:
    void Destroy() {
        switch (tag_) {
        case INT_TAG:
        case EMPTY:
            break;                          
        case STR_TAG:
            u_.str_.~basic_string();
            break;
        case TTL_STR_TAG:
            u_.ttl_.~TtlString();
            break;
        case ROBJ_TAG:
            RobjFree(u_.robj_.ptr, u_.robj_.type);
            break;
        default:
            LOG(FATAL) << "Invalid tag: " << int(tag_);
        }
        tag_ = EMPTY;
    }
    void MoveFrom(CompactObj&& o) {
        switch (o.tag_) {
        case EMPTY:
            break;
        case INT_TAG:
            u_.ival_ = o.u_.ival_;
            break;
        case STR_TAG:
            new (&u_.str_) std::string(std::move(o.u_.str_));
            o.u_.str_.~basic_string();   
            break;
        case TTL_STR_TAG:
            new (&u_.ttl_) TtlString(std::move(o.u_.ttl_));
            o.u_.ttl_.~TtlString();      
            break;
        case ROBJ_TAG:
            u_.robj_ = o.u_.robj_;
            o.u_.robj_.ptr = nullptr;     
            break;
        }
        tag_ = o.tag_;
        o.tag_ = EMPTY;
    }
    static void RobjFree(void* ptr, CompactObjType type) {
        if (!ptr) return;
        switch (type) {
        case OBJ_LIST:
            static_cast<ListObject*>(ptr)->~ListObject();
            break;
        case OBJ_HASH:
            static_cast<HashObject*>(ptr)->~HashObject();
            break;
        case OBJ_SET:
            static_cast<SetObject*>(ptr)->~SetObject();
            break;
        case OBJ_ZSET:
            static_cast<ZSetObject*>(ptr)->~ZSetObject();
            break;
        default:
            LOG(WARNING) << "Unknown robj type: " << type;
            break;
        }
        mi_free(ptr);
    }
    bool RobjCmp(const CompactObj& o) const {
        const auto& a = u_.robj_;
        const auto& b = o.u_.robj_;
        if (a.type != b.type) return false;
        if (!a.ptr || !b.ptr) return a.ptr == b.ptr;

        switch (a.type) {
        case OBJ_LIST:
            return static_cast<const ListObject*>(a.ptr)->Data()
                == static_cast<const ListObject*>(b.ptr)->Data();
        case OBJ_HASH:
            return static_cast<const HashObject*>(a.ptr)->Data()
                == static_cast<const HashObject*>(b.ptr)->Data();
        case OBJ_SET:
            return static_cast<const SetObject*>(a.ptr)->Data()
                == static_cast<const SetObject*>(b.ptr)->Data();
        case OBJ_ZSET:
            return static_cast<const ZSetObject*>(a.ptr)->Range(0, -1)
                == static_cast<const ZSetObject*>(b.ptr)->Range(0, -1);
        default:
            LOG(FATAL) << "Invalid robj type: " << a.type;
            return false;
        }
    }
    CompactU u_;
    const bool is_key_ : 1;
    Tag tag_ : 5;
};
struct CompactKey : public CompactObj {
    CompactKey() : CompactObj(true) {}
    explicit CompactKey(std::string_view str) : CompactObj(str, true) {}
    bool HasExpire() const {
        return tag_ == TTL_STR_TAG;
    }
    void SetExpireTime(uint64_t abs_ms) {
        if (tag_ == TTL_STR_TAG) {
            u_.ttl_.exp_ms = abs_ms;
            return;
        }
        std::string cur = std::move(u_.str_);
        Destroy();
        new (&u_.ttl_) TtlString{std::move(cur), abs_ms};
        tag_ = TTL_STR_TAG;
    }

    bool ClearExpireTime() {
        if (tag_ != TTL_STR_TAG) return false;
        std::string s = std::move(u_.ttl_.val);
        Destroy();
        new (&u_.str_) std::string(std::move(s));
        tag_ = STR_TAG;
        return true;
    }

    uint64_t GetExpireTime() const {
        if (tag_ != TTL_STR_TAG) return 0;
        return u_.ttl_.exp_ms;
    }
    CompactKey& operator=(std::string_view sv) noexcept {
        SetString(sv);
        return *this;
    }
    bool operator==(std::string_view sl) const {
        if (tag_ == STR_TAG)      return u_.str_ == sl;
        if (tag_ == TTL_STR_TAG)  return u_.ttl_.val == sl;
        return false;
    }

    bool operator!=(std::string_view sl) const {
        return !(*this == sl);
    }

    friend bool operator==(std::string_view sl, const CompactKey& o) {
        return o == sl;
    }
};
struct CompactValue : public CompactObj {
    CompactValue() : CompactObj(false) {}

    explicit CompactValue(std::string_view str) : CompactObj(str, false) {}
    template <typename ObjType, CompactObjType ObjTag>
    static CompactValue Make() {
        CompactValue v;
        void* p = mi_malloc(sizeof(ObjType));
        new (p) ObjType();
        v.SetRobj(ObjTag, p);
        return v;
    }

    static CompactValue MakeList() { return Make<ListObject, OBJ_LIST>(); }
    static CompactValue MakeHash() { return Make<HashObject, OBJ_HASH>(); }
    static CompactValue MakeSet()  { return Make<SetObject,  OBJ_SET>(); }
    static CompactValue MakeZSet() { return Make<ZSetObject, OBJ_ZSET>(); }
    template <typename ObjType, CompactObjType ObjTag>
    ObjType* GetObj() {
        if (tag_ != ROBJ_TAG || u_.robj_.type != ObjTag) return nullptr;
        return static_cast<ObjType*>(u_.robj_.ptr);
    }

    template <typename ObjType, CompactObjType ObjTag>
    const ObjType* GetObj() const {
        if (tag_ != ROBJ_TAG || u_.robj_.type != ObjTag) return nullptr;
        return static_cast<const ObjType*>(u_.robj_.ptr);
    }

    ListObject* GetList() { return GetObj<ListObject, OBJ_LIST>(); }
    const ListObject* GetList() const { return GetObj<ListObject, OBJ_LIST>(); }

    HashObject* GetHash() { return GetObj<HashObject, OBJ_HASH>(); }
    const HashObject* GetHash() const { return GetObj<HashObject, OBJ_HASH>(); }

    SetObject* GetSet() { return GetObj<SetObject, OBJ_SET>(); }
    const SetObject* GetSet() const { return GetObj<SetObject, OBJ_SET>(); }

    ZSetObject* GetZSet() { return GetObj<ZSetObject, OBJ_ZSET>(); }
    const ZSetObject* GetZSet() const { return GetObj<ZSetObject, OBJ_ZSET>(); }
};

}  

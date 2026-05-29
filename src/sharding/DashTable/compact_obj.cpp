

#include "compact_obj.hpp"
#include "redis/redis_aux.hpp"

namespace dfly{

RobjWrapper::~RobjWrapper() {
    if (type_ == kInvalidCompactObjType || !obj_inner_)
        return;
    switch (type_){
        case OBJ_LIST:
            delete static_cast<ListObject*>(obj_inner_);
            break;
        case OBJ_HASH:
            delete static_cast<HashObject*>(obj_inner_);
            break;
        case OBJ_SET:
            delete static_cast<SetObject*>(obj_inner_);
            break;
        case OBJ_ZSET:
            delete static_cast<ZSetObject*>(obj_inner_);
            break;
        default:
            LOG(WARNING) << "Unknown type in RobjWrapper destructor: " << type_;
            break;
    }
    
}

bool RobjWrapper::operator==(const RobjWrapper& o) const {
    if (type_ != o.type_) return false;
    if (!obj_inner_ || !o.obj_inner_) {
        return obj_inner_ == o.obj_inner_;
    }
    
    switch (type_) {
        case OBJ_LIST: {
            const ListObject* lhs = static_cast<const ListObject*>(obj_inner_);
            const ListObject* rhs = static_cast<const ListObject*>(o.obj_inner_);
            return lhs->Data() == rhs->Data();
        }
        case OBJ_HASH: {
            const HashObject* lhs = static_cast<const HashObject*>(obj_inner_);
            const HashObject* rhs = static_cast<const HashObject*>(o.obj_inner_);
            return lhs->Data() == rhs->Data();
        }
        case OBJ_SET: {
            const SetObject* lhs = static_cast<const SetObject*>(obj_inner_);
            const SetObject* rhs = static_cast<const SetObject*>(o.obj_inner_);
            return lhs->Data() == rhs->Data();
        }
        default:
            LOG(FATAL) << "Invalid type: " << type_;
            return false;
    }
}


uint64_t CompactObj::HashCode() const {
        switch (tag_)
        {
        case INT_TAG:
            return std::hash<int64_t>{}(u_.ival_);
        case STR_TAG:
            return std::hash<std::string>{}(u_.str_);
        case TTL_STR_TAG:
            return std::hash<std::string>{}(u_.ttl_str_.str_); // 这里简化处理了，注意一下
        default:
            
            LOG(FATAL) << "Invalid tag: " << tag_;
            return 0;
        }      

}

uint64_t CompactObj::HashCode(std::string_view str) {
  return std::hash<std::string_view>{}(str);
}




CompactObjType CompactObj::ObjType() const {
    if (tag_ == 0)
        return kInvalidCompactObjType; // 新建的键没有类型
    if (tag_ == INT_TAG || tag_ == STR_TAG || tag_ == TTL_STR_TAG)
        return OBJ_STRING; // 返回字符串类型
    if (tag_ == ROBJ_TAG)
        return u_.r_obj_.type();
    LOG(FATAL) << "TBD " << int(tag_);
    return kInvalidCompactObjType;
}




void CompactKey::SetExpireTime(uint64_t abs_ms) {
    if (tag_ == TTL_STR_TAG) {
        u_.ttl_str_.exp_ms_ = abs_ms;
        return;
    }

    u_.ttl_str_.str_ = u_.str_;
    u_.ttl_str_.exp_ms_ = abs_ms;
    tag_ = TTL_STR_TAG;
}

bool CompactKey::ClearExpireTime() {
    if (tag_ != TTL_STR_TAG)
        return false;

    std::string str=std::move(u_.ttl_str_.str_); // 会有问题吗
    SetString(str);
    return true;
}

uint64_t CompactKey::GetExpireTime() const {
    if (tag_ != TTL_STR_TAG)
        return 0;
    return u_.ttl_str_.exp_ms_;
}














}
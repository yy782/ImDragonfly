

#include "compact_obj.hpp"
#include "redis/redis_aux.hpp"
namespace dfly{



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
    return OBJ_STRING;
}




void CompactKey::SetExpireTime(uint64_t abs_ms) {
    if (tag_ == TTL_STR_TAG) {
        u_.ttl_str_.exp_ms_ = abs_ms;
        return;
    }

    u_.ttl_str_.str_ = std::move(u_.str_);
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
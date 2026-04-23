#include "compact_obj.hpp"
#include "redis_aux.hpp"
namespace dfly{


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

bool CompactObj::operator==(const CompactObj& o) noexcept{
    return u_.str_==o.u_.str_;
}
}
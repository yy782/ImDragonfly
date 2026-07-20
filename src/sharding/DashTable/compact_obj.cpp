#include "compact_obj.hpp"

namespace dfly {

uint64_t CompactObj::HashCode() const {
    switch (tag_) {
    case INT_TAG:
        return std::hash<int64_t>{}(u_.ival_);
    case STR_TAG:
        return std::hash<std::string>{}(u_.str_);
    case TTL_STR_TAG:
        // Hash only the key part (same semantics as plain STR_TAG)
        return std::hash<std::string>{}(u_.ttl_.val);
    default:
        LOG(FATAL) << "HashCode: invalid tag " << int(tag_);
        return 0;
    }
}

uint64_t CompactObj::HashCode(std::string_view str) {
    return std::hash<std::string_view>{}(str);
}

}  // namespace dfly

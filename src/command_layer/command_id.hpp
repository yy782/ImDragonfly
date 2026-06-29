// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <limits>
namespace facade {
const size_t kInvalidKeysStart = std::numeric_limits<size_t>::max() - 1;
const size_t kInvalidKeysNum = std::numeric_limits<size_t>::max() - 1;
const size_t kInvalidKeysOffset = std::numeric_limits<size_t>::max() - 1;
class CommandId {
public:
    CommandId(const char* name, size_t keys_start, size_t keys_nums, size_t keys_offset, uint32_t opt_mask = 0);

    std::string_view name() const {
        return name_;
    }

    size_t keys_nums() const {
        return keys_nums_;
    }

    size_t keys_offset() const {
        return keys_offset_;
    }

    size_t keys_start() const {
        return keys_start_;
    }


    uint32_t opt_mask() const {
        return opt_mask_;
    }


    CommandId& SetFamily(size_t fam) {
        family_ = fam;
        return *this;
    }


    size_t GetFamily() const {
        return family_;
    }

protected:
    std::string name_; // 命令名称

    uint32_t opt_mask_; // 选项掩码（READONLY, FAST, JOURNALED 等）
    // Acl commands indices
    size_t family_; // 所属命令家族索引

    size_t keys_start_;
    size_t keys_nums_;
    size_t keys_offset_;
};

}  // namespace facade

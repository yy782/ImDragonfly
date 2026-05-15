// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace facade {

class CommandId {
public:
    CommandId(const char* name, int8_t arity, int8_t first_key, int8_t last_key);

    std::string_view name() const {
        return name_;
    }

    int arity() const {
        return arity_;
    }

    uint32_t opt_mask() const {
        return opt_mask_;
    }

    int8_t first_key_pos() const {
        return first_key_;
    }

    int8_t last_key_pos() const {
        return last_key_;
    }

    void SetFamily(size_t fam) {
        family_ = fam;
    }

    void SetBitIndex(uint64_t bit) {
        bit_index_ = bit;
    }

    size_t GetFamily() const {
        return family_;
    }

    uint64_t GetBitIndex() const {
        return bit_index_;
    }

    // bool IsRestricted() const {
    //     return restricted_;
    // }

    // void SetRestricted(bool restricted) {
    //     restricted_ = restricted;
    // }

    void SetFlag(uint32_t flag) {
        opt_mask_ |= flag;
    }

protected:
    std::string name_; // 命令名称

    uint32_t opt_mask_; // 选项掩码（READONLY, FAST, JOURNALED 等）
    int8_t arity_; // 参数数量（正=固定，负=最少）
    int8_t first_key_;
    int8_t last_key_;

    // Acl commands indices
    size_t family_; // 所属命令家族索引
    uint64_t bit_index_;
};

}  // namespace facade

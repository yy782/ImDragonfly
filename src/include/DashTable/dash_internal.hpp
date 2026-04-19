#pragma once 
namespace dfly {
namespace detail {

// SlotBitmap
// 模式1：单整数模式
// ┌─────────────────────────────────────────────────────────────────┐
// │                    单个 uint32_t (32 位)                        │
// ├───────────────┬───────────────┬───────────────┬───────────────┤
// │   高 14 位     │   中 14 位     │   低 4 位     │               │
// │   (busy 位图)  │   (probe 位图) │(槽位计数size_)│               │
// │   位 18-31     │   位 4-17      │   位 0-3      │               │
// └───────────────┴───────────────┴───────────────┴───────────────┘
// 模式2：双整数模式
// ┌─────────────────────────────────────────────────────────────────┐
// │                    第一个 uint32_t                              │
// ├─────────────────────────────────────────────────────────────────┤
// │                      busy 位图 (低 28 位)                        │
// │                      高 4 位未使用                               │
// └─────────────────────────────────────────────────────────────────┘

// ┌─────────────────────────────────────────────────────────────────┐
// │                    第二个 uint32_t                              │
// ├─────────────────────────────────────────────────────────────────┤
// │                      probe 位图 (低 28 位)                       │
// │                      高 4 位未使用                               │
// └─────────────────────────────────────────────────────────────────┘

template <unsigned NUM_SLOTS> 
class SlotBitmap {
    static_assert(NUM_SLOTS > 0 && NUM_SLOTS <= 28); // 超过 28 个槽位，单个 uint32_t（32 位）存不下所有状态
    static constexpr bool SINGLE = NUM_SLOTS <= 14; // 是否可以使用单 32 位整数存储位图 , 每个槽位需要 2 位信息（busy + probe）
    static constexpr unsigned kLen = SINGLE ? 1 : 2;
    static constexpr unsigned kAllocMask = (1u << NUM_SLOTS) - 1; // 槽位掩码 ， 用于操作高 14 位的 busy 位图
    static constexpr unsigned kBitmapLenMask = (1 << 4) - 1; // 长度掩码 , 对应已用槽位数量

public:
    uint32_t GetProbe(bool probe) const {
        if constexpr (SINGLE)
            return ((val_[0].d >> 4) & kAllocMask) ^ ((!probe) * kAllocMask);
        else
            return (val_[1].d & kAllocMask) ^ ((!probe) * kAllocMask);
    }
    uint32_t GetBusy() const {
        return SINGLE ? val_[0].d >> 18 : val_[0].d;
    }

    bool IsFull() const {
        return Size() == NUM_SLOTS;
    }

    unsigned Size() const {
        return SINGLE ? (val_[0].d & kBitmapLenMask) : __builtin_popcount(val_[0].d);
    }
    int FindEmptySlot() const {
        uint32_t mask = ~(GetBusy());
        int slot = __builtin_ctz(mask);
        assert(slot < int(NUM_SLOTS));
        return slot;
    }
    void ClearSlots(uint32_t mask){
        if (SINGLE) {
            uint32_t count = __builtin_popcount(mask);
            assert(count <= (val_[0].d & 0xFF));
            mask = (mask << 4) | (mask << 18);
            val_[0].d &= ~mask;
            val_[0].d -= count;
        } else {
            val_[0].d &= ~mask;
            val_[1].d &= ~mask;
        }
    }


    void Clear() {
        if (SINGLE) {
            val_[0].d = 0;
        } else {
            val_[0].d = val_[1].d = 0;
        }
    }

    void ClearSlot(unsigned index);
    void SetSlot(unsigned index, bool probe);

    bool ShiftLeft();

    void Swap(unsigned slot_a, unsigned slot_b);

private:
    struct Unaligned {
        // 强制非对齐，性能换内存? ，可能不会牺牲性能，对CPU缓存友好?
        uint32_t d __attribute__((packed, aligned(1)));

        Unaligned() : d(0) {
        }
    };

    Unaligned val_[kLen];
};  // SlotBitmap



template <unsigned NUM_SLOTS> 
class BucketBase {
    static constexpr unsigned kStashFpLen = 4;
    static constexpr unsigned kStashPresentBit = 1 << 4;

    using FpArray = std::array<uint8_t, NUM_SLOTS>;
    using StashFpArray = std::array<uint8_t, kStashFpLen>;

public:
    using SlotId = uint8_t;
    static constexpr SlotId kNanSlot = 255;

    bool IsFull() const {
        return Size() == NUM_SLOTS;
    }

    bool IsEmpty() const {
        return GetBusy() == 0;
    }

    unsigned Size() const {
        return slotb_.Size();
    }

    void Delete(SlotId sid) {
        slotb_.ClearSlot(sid);
    }

    unsigned Find(uint8_t fp_hash, bool probe) const {
        unsigned mask = CompareFP(fp_hash) & GetBusy();
        return mask & GetProbe(probe);
    }

    uint8_t Fp(unsigned i) const {
        assert(i < finger_arr_.size());
        return finger_arr_[i];
    }

    // void SetStashPtr(unsigned stash_pos, uint8_t meta_hash, BucketBase* next);
    // unsigned UnsetStashPtr(uint8_t fp_hash, unsigned stash_pos, BucketBase* next);
    uint32_t GetProbe(bool probe) const {
        return slotb_.GetProbe(probe);
    }
    uint32_t GetBusy() const {
        return slotb_.GetBusy();
    }

    bool IsBusy(unsigned slot) const {
        return (GetBusy() & (1u << slot)) != 0;
    }
    void ClearSlots(uint32_t mask) {
        slotb_.ClearSlots(mask);
    }
    void Clear() {
        slotb_.Clear();
    }

    void ClearStashPtrs() {
        stash_busy_ = 0;
        stash_pos_ = 0;
        stash_probe_mask_ = 0;
        overflow_count_ = 0;
    }

    bool HasStash() const {
        return stash_busy_ & kStashPresentBit;
    }

    // void SetHash(unsigned slot_id, uint8_t meta_hash, bool probe);

    bool HasStashOverflow() const {
        return overflow_count_ > 0;
    }
    // template <typename F>
    // std::pair<unsigned, SlotId> IterateStash(uint8_t fp, bool is_probe, F&& func) const;

    void Swap(unsigned slot_a, unsigned slot_b) {
        slotb_.Swap(slot_a, slot_b);
        std::swap(finger_arr_[slot_a], finger_arr_[slot_b]);
    }

protected:
    uint32_t CompareFP(uint8_t fp) const;
    bool ShiftRight();
    bool SetStash(uint8_t fp, unsigned stash_pos, bool probe);
    bool ClearStash(uint8_t fp, unsigned stash_pos, bool probe);

    SlotBitmap<NUM_SLOTS> slotb_;  // allocation bitmap + pointer bitmap + counter

    /*only use the first 14 bytes, can be accelerated by
        SSE instruction,0-13 for finger, 14-17 for overflowed*/
    FpArray finger_arr_;
    StashFpArray stash_arr_;

    uint8_t stash_busy_ = 0;  // kStashFpLen+1 bits are used
    uint8_t stash_pos_ = 0;   // 4x2 bits for pointing to stash bucket.

    // stash_probe_mask_ indicates whether the overflow fingerprint is for the neighbour (1)
    // or for this bucket (0). kStashFpLen bits are used.
    uint8_t stash_probe_mask_ = 0;

    // number of overflowed items stored in stash buckets that do not have fp hashes.
    uint8_t overflow_count_ = 0;
};  // BucketBase

struct DefaultSegmentPolicy {
    static constexpr unsigned kSlotNum = 12;
    static constexpr unsigned kBucketNum = 64;
    static constexpr unsigned  kStashBucketNum = 4;
    // static constexpr bool kUseVersion = true;
};

using PhysicalBid = uint8_t; // 数据实际存储的桶位置
using LogicalBid = uint8_t; // 键经过哈希后应该归属的桶位置

template <typename KeyType, typename ValueType, typename Policy = DefaultSegmentPolicy>
class Segment {
    static constexpr unsigned kSlotNum = Policy::kSlotNum;
    static constexpr unsigned kBucketNum = Policy::kBucketNum;
    static constexpr unsigned kStashBucketNum = Policy::kStashBucketNum; // not same
    // static constexpr bool kUseVersion = Policy::kUseVersion;
public:
};

template <unsigned NUM_SLOTS> 
uint32_t BucketBase<NUM_SLOTS>::CompareFP(uint8_t fp) const {
    static_assert(FpArray{}.size() <= 16);
    const __m128i key_data = _mm_set1_epi8(fp);
    __m128i seg_data = mm_loadu_si128(reinterpret_cast<const __m128i*>(finger_arr_.data()));
    __m128i rv_mask = _mm_cmpeq_epi8(seg_data, key_data);
    int mask = _mm_movemask_epi8(rv_mask);
    return mask;
}


}
}
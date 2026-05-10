#pragma once 

#include <assert.h>
#include <immintrin.h>

#include "detail/memory_resource.hpp"

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

    void ClearSlot(unsigned index)
    {
        assert(Size() > 0);
        if constexpr (SINGLE) {
            uint32_t new_bitmap = val_[0].d & 
                (~(1u << (index + 18))) & (~(1u << (index + 4)));
            new_bitmap -= 1;
            val_[0].d = new_bitmap;
        } else {
            uint32_t mask = 1u << index;
            val_[0].d &= ~mask;
            val_[1].d &= ~mask;
        }        
    }

    void SetSlot(unsigned index, bool probe){
        if constexpr (SINGLE) {
            assert(((val_[0].d >> (index + 18)) & 1) == 0);
            val_[0].d |= (1 << (index + 18));
            val_[0].d |= (unsigned(probe) << (index + 4));

            assert((val_[0].d & kBitmapLenMask) < NUM_SLOTS);
            ++val_[0].d;
            assert(__builtin_popcount(val_[0].d >> 18) == (val_[0].d & kBitmapLenMask));
        } else {
            assert(((val_[0].d >> index) & 1) == 0);
            val_[0].d |= (1u << index);
            val_[1].d |= (unsigned(probe) << index);
        }
    }

    bool ShiftLeft(){
        constexpr uint32_t kBusyLastSlot = (kAllocMask >> 1) + 1;
        bool res;
        if constexpr (SINGLE) {
            constexpr uint32_t kShlMask = kAllocMask - 1;  // reset lsb
            res = (val_[0].d & (kBusyLastSlot << 18)) != 0;
            uint32_t l = (val_[0].d << 1) & (kShlMask << 4);
            uint32_t p = (val_[0].d << 1) & (kShlMask << 18);
            val_[0].d = __builtin_popcount(p) | l | p;
        } else {
            res = (val_[0].d & kBusyLastSlot) != 0;
            val_[0].d <<= 1;
            val_[0].d &= kAllocMask;
            val_[1].d <<= 1;
            val_[1].d &= kAllocMask;
        }
        return res;        
    }

    void Swap(unsigned slot_a, unsigned slot_b)
    {
        if (slot_a > slot_b)
            std::swap(slot_a, slot_b);

        if constexpr (SINGLE) {
            uint32_t a = (val_[0].d << (slot_b - slot_a)) ^ val_[0].d;
            uint32_t bm = (1 << (slot_b + 4)) | (1 << (slot_b + 18));
            a &= bm;
            a |= (a >> (slot_b - slot_a));
            val_[0].d ^= a;
        } else {
            uint32_t a = (val_[0].d << (slot_b - slot_a)) ^ val_[0].d;
            a &= (1 << slot_b);
            a |= (a >> (slot_b - slot_a));
            val_[0].d ^= a;

            a = (val_[1].d << (slot_b - slot_a)) ^ val_[1].d;
            a &= (1 << slot_b);
            a |= (a >> (slot_b - slot_a));
            val_[1].d ^= a;
        }
    }

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


    template <typename F>
    std::pair<unsigned, SlotId> IterateStash(uint8_t fp, bool is_probe, F&& func) const{
        unsigned om = is_probe ? stash_probe_mask_ : ~stash_probe_mask_;
        unsigned ob = stash_busy_;

        for (unsigned i = 0; i < kStashFpLen; ++i) {
            if ((ob & 1) && (stash_arr_[i] == fp) && (om & 1)) {
                unsigned pos = (stash_pos_ >> (i * 2)) & 3;
                auto sid = func(i, pos);
                if (sid != BucketBase::kNanSlot) {
                    return std::pair<unsigned, SlotId>(pos, sid);
                }
            }
            ob >>= 1;
            om >>= 1;
        }
        return {0, BucketBase::kNanSlot};
    }

    void SetStashPtr(unsigned stash_pos, uint8_t meta_hash, BucketBase* next){
        assert(stash_pos < 4);
        if (!SetStash(meta_hash, stash_pos, false)) {
            if (!next->SetStash(meta_hash, stash_pos, true)) {
                overflow_count_++;
            }
        }
        stash_busy_ |= kStashPresentBit;
    }

    unsigned UnsetStashPtr(uint8_t fp_hash, unsigned stash_pos, BucketBase* next){
  /*also needs to ensure that this meta_hash must belongs to other bucket*/
        bool clear_success = ClearStash(fp_hash, stash_pos, false);
        unsigned res = 0;

        if (!clear_success) {
            clear_success = next->ClearStash(fp_hash, stash_pos, true);
            res += clear_success;
        }

        if (!clear_success) {
            assert(overflow_count_ > 0);
            overflow_count_--;
        }
        unsigned mask1 = stash_busy_ & (kStashPresentBit - 1);
        unsigned mask2 = next->stash_busy_ & (kStashPresentBit - 1);

        if (((mask1 & (~stash_probe_mask_)) == 0) && (overflow_count_ == 0) &&
            ((mask2 & next->stash_probe_mask_) == 0)) {
            stash_busy_ &= ~kStashPresentBit;
        }

        return res;        
    }

protected:
    uint32_t CompareFP(uint8_t fp) const{
        static_assert(FpArray{}.size() <= 16);

        // Replicate 16 times fp to key_data.
        const __m128i key_data = _mm_set1_epi8(fp);

        // Loads 16 bytes of src into seg_data.
        __m128i seg_data = _mm_loadu_si128(reinterpret_cast<const __m128i*>(finger_arr_.data()));

        // compare 16-byte vectors seg_data and key_data, dst[i] := ( a[i] == b[i] ) ? 0xFF : 0.
        __m128i rv_mask = _mm_cmpeq_epi8(seg_data, key_data);

        // collapses 16 msb bits from each byte in rv_mask into mask.
        int mask = _mm_movemask_epi8(rv_mask);

        // Note: Last 2 operations can be combined in skylake with _mm_cmpeq_epi8_mask.
        return mask;        
    }

    bool ShiftRight(){
        for (int i = NUM_SLOTS - 1; i > 0; --i) {
            finger_arr_[i] = finger_arr_[i - 1];
        }
        bool res = slotb_.ShiftLeft();
        assert(slotb_.FindEmptySlot() == 0);
        return res;        
    }

    bool SetStash(uint8_t fp, unsigned stash_pos, bool probe){
        unsigned free_slot = __builtin_ctz(~stash_busy_);
        if (free_slot >= kStashFpLen)
            return false;

        stash_arr_[free_slot] = fp;
        stash_busy_ |= (1u << free_slot); 
        stash_probe_mask_ |= (unsigned(probe) << free_slot);
        free_slot *= 2;
        stash_pos_ &= (~(3 << free_slot));       
        stash_pos_ |= (stash_pos << free_slot);  
        return true;        
    }


    bool ClearStash(uint8_t fp, unsigned stash_pos, bool probe){
        auto cb = [stash_pos, this](unsigned i, unsigned pos) -> SlotId {
            if (pos == stash_pos) {
            stash_busy_ &= (~(1u << i));
            stash_probe_mask_ &= (~(1u << i));
            stash_pos_ &= (~(3u << (i * 2)));

            assert(0u == ((stash_pos_ >> (i * 2)) & 3));
            return 0;
            }
            return kNanSlot;
        };

        std::pair<unsigned, SlotId> res = IterateStash(fp, probe, std::move(cb));
        return res.second != kNanSlot;        
    }

    SlotBitmap<NUM_SLOTS> slotb_;  
    FpArray finger_arr_;
    StashFpArray stash_arr_; // 存储 Stash 槽位的指纹

    uint8_t stash_busy_ = 0;  
    uint8_t stash_pos_ = 0;   
    uint8_t stash_probe_mask_ = 0;


    uint8_t overflow_count_ = 0;  // 溢出计数器。记录有多少个 Stash 引用被“溢出”存储到了邻居桶中。
};  // BucketBase

struct DefaultSegmentPolicy {
    static constexpr unsigned kSlotNum = 12;
    static constexpr unsigned kBucketNum = 64;
    // static constexpr unsigned  kStashBucketNum = 4;
    // static constexpr bool kUseVersion = true;
};

using PhysicalBid = uint8_t; // 数据实际存储的桶位置
using LogicalBid = uint8_t; // 键经过哈希后应该归属的桶位置

template <typename KeyType, typename ValueType, typename Policy = DefaultSegmentPolicy>
class Segment {
    static constexpr unsigned kSlotNum = Policy::kSlotNum;
    static constexpr unsigned kBucketNum = Policy::kBucketNum;
    static constexpr unsigned kStashBucketNum = 4;
    // static constexpr bool kUseVersion = Policy::kUseVersion;
    static constexpr unsigned kFingerBits = 8;
    static constexpr unsigned kTotalBuckets = kBucketNum + kStashBucketNum;
    static_assert(kTotalBuckets < 0xFF);
    using BucketType = BucketBase<kSlotNum>;

    struct Bucket : public BucketType{
        using BucketType::kNanSlot;
        using typename BucketType::SlotId;
        KeyType key[kSlotNum];
        ValueType value[kSlotNum];  
        
        template <typename U, typename V>
        void Insert(uint8_t slot, U&& u, V&& v, uint8_t meta_hash, bool probe);
        template <typename U, typename V>
        int TryInsertToBucket(U&& new_key, V&& new_value, uint8_t meta_hash, bool probe);  
        template <typename Pred> 
        SlotId FindByFp(uint8_t fp_hash, bool probe, Pred&& pred) const;

        bool ShiftRight();

        void Swap(unsigned slot_a, unsigned slot_b) {
            BucketType::Swap(slot_a, slot_b);
            std::swap(key[slot_a], key[slot_b]);
            std::swap(value[slot_a], value[slot_b]);
        }

        template <typename This, typename Cb> 
        void ForEachSlotImpl(This obj, Cb&& cb) const ;

        // calls for each busy slot: cb(iterator, probe)
        template <typename Cb> void ForEachSlot(Cb&& cb) const {
            ForEachSlotImpl(this, std::forward<Cb&&>(cb));
        }

        // calls for each busy slot: cb(iterator, probe)
        template <typename Cb> void ForEachSlot(Cb&& cb) {
            ForEachSlotImpl(this, std::forward<Cb&&>(cb));
        }  
    };



    static constexpr PhysicalBid kNanBid = 0xFF;
    using SlotId = typename BucketType::SlotId;    
public:
    struct Iterator {
        PhysicalBid index;  // bucket index
        uint8_t slot;

        Iterator() : index(kNanBid), slot(BucketType::kNanSlot) {
        }

        Iterator(PhysicalBid bi, uint8_t sid) : index(bi), slot(sid) {
        }

        bool found() const {
            return index != kNanBid;
        }
    };


    static constexpr size_t kFpMask = (1 << kFingerBits) - 1;
    using Value_t = ValueType;
    using Key_t = KeyType;
    using Hash_t = uint64_t;

    explicit Segment(size_t depth, uint32_t id, PMR_NS::memory_resource* mr)
        : local_depth_(depth), segment_id_(id), mr_(mr) {
    }

    ~Segment() {
        Clear();
    }

    Segment(const Segment&) = delete;
    Segment& operator=(const Segment&) = delete;



    template <typename K, typename V, typename Pred, typename OnMoveCb>
    std::pair<Iterator, bool> Insert(K&& key, V&& value, Hash_t key_hash, Pred&& pred,
                                    OnMoveCb&& on_move_cb);

    template <typename U, typename V, typename OnMoveCb>
    Iterator InsertUniq(U&& key, V&& value, Hash_t key_hash, 
                        bool spread, //  是否在主桶和邻居桶之间做负载均衡
                        /*
                            spread true:
                                选择负载较小的桶（主桶或邻居桶）
                            spread false:
                                 优先选择主桶   	   
                        */                          
                        OnMoveCb&& on_move_cb); // 条目移动时的回调（用于通知淘汰策略）  
 
    template <typename Pred>
    auto FindIt(Hash_t key_hash, Pred&& pred) const -> Iterator;                        

                        
    template <typename HashFn, typename OnMoveCb>
    void Split(HashFn&& hfunc, Segment* dest, OnMoveCb&& on_move_cb);

    void Delete(const Iterator& it, Hash_t key_hash);

    void Clear();  // clears the segment.

    size_t SlowSize() const;
    


    size_t local_depth() const {
        return local_depth_;
    }

    void set_local_depth(uint32_t depth) {
        local_depth_ = depth;
    }

    unsigned num_buckets() const {
        return kBucketNum + kStashBucketNum;
    }
    uint32_t segment_id() const {
        return segment_id_;
    }
    void set_segment_id(uint32_t new_id) {
        segment_id_ = new_id;
    }
    const Bucket& GetBucket(PhysicalBid i) const {
        return bucket_[i];
    }

    Bucket& GetBucket(PhysicalBid i) {
        return bucket_[i];
    }

    bool IsBusy(PhysicalBid bid, unsigned slot) const {
        return GetBucket(bid).GetBusy() & (1U << slot);
    }    
    Key_t& Key(PhysicalBid bid, unsigned slot) {
        assert(IsBusy(bid, slot));
        return GetBucket(bid).key[slot];
    }

    const Key_t& Key(PhysicalBid bid, unsigned slot) const {
        assert(IsBusy(bid, slot));
        return GetBucket(bid).key[slot];
    }

    Value_t& Value(PhysicalBid bid, unsigned slot) {
        assert(IsBusy(bid, slot));
        return GetBucket(bid).value[slot];
    }

    const Value_t& Value(PhysicalBid bid, unsigned slot) const {
        assert(IsBusy(bid, slot));
        return GetBucket(bid).value[slot];
    }



    template <typename Cb, typename HashFn>
    bool TraverseLogicalBucket(LogicalBid bid, HashFn&& hfun, Cb&& cb) const;

    template <typename Cb> 
    void TraverseAll(Cb&& cb) const; // 对当前 Segment 中所有被占用的槽位遍历接口

    int MoveToOther(bool own_items, unsigned from, unsigned to);

    void RemoveStashReference(unsigned stash_pos, Hash_t key_hash);

    auto TryMoveFromStash(unsigned stash_id, unsigned stash_slot_id,
                                                   Hash_t key_hash) -> Iterator;
    
private:

    static LogicalBid HomeIndex(Hash_t hash) { // 计算主桶位置
        return (hash >> kFingerBits) % kBucketNum;
    }

    static LogicalBid NextBid(LogicalBid bid) { // 下一个桶（线性探测）
        return bid < kBucketNum - 1 ? bid + 1 : 0;
    }

    static LogicalBid PrevBid(LogicalBid bid) { // 上一个桶
        return bid ? bid - 1 : kBucketNum - 1;
    }

    auto FindValidStartingFrom(PhysicalBid bid, unsigned slot) const-> Iterator;
    Bucket bucket_[kTotalBuckets];
    uint8_t local_depth_; 
    uint32_t segment_id_;  // segment id in the table.
    PMR_NS::memory_resource* mr_ = nullptr;
};

class DashTableBase {
 public:
    explicit DashTableBase(uint32_t gd)
        : unique_segments_(1 << gd), initial_depth_(gd), global_depth_(gd) {
    }

    DashTableBase(const DashTableBase&) = delete;
    DashTableBase& operator=(const DashTableBase&) = delete;

    uint32_t unique_segments() const {
        return unique_segments_;
    }

    uint16_t depth() const {
        return global_depth_;
    }

    size_t size() const {
        return size_;
    }

    size_t Empty() const {
        return size_ == 0;
    }

 protected:
    uint32_t SegmentId(size_t hash) const {
        if (global_depth_) {
            return hash >> (64 - global_depth_);
        }

        return 0;
    }

    size_t size_ = 0;
    uint32_t unique_segments_ = 0; // 实际段数
    uint32_t bucket_count_ = 0;
    uint8_t initial_depth_;
    uint8_t global_depth_;
};  // DashTableBase
template <typename KeyType, typename ValueType> 
struct IteratorPair {
    IteratorPair(KeyType& k, ValueType& v) : 
    first(k), second(v) {
    }

    IteratorPair* operator->() {
        return this;
    }

    const IteratorPair* operator->() const {
        return this;
    }

    KeyType& first;
    ValueType& second;
};
class DashCursor {
public:
    explicit DashCursor(uint64_t token = 0) : val_(token) {
    }

    DashCursor(uint8_t depth, uint32_t seg_id, PhysicalBid bid)
        : val_((uint64_t(seg_id) << (40 - depth)) | bid) {
    }

    static DashCursor end() {
        return DashCursor{};
    }

    PhysicalBid bucket_id() const {
        return val_ & 0xFF;
    }
    uint32_t segment_id(uint8_t depth) const {
        return val_ >> (40 - depth);
    }
    uint64_t token() const {
        return val_;
    }
    explicit operator bool() const { // 为什么要 explicit
        return val_ != 0;
    }
private:
    uint64_t val_;
};


template <typename Key, typename Value, typename Policy>
template <typename U, typename V>
int Segment<Key, Value, Policy>::Bucket::TryInsertToBucket(U&& new_key, V&& new_value, 
                                                            uint8_t meta_hash, bool probe)
{
    if (this->IsFull()) { // ???? 不加this,会报错？？？ 告诉编译器是由依赖的
        return -1;  // no free space in the bucket.
    }

    int slot = this->slotb_.FindEmptySlot();
    assert(slot >= 0);
    Insert(slot, std::forward<U>(new_key), std::forward<V>(new_value), meta_hash, probe);
    return slot;    
}

template <typename Key, typename Value, typename Policy>
bool Segment<Key, Value, Policy>::Bucket::ShiftRight() {
    bool res = BucketType::ShiftRight();
    for (int i = kSlotNum - 1; i > 0; i--) {
        std::swap(key[i], key[i - 1]);
        std::swap(value[i], value[i - 1]);
    }
    return res;
}

template <typename Key, typename Value, typename Policy>
template <typename U, typename V>
void Segment<Key, Value, Policy>::Bucket::Insert(uint8_t slot, U&& u, V&& v, 
                                                uint8_t meta_hash, bool probe)
{
    assert(slot < kSlotNum);
    key[slot] = std::forward<U>(u);
    value[slot] = std::forward<V>(v);
    this->SetStash(slot, meta_hash, probe);  // not same   
}
template <typename Key, typename Value, typename Policy>
template <typename This, typename Cb>
void Segment<Key, Value, Policy>::Bucket::ForEachSlotImpl(This obj, Cb&& cb) const {
    uint32_t mask = this->GetBusy();
    uint32_t probe_mask = this->GetProbe(true);

    for (unsigned j = 0; j < kSlotNum; ++j) {
        if (mask & 1) {
            cb(obj, j, probe_mask & 1);
        }
        mask >>= 1;
        probe_mask >>= 1;
    }
}


template <typename Key, typename Value, typename Policy>
template <typename Pred>
auto Segment<Key, Value, Policy>::Bucket::FindByFp(uint8_t fp_hash, bool probe, Pred&& pred) const
    -> SlotId {
    unsigned mask = this->Find(fp_hash, probe);
    if (!mask)
        return kNanSlot;

    unsigned delta = __builtin_ctz(mask);
    mask >>= delta;
    for (unsigned i = delta; i < kSlotNum; ++i) {
        // Filterable just by key
        if constexpr (std::is_invocable_v<Pred, const Key_t&>) {
            if ((mask & 1) && pred(key[i]))
                return i;
        }

        // Filterable by key and value
        if constexpr (std::is_invocable_v<Pred, const Key_t&, const Value_t&>) {
            if ((mask & 1) && pred(key[i], value[i]))
                return i;
        }

        mask >>= 1;
    };

    return kNanSlot;
}



template <typename Key, typename Value, typename Policy>
template <typename U, typename V, typename Pred, typename OnMoveCb>
auto Segment<Key, Value, Policy>::Insert(U&& key, V&& value, Hash_t key_hash, Pred&& pred,
                                         OnMoveCb&& on_move_cb) -> std::pair<Iterator, bool> {
    Iterator it = FindIt(key_hash, pred);
    if (it.found()) {
        return std::make_pair(it, false); /* duplicate insert*/
    }

    it = InsertUniq(std::forward<U>(key), std::forward<V>(value), key_hash, true,
                    std::forward<OnMoveCb>(on_move_cb));

    return std::make_pair(it, it.found());
}

template <typename Key, typename Value, typename Policy>
template <typename U, typename V, typename OnMoveCb>
auto Segment<Key, Value, Policy>::InsertUniq(U&& key, V&& value, Hash_t key_hash, bool spread,
                                             OnMoveCb&& on_move_cb) -> Iterator {
    const uint8_t bid = HomeIndex(key_hash);
    const uint8_t nid = NextBid(bid); 

    Bucket& target = bucket_[bid]; // 主桶
    Bucket& neighbor = bucket_[nid]; // 邻居桶
    Bucket* insert_first = &target; 

    uint8_t meta_hash = key_hash & kFpMask; // 8 位指纹，用于快速过滤
    unsigned ts = target.Size(), ns = neighbor.Size();
    bool probe = false;

    if (spread && ts > ns) {
        insert_first = &neighbor;
        probe = true;
    }

    int slot = insert_first->TryInsertToBucket(std::forward<U>(key), std::forward<V>(value),
                                                meta_hash, probe);

    if (slot >= 0) {
        return Iterator{PhysicalBid(insert_first - bucket_), uint8_t(slot)};
    }

    if (!spread) {
        slot =
            neighbor.TryInsertToBucket(std::forward<U>(key), std::forward<V>(value), meta_hash, true);
        if (slot >= 0) {
            return Iterator{nid, uint8_t(slot)};
        }
    }

    int displace_index = MoveToOther(true, nid, NextBid(nid));
    if (displace_index >= 0) {
        neighbor.Insert(displace_index, std::forward<U>(key), std::forward<V>(value), meta_hash, true);
        on_move_cb(segment_id_, nid, NextBid(nid));
        return Iterator{nid, uint8_t(displace_index)};
    }

    unsigned prev_idx = PrevBid(bid);
    displace_index = MoveToOther(false, bid, prev_idx);
    if (displace_index >= 0) {
        target.Insert(displace_index, std::forward<U>(key), std::forward<V>(value), meta_hash, false);
        on_move_cb(segment_id_, bid, prev_idx);
        return Iterator{bid, uint8_t(displace_index)};
    }

    // we balance stash fill rate  by starting from y % STASH_BUCKET_NUM.
    for (unsigned i = 0; i < kStashBucketNum; ++i) {
        unsigned stash_pos = (bid + i) % kStashBucketNum;

        int stash_slot = bucket_[kBucketNum + stash_pos].TryInsertToBucket(
            std::forward<U>(key), std::forward<V>(value), meta_hash, false);
        if (stash_slot >= 0) {
        target.SetStashPtr(stash_pos, meta_hash, &neighbor);
        return Iterator{PhysicalBid(kBucketNum + stash_pos), uint8_t(stash_slot)};
        }
    }

    return Iterator{};
}


template <typename Key, typename Value, typename Policy>
template <typename Pred>
auto Segment<Key, Value, Policy>::FindIt(Hash_t key_hash, Pred&& pred) const -> Iterator {
    LogicalBid bidx = HomeIndex(key_hash);
    const Bucket& target = bucket_[bidx];
    __builtin_prefetch(&target);

    uint8_t fp_hash = key_hash & kFpMask;
    SlotId sid = target.FindByFp(fp_hash, false, pred); //  指纹查找
    if (sid != BucketType::kNanSlot) {
        return Iterator{bidx, sid};
    }

    LogicalBid nid = NextBid(bidx);
    const Bucket& probe = GetBucket(nid);
    sid = probe.FindByFp(fp_hash, true, pred); // 邻居桶查找

    if (sid != BucketType::kNanSlot) {
        return Iterator{nid, sid};
    }

    if (!target.HasStash()) {
        return Iterator{};
    }

    auto stash_cb = [&](unsigned overflow_index, PhysicalBid pos) -> SlotId {

        (void)overflow_index;

        assert(pos < kStashBucketNum);

        pos += kBucketNum;
        const Bucket& bucket = bucket_[pos];
        return bucket.FindByFp(fp_hash, false, pred);
    };

    if (target.HasStashOverflow()) { // Stash 溢出
        for (unsigned i = 0; i < kStashBucketNum; ++i) {
        auto st_sid = stash_cb(0, i);
            if (st_sid != BucketType::kNanSlot) {
                return Iterator{PhysicalBid(kBucketNum + i), st_sid};
            }
        }
        return Iterator{};
    }

    auto stash_res = target.IterateStash(fp_hash, false, stash_cb); // 正常 Stash
    if (stash_res.second != BucketType::kNanSlot) {
        return Iterator{PhysicalBid(kBucketNum + stash_res.first), stash_res.second};
    }

    stash_res = probe.IterateStash(fp_hash, true, stash_cb);
    if (stash_res.second != BucketType::kNanSlot) {
        return Iterator{PhysicalBid(kBucketNum + stash_res.first), stash_res.second};
    }
    return Iterator{};
}


template <typename Key, typename Value, typename Policy>
void Segment<Key, Value, Policy>::Delete(const Iterator& it, Hash_t key_hash) {
    assert(it.found());

    auto& b = bucket_[it.index];

    if (it.index >= kBucketNum) {
        RemoveStashReference(it.index - kBucketNum, key_hash);
    }

    b.Delete(it.slot);
}

template <typename Key, typename Value, typename Policy> 
void Segment<Key, Value, Policy>::Clear() {
    for (unsigned i = 0; i < kTotalBuckets; ++i) {
        bucket_[i].Clear();
        bucket_[i].ClearStashPtrs();
    }
}

template <typename Key, typename Value, typename Policy>
template <typename HFunc, typename MoveCb>
void Segment<Key, Value, Policy>::Split(HFunc&& hfn, Segment* dest_right, MoveCb&& on_move_cb) {
    ++local_depth_;
    dest_right->local_depth_ = local_depth_;

    auto is_mine = [this](Hash_t hash) { return (hash >> (64 - local_depth_) & 1) == 0; };

    for (unsigned i = 0; i < kBucketNum; ++i) {
        uint32_t invalid_mask = 0;

        auto cb = [&](auto* bucket, unsigned slot, bool probe) {

            (void)probe;

            auto& key = bucket->key[slot];
            Hash_t hash = hfn(key);

            if (is_mine(hash))
                return;  // keep this key in the source

            invalid_mask |= (1u << slot);
            Iterator it = dest_right->InsertUniq(std::forward<Key_t>(bucket->key[slot]),
                                                std::forward<Value_t>(bucket->value[slot]), hash, false,
                                                [](auto&&...) {});
            assert(it.found());
            on_move_cb(segment_id_, i, dest_right->segment_id_, it.index);
        };

        bucket_[i].ForEachSlot(std::move(cb));
        bucket_[i].ClearSlots(invalid_mask);
    }

    for (unsigned i = 0; i < kStashBucketNum; ++i) {
        uint32_t invalid_mask = 0;
        PhysicalBid bid = kBucketNum + i;
        Bucket& stash = bucket_[bid];

        auto cb = [&](auto* bucket, unsigned slot, bool probe) {

            (void)probe;

            auto& key = bucket->key[slot];
            Hash_t hash = hfn(key);

            if (is_mine(hash)) {
                // If the entry stays in the same segment we try to unload it back to the regular bucket.
                Iterator it = TryMoveFromStash(i, slot, hash); // 移到原段
                if (it.found()) {
                    invalid_mask |= (1u << slot);
                    on_move_cb(segment_id_, i, segment_id_, it.index);
                }

                return;
            }

            invalid_mask |= (1u << slot); // 迁移到新段
            auto it = dest_right->InsertUniq(std::forward<Key_t>(bucket->key[slot]),
                                            std::forward<Value_t>(bucket->value[slot]), hash, false,
                                            /* not interested in these movements */ [](auto&&...) {});
            (void)it;
            assert(it.index != kNanBid);
            on_move_cb(segment_id_, i, dest_right->segment_id_, it.index);

            // Remove stash reference pointing to stash bucket i.
            RemoveStashReference(i, hash); // 清除原段的 Stash 指针引用
        };

        stash.ForEachSlot(std::move(cb));
        stash.ClearSlots(invalid_mask);
    }
}

template <typename Key, typename Value, typename Policy>
template <typename Cb>
void Segment<Key, Value, Policy>::TraverseAll(Cb&& cb) const {
    for (uint8_t i = 0; i < kTotalBuckets; ++i) {
        bucket_[i].ForEachSlot([&](auto*, SlotId slot, bool) 
        { cb(Iterator{i, slot}); });
    }
}

// stash_pos is index of the stash bucket, in the range of [0, STASH_BUCKET_NUM).
template <typename Key, typename Value, typename Policy>
void Segment<Key, Value, Policy>::RemoveStashReference(unsigned stash_pos, Hash_t key_hash) {
    LogicalBid y = HomeIndex(key_hash);
    uint8_t fp_hash = key_hash & kFpMask;
    auto* target = &bucket_[y];
    auto* next = &bucket_[NextBid(y)];

    target->UnsetStashPtr(fp_hash, stash_pos, next);
}

template <typename Key, typename Value, typename Policy>
auto Segment<Key, Value, Policy>::TryMoveFromStash(unsigned stash_id, unsigned stash_slot_id,
                                                   Hash_t key_hash) -> Iterator {
    LogicalBid bid = HomeIndex(key_hash);
    uint8_t hash_fp = key_hash & kFpMask;
    PhysicalBid stash_bid = kBucketNum + stash_id;
    auto& key = Key(stash_bid, stash_slot_id);
    auto& value = Value(stash_bid, stash_slot_id);

    int reg_slot = bucket_[bid].TryInsertToBucket(std::forward<Key_t>(key),
                                                    std::forward<Value_t>(value), hash_fp, false);

    if (reg_slot < 0) {
        bid = NextBid(bid);
        reg_slot = bucket_[bid].TryInsertToBucket(std::forward<Key_t>(key),
                                                std::forward<Value_t>(value), hash_fp, true);
    }

    if (reg_slot >= 0) {
        RemoveStashReference(stash_id, key_hash);
        return Iterator{bid, SlotId(reg_slot)};
    }

    return Iterator{};
}


template <typename Key, typename Value, typename Policy>
int Segment<Key, Value, Policy>::MoveToOther(bool own_items, 
                            /*
                                true：移动自己的条目（非探测槽位）；
                                false：移动别人的条目（探测槽位）                            
                            */

                        unsigned from_bid, unsigned to_bid) { // 桶满时将一个条目从当前桶移动到另一个桶，为新条目腾出空间
    assert(from_bid < kBucketNum && to_bid < kBucketNum);
    auto& src = bucket_[from_bid];
    uint32_t mask = src.GetProbe(!own_items);
    if (mask == 0) {
        return -1;
    }

    int src_slot = __builtin_ctz(mask);
    int dst_slot = bucket_[to_bid].TryInsertToBucket(std::forward<Key_t>(src.key[src_slot]),
                                                    std::forward<Value_t>(src.value[src_slot]),
                                                    src.Fp(src_slot), own_items);
    if (dst_slot < 0)
        return -1;


    src.Delete(src_slot);

    return src_slot;
}

template <typename Key, typename Value, typename Policy>
auto Segment<Key, Value, Policy>::FindValidStartingFrom(PhysicalBid bid, unsigned slot) const
    -> Iterator {
    while (bid < kTotalBuckets) {
        uint32_t mask = bucket_[bid].GetBusy();
        mask >>= slot;
        if (mask) {
            return Iterator(bid, slot + __builtin_ctz(mask));
        }
        ++bid;
        slot = 0;
    }
    return Iterator{};
}



template <typename Key, typename Value, typename Policy>
template <typename Cb, typename HashFn>
bool Segment<Key, Value, Policy>::TraverseLogicalBucket(LogicalBid bid, HashFn&& hfun,
                                                        Cb&& cb) const {
    assert(bid < kBucketNum);

    const Bucket& b = bucket_[bid];
    bool found = false;
    if (b.GetProbe(false)) {  // Check items that this bucket owns.
        b.ForEachSlot([&](auto* bucket, SlotId slot, bool probe) {

            (void)bucket;

            if (!probe) {
                found = true;
                cb(Iterator{bid, slot});
            }
        });
    }

    uint8_t nid = NextBid(bid);
    const Bucket& next = GetBucket(nid);

    // check for probing entries in the next bucket, i.e. those that should reside in b.
    if (next.GetProbe(true)) {
        next.ForEachSlot([&](auto* bucket, SlotId slot, bool probe) {

            (void)bucket;

            if (probe) {
                found = true;
                assert(HomeIndex(hfun(bucket->key[slot])) == bid);
                cb(Iterator{nid, slot});
            }
        });
    }

    // Finally go over stash buckets and find those entries that belong to b.
    if (b.HasStash()) {
        // do not bother with overflow fps. Just go over all the stash buckets.
        for (uint8_t j = kBucketNum; j < kTotalBuckets; ++j) {
        const auto& stashb = bucket_[j];
        stashb.ForEachSlot([&](auto* bucket, SlotId slot, bool probe) {

            (void)probe;

            if (HomeIndex(hfun(bucket->key[slot])) == bid) {
                found = true;
                cb(Iterator{j, slot});
            }
        });
        }
    }

    return found;
}
































}
}
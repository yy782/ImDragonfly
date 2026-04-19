#pragma once

#include <atomic>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include "dash_internal.hpp"

namespace dfly{
inline size_t HashString(const std::string& s) {
    // 使用 xxhash 风格的简单哈希
    size_t hash = 0x811c9dc5;
    for (char c : s) {
        hash ^= c;
        hash *= 0x01000193;
    }
    return hash;
}


constexpr size_t kSlotNum = 14;           // 每个桶的槽位数
constexpr size_t kBucketNum = 64;         // 普通桶数量
constexpr size_t kStashBucketNum = 4;     // 溢出桶数量
constexpr size_t kTotalBuckets = kBucketNum + kStashBucketNum;
constexpr size_t kFingerprintBits = 8;    // 指纹位数
constexpr size_t kFingerprintMask = (1 << kFingerprintBits) - 1;


class SlotBitmap {
public:
    SlotBitmap() : busy_(0), probe_(0), size_(0) {}
    
    // 查找空闲槽位
    int FindEmptySlot() const {
        if (size_ >= kSlotNum) return -1;
        
        uint32_t mask = ~busy_;
        int slot = __builtin_ctz(mask);  // 找最低位的1
        return slot < (int)kSlotNum ? slot : -1;
    }
    
    // 设置槽位
    void SetSlot(int slot, bool probe) {
        busy_ |= (1u << slot);
        if (probe) probe_ |= (1u << slot);
        size_++;
    }
    
    // 清除槽位
    void ClearSlot(int slot) {
        busy_ &= ~(1u << slot);
        probe_ &= ~(1u << slot);
        size_--;
    }
    
    bool IsBusy(int slot) const { return (busy_ >> slot) & 1; }
    bool IsProbe(int slot) const { return (probe_ >> slot) & 1; }
    size_t Size() const { return size_; }
    bool IsFull() const { return size_ >= kSlotNum; }
    uint32_t GetBusyMask() const { return busy_; }
    uint32_t GetProbeMask() const { return probe_; }
    
    void Clear() {
        busy_ = 0;
        probe_ = 0;
        size_ = 0;
    }
    
private:
    uint32_t busy_ = 0;   // 哪些槽位被占用
    uint32_t probe_ = 0;  // 哪些是探测槽位
    uint8_t size_ = 0;    // 当前占用数
};
// Bucket (64 bytes)
// ┌─────────────────────────────────────────────────────────────┐
// │  Slot 0 │ Slot 1 │ Slot 2 │ ... │ Slot 12 │ Slot 13        │
// ├─────────┼────────┼────────┼─────┼─────────┼────────────────┤
// │ key[0]  │ key[1] │ key[2] │ ... │ key[12] │ key[13]        │
// │ value[0]│ value[1]│...                                      │
// │ fp[0]   │ fp[1]  │ ...                                     │
// └─────────────────────────────────────────────────────────────┘

template<typename _Key, typename _Value>
class Bucket {
public:
    Bucket() : fingerprints_{0} {
        std::memset(keys_, 0, sizeof(keys_));
        std::memset(values_, 0, sizeof(values_));
    }
    
    // 插入到指定槽位
    void Insert(int slot, const _Key& key, const _Value& value, uint8_t fingerprint, bool probe) {
        keys_[slot] = key;
        values_[slot] = value;
        fingerprints_[slot] = fingerprint;
        bitmap_.SetSlot(slot, probe);
    }
    
    // 查找指纹匹配的槽位
    int FindByFingerprint(uint8_t fp) const {
        uint32_t mask = bitmap_.GetBusyMask();
        for (int i = 0; i < (int)kSlotNum && mask; i++) {
            if ((mask & 1) && fingerprints_[i] == fp) {
                return i;
            }
            mask >>= 1;
        }
        return -1;
    }
    
    // 根据键查找（精确匹配）
    int FindByKey(const _Key& key) const {
        uint32_t mask = bitmap_.GetBusyMask();
        for (int i = 0; i < (int)kSlotNum && mask; i++) {
            if ((mask & 1) && keys_[i] == key) {
                return i;
            }
            mask >>= 1;
        }
        return -1;
    }
    
    // 尝试插入（返回槽位或-1）
    int TryInsert(const _Key& key, const _Value& value, uint8_t fingerprint, bool probe) {
        if (bitmap_.IsFull()) return -1;
        int slot = bitmap_.FindEmptySlot();
        if (slot >= 0) {
            Insert(slot, key, value, fingerprint, probe);
        }
        return slot;
    }
    
    // 删除槽位
    void Delete(int slot) {
        bitmap_.ClearSlot(slot);
        keys_[slot] = _Key();
        values_[slot] = _Value();
        fingerprints_[slot] = 0;
    }
    
    // 获取键值
    const _Key& GetKey(int slot) const { return keys_[slot]; }
    const _Value& GetValue(int slot) const { return values_[slot]; }
    _Value& GetValue(int slot) { return values_[slot]; }
    uint8_t GetFingerprint(int slot) const { return fingerprints_[slot]; }
    
    bool IsFull() const { return bitmap_.IsFull(); }
    bool IsEmpty() const { return bitmap_.Size() == 0; }
    size_t Size() const { return bitmap_.Size(); }
    
    void Clear() {
        bitmap_.Clear();
        for (int i = 0; i < (int)kSlotNum; i++) {
            keys_[i] = _Key();
            values_[i] = _Value();
            fingerprints_[i] = 0;
        }
    }
    
    // 遍历所有槽位
    template<typename Func>
    void ForEach(Func&& func) const {
        uint32_t mask = bitmap_.GetBusyMask();
        for (int i = 0; i < (int)kSlotNum && mask; i++) {
            if (mask & 1) {
                func(i, keys_[i], values_[i], bitmap_.IsProbe(i));
            }
            mask >>= 1;
        }
    }
    
private:
    _Key keys_[kSlotNum];
    _Value values_[kSlotNum];
    uint8_t fingerprints_[kSlotNum];
    SlotBitmap bitmap_;
};


template<typename _Key, typename _Value>
class DashTable {
public:
    DashTable(const DashTable&) = delete;
    DashTable& operator=(const DashTable&) = delete;
    using Key_t = _Key;
    using Value_t = _Value;
    template <bool IsConst, bool IsSingleBucket = false> 
    class Iterator;
    using const_iterator = Iterator<true>;
    using iterator = Iterator<false>;
    using const_bucket_iterator = Iterator<true, true>;
    using bucket_iterator = Iterator<false, true>;


    DashTable() : size_(0) {}
    
    // 插入键值对，返回是否成功（新插入或更新）
    bool Insert(const _Key& key, const _Value& value) {
        size_t hash = HashString(key);
        uint8_t fingerprint = hash & kFingerprintMask;
        size_t home_idx = (hash >> kFingerprintBits) % kBucketNum;
        size_t next_idx = NextBucket(home_idx);
        
        // 1. 先检查是否已存在（更新）
        int slot;
        if ((slot = FindInBucket(buckets_[home_idx], key)) >= 0) {
            buckets_[home_idx].GetValue(slot) = value;
            return false;  // 更新，不是新插入
        }
        if ((slot = FindInBucket(buckets_[next_idx], key)) >= 0) {
            buckets_[next_idx].GetValue(slot) = value;
            return false;
        }
        
        // 2. 尝试插入到负载较小的桶
        Bucket<_Key, _Value>& target = (buckets_[home_idx].Size() <= buckets_[next_idx].Size()) 
                                ? buckets_[home_idx] : buckets_[next_idx];
        bool is_probe = (&target == &buckets_[next_idx]);
        
        slot = target.TryInsert(key, value, fingerprint, is_probe);
        if (slot >= 0) {
            size_++;
            return true;
        }
        
        // 3. 尝试 Stash 桶
        for (size_t i = 0; i < kStashBucketNum; i++) {
            size_t stash_idx = kBucketNum + i;
            slot = buckets_[stash_idx].TryInsert(key, value, fingerprint, false);
            if (slot >= 0) {
                // 记录 Stash 指针
                buckets_[home_idx].SetStashPtr(i, fingerprint);
                size_++;
                return true;
            }
        }
        
        return false;  // 表满了
    }
    
    // 查找键，返回指针（nullptr 表示不存在）
    template <typename U>
    const_iterator Find(U&& key) const {
        return Find(std::forward<U>(key));
    }
    template <typename U>
    iterator Find(U&& key) {
        size_t hash = HashString(key);
        uint8_t fingerprint = hash & kFingerprintMask;
        size_t home_idx = (hash >> kFingerprintBits) % kBucketNum;
        size_t next_idx = NextBucket(home_idx);
        
        // 1. 检查主桶
        int slot = FindInBucket(buckets_[home_idx], key);
        if (slot >= 0) 
            return iterator(this, 0, home_idx, slot);
        
        // 2. 检查邻居桶
        slot = FindInBucket(buckets_[next_idx], key);
        if (slot >= 0) 
            return iterator(this, 0, next_idx, slot);
        
        // 3. 检查 Stash 桶
        for (size_t i = 0; i < kStashBucketNum; i++) {
            size_t stash_idx = kBucketNum + i;
            slot = FindInBucket(buckets_[stash_idx], key);
            if (slot >= 0) 
                return iterator(this, 0, next_idx, slot);
        }
        
        return iterator();
    }
    
    // 删除键
    bool Delete(const _Key& key) {
        size_t hash = HashString(key);
        uint8_t fingerprint = hash & kFingerprintMask;
        size_t home_idx = (hash >> kFingerprintBits) % kBucketNum;
        size_t next_idx = NextBucket(home_idx);
        
        // 1. 检查主桶
        int slot = FindInBucket(buckets_[home_idx], key);
        if (slot >= 0) {
            buckets_[home_idx].Delete(slot);
            size_--;
            return true;
        }
        
        // 2. 检查邻居桶
        slot = FindInBucket(buckets_[next_idx], key);
        if (slot >= 0) {
            buckets_[next_idx].Delete(slot);
            size_--;
            return true;
        }
        
        // 3. 检查 Stash 桶
        for (size_t i = 0; i < kStashBucketNum; i++) {
            size_t stash_idx = kBucketNum + i;
            slot = FindInBucket(buckets_[stash_idx], key);
            if (slot >= 0) {
                buckets_[stash_idx].Delete(slot);
                buckets_[home_idx].ClearStashPtr(i, fingerprint);
                size_--;
                return true;
            }
        }
        
        return false;
    }
    
    size_t Size() const { return size_; }
    bool Empty() const { return size_ == 0; }
    
    // 获取内存使用（估算）
    size_t MemoryUsage() const {
        return size_ * (sizeof(_Key) + sizeof(_Value) + sizeof(uint8_t)) + sizeof(*this);
    }
    
    // 清空所有数据
    void Clear() {
        for (auto& bucket : buckets_) {
            bucket.Clear();
        }
        size_ = 0;
    }
    
    // 遍历所有键值对
    template<typename Func>
    void ForEach(Func&& func) const {
        for (size_t bid = 0; bid < kTotalBuckets; bid++) {
            buckets_[bid].ForEach([&](int slot, const _Key& key, const _Value& val, bool is_probe) {
                func(key, val);
            });
        }
    }  
private:
    static size_t NextBucket(size_t idx) {
        return (idx + 1) % kBucketNum;
    }
    
    int FindInBucket(const Bucket<_Key, _Value>& bucket, const _Key& key) const {
        // 先尝试指纹快速过滤
        size_t hash = HashString(key);
        uint8_t fp = hash & kFingerprintMask;
        int slot = bucket.FindByFingerprint(fp);
        if (slot >= 0 && bucket.GetKey(slot) == key) {
            return slot;
        }
        // 指纹不匹配，直接精确查找
        return bucket.FindByKey(key);
    }
    
    Bucket<_Key, _Value> buckets_[kTotalBuckets];
    size_t size_;

};


template <typename _Key, typename _Value>
template <bool IsConst, bool IsSingleBucket>
class DashTable<_Key, _Value>::Iterator {

public:
    Iterator()=default;
    template <bool TIsConst = IsConst, bool TIsSingleB>
    requires TIsConst Iterator(const Iterator<!TIsConst, TIsSingleB>& other)
    noexcept : 
        owner_(other.owner_),
        seg_id_(other.seg_id_),
        bucket_id_(other.bucket_id_),
        slot_id_(other.slot_id_) {
  }

    // 判断迭代器是否结束
    bool is_done() const;
    
    // 检查槽位是否被占用
    bool IsOccupied() const;
    
    // 获取版本号（用于乐观并发控制）
    uint64_t GetVersion() const;
    
    // 访问 key 和 value
    const _Key& first() const;
    const _Value& second() const;
    
    // 非 const 版本（仅当 IsConst=false 时可用）
    _Key& first();
    _Value& second();
    
    // 获取底层 Segment 和 Bucket
    //Segment& GetSegment();
    uint8_t GetBucketId() const;
    uint8_t GetSlotId() const;
    
    // 获取 owner（用于重新查找）
    DashTable& owner() const;
    
private:
    friend class DashTable;
    Iterator(DashTable* me, uint32_t seg_id, detail::PhysicalBid bid, uint8_t sid) : 
    owner_(me), 
    seg_id_(seg_id), 
    bucket_id_(bid), 
    slot_id_(sid) { }    
    // 迭代器的位置信息
    DashTable* owner_;      // 所属的 DashTable
    uint32_t seg_id_;   // 当前 segment 索引
    uint8_t bucket_id_;     // 当前 bucket 索引（0-67）
    uint8_t slot_id_;       // 当前槽位索引（0-13）
    
    // 对于单桶迭代器，bucket_id_ 是固定的
    // 对于全表迭代器，bucket_id_ 会递增
};



// 判断迭代器是否结束
template<typename _Key, typename _Value>
template<bool IsConst, bool IsSingleBucket>
bool DashTable<_Key, _Value>::Iterator<IsConst, IsSingleBucket>::is_done() const {
    if (IsSingleBucket) {
        // 单桶迭代器：只需要检查是否还在当前桶内
        return bucket_id_ >= kTotalBuckets || slot_id_ >= kSlotNum;
    } else {
        // 全表迭代器：检查是否遍历完所有桶
        return bucket_id_ >= kTotalBuckets;
    }
}

// 检查槽位是否被占用
template<typename _Key, typename _Value>
template<bool IsConst, bool IsSingleBucket>
bool DashTable<_Key, _Value>::Iterator<IsConst, IsSingleBucket>::IsOccupied() const {
    if (is_done()) return false;
    return owner_->buckets_[bucket_id_].GetBusyMask() & (1u << slot_id_);
}

// 获取版本号（简化版：始终返回 0，因为未实现版本控制）
template<typename _Key, typename _Value>
template<bool IsConst, bool IsSingleBucket>
uint64_t DashTable<_Key, _Value>::Iterator<IsConst, IsSingleBucket>::GetVersion() const {
    return 0;
}

// 获取 key（const 版本）
template<typename _Key, typename _Value>
template<bool IsConst, bool IsSingleBucket>
const _Key& 
 DashTable<_Key, _Value>::Iterator<IsConst, IsSingleBucket>::first() const {
    static _Key empty_key{};
    if (is_done() || !IsOccupied()) return empty_key;
    return owner_->buckets_[bucket_id_].GetKey(slot_id_);
}

// 获取 value（const 版本）
template<typename _Key, typename _Value>
template<bool IsConst, bool IsSingleBucket>
const _Value& 
DashTable<_Key, _Value>::Iterator<IsConst, IsSingleBucket>::second() const {
    static _Value empty_value{};
    if (is_done() || !IsOccupied()) return empty_value;
    return owner_->buckets_[bucket_id_].GetValue(slot_id_);
}

// 获取 key（非 const 版本，仅当 IsConst=false）
template<typename _Key, typename _Value>
template<bool IsConst, bool IsSingleBucket>
_Key& 
DashTable<_Key, _Value>::Iterator<IsConst, IsSingleBucket>::first() {
    static _Key empty_key{};
    if (is_done() || !IsOccupied()) return empty_key;
    return owner_->buckets_[bucket_id_].GetKey(slot_id_);
}

// 获取 value（非 const 版本，仅当 IsConst=false）
template<typename _Key, typename _Value>
template<bool IsConst, bool IsSingleBucket>
_Value&
DashTable<_Key, _Value>::Iterator<IsConst, IsSingleBucket>::second() {
    static _Value empty_value{};
    if (is_done() || !IsOccupied()) return empty_value;
    return owner_->buckets_[bucket_id_].GetValue(slot_id_);
}

// 获取 owner（用于重新查找）
template<typename _Key, typename _Value>
template<bool IsConst, bool IsSingleBucket>
DashTable<_Key, _Value>& 
DashTable<_Key, _Value>::Iterator<IsConst, IsSingleBucket>::owner() const {
    return *owner_;
}

// // 获取 Segment（简化版：返回 owner 的引用，因为只有一个 segment）
// template<typename _Key, typename _Value>
// template<bool IsConst, bool IsSingleBucket>
// auto DashTable<_Key, _Value>::Iterator<IsConst, IsSingleBucket>::GetSegment() 
//     -> decltype(owner_->buckets_[0])& {
//     return owner_->buckets_[0];
// }

// 获取桶 ID
template<typename _Key, typename _Value>
template<bool IsConst, bool IsSingleBucket>
uint8_t DashTable<_Key, _Value>::Iterator<IsConst, IsSingleBucket>::GetBucketId() const {
    return bucket_id_;
}
//
// 获取槽位 ID
template<typename _Key, typename _Value>
template<bool IsConst, bool IsSingleBucket>
uint8_t DashTable<_Key, _Value>::Iterator<IsConst, IsSingleBucket>::GetSlotId() const {
    return slot_id_;
}

}














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
#include <ranges>
#include "dash_internal.hpp"

namespace dfly{



template<typename _Key, typename _Value, typename Policy>
class DashTable : public detail::DashTableBase{
public:
    DashTable(const DashTable&) = delete;
    DashTable& operator=(const DashTable&) = delete;
    using Base = detail::DashTableBase;
    using SegmentType = detail::Segment<_Key, _Value, Policy>;
    using SegmentIterator = typename SegmentType::Iterator;

    using Key_t = _Key; // not same
    using Value_t = _Value; 
    using Segment_t = SegmentType;


    template <bool IsConst, bool IsSingleBucket = false> 
    class Iterator;
    using const_iterator = Iterator<true>;
    using iterator = Iterator<false>;

    struct BucketSet;
    using const_bucket_iterator = Iterator<true, true>;
    using bucket_iterator = Iterator<false, true>;
    using Cursor = detail::DashCursor;




    struct DefaultEvictionPolicy{
        static constexpr bool can_gc = false;
        static constexpr bool can_evict = false; 
        
        bool CanGrow(const DashTable&) {
            return true;
        }

        void OnMove(Cursor, Cursor) {

        }

        void RecordSplit(SegmentType*) {

        }        

    };

    explicit DashTable(size_t capacity_log = 1, const Policy& policy = Policy{},
            PMR_NS::memory_resource* mr = PMR_NS::get_default_resource());
    ~DashTable();
    template <typename U, typename V> 
    std::pair<iterator, bool> 
    Insert(U&& key, V&& value) {
        DefaultEvictionPolicy policy;
        return InsertInternal(std::forward<U>(key), std::forward<V>(value), policy,
                            InsertMode::kInsertIfNotFound);
    } 
    
    template <typename U, typename V> 
    iterator InsertNew(U&& key, V&& value){
        DefaultEvictionPolicy policy;
        return InsertNew(std::forward<U>(key), std::forward<V>(value), policy);        
    }    

    template <typename U, typename V, typename EvictionPolicy>
    iterator InsertNew(U&& key, V&& value, EvictionPolicy&& ev){
        return InsertInternal(std::forward<U>(key), std::forward<V>(value), std::forward<EvictionPolicy>(ev),
                            InsertMode::kForceInsert).first;
    }



    template <typename U> 
    const_iterator Find(U&& key) const;
    template <typename U> 
    iterator Find(U&& key){ 
        return FindFirst(DoHash(key), EqPred(key)); 
    }
  
    template <typename Pred> 
    iterator FindFirst(uint64_t key_hash, Pred&& pred);    


    void Erase(iterator it);
    size_t Erase(const Key_t& k);
    
    iterator begin() {
        iterator it{this, 0, 0, 0};
        it.Seek2Occupied(); // 将迭代器向前移动到下一个“被占用”的槽位。
        return it;
    }

    const_iterator cbegin() const {
        const_iterator it{this, 0, 0, 0};
        it.Seek2Occupied();
        return it;
    }

    iterator end() const {
        return iterator{};
    }
    const_iterator cend() const {
        return const_iterator{};
    }    

    using Base::depth;
    using Base::Empty;
    using Base::size;
    using Base::unique_segments;

    void Clear();
    
    size_t NextSeg(size_t sid) const {
        size_t delta = (1u << (global_depth_ - segment_[sid]->local_depth()));
        return sid + delta;
    }  
    
    template <typename U> 
    uint64_t DoHash(U&& k) const {   // not same
        return policy_.HashFn(std::forward<U>(k));                       
    } 
    
    template <typename Cb>
    auto Traverse(Cursor curs, Cb&& cb) -> Cursor;    


private:
    enum class InsertMode {
        kInsertIfNotFound,
        kForceInsert,
    };    


    template <typename U, typename V, typename EvictionPolicy>
    std::pair<iterator, bool> 
    InsertInternal(U&& key, V&& value, EvictionPolicy& policy,
                                            InsertMode mode);

    SegmentType* ConstructSegment(uint8_t depth, uint32_t id);  




    template <typename K> 
    auto EqPred(const K& key) const {
        return [p = &policy_, &key](const auto& probe) -> bool { return p->Equal(probe, key); };
    }

    void IncreaseDepth(unsigned new_depth);

    template <typename EvictionPolicy> 
    void Split(uint32_t seg_id, EvictionPolicy& ev);


    template <typename Cb> 
    void IterateDistinct(Cb&& cb);


    Policy policy_;
    std::vector<SegmentType*, PMR_NS::polymorphic_allocator<SegmentType*>> segment_;

};


template <typename _Key, typename _Value, typename Policy>
template <bool IsConst, bool IsSingleBucket>
class DashTable<_Key, _Value, Policy>::Iterator {
    using Owner = std::conditional_t<IsConst, const DashTable, DashTable>;
public:

    using iterator_category = std::forward_iterator_tag; // 前向迭代器
    using difference_type = std::ptrdiff_t;
    using IteratorPairType =
        std::conditional_t<IsConst, 
                            detail::IteratorPair<const Key_t, const Value_t>,
                            detail::IteratorPair<Key_t, Value_t>>;
    template <bool TIsConst = IsConst, bool TIsSingleB>
    requires TIsConst Iterator(const Iterator<!TIsConst, TIsSingleB>& other)
    noexcept : 
        owner_(other.owner_),
        seg_id_(other.seg_id_),
        bucket_id_(other.bucket_id_),
        slot_id_(other.slot_id_) {}
    template <bool TIsSingle>
    Iterator(const Iterator<IsConst, TIsSingle>& other) 
    noexcept : 
        owner_(other.owner_),
        seg_id_(other.seg_id_),
        bucket_id_(other.bucket_id_),
        slot_id_(IsSingleBucket ? 0 : other.slot_id_)
        {

            if constexpr (IsSingleBucket) {
                Seek2Occupied();
            }
        }
    Iterator()=default; // not same
    Iterator(const Iterator& other) = default;
    Iterator(Iterator&& other) = default;
    Iterator& operator=(const Iterator& other) = default;
    Iterator& operator=(Iterator&& other) = default;   
    
    Iterator& operator++() {
        ++slot_id_;
        Seek2Occupied();
        return *this;
    }

    Iterator& operator+=(int delta) {
        slot_id_ += delta;
        Seek2Occupied();
        return *this;
    }    
    IteratorPairType operator->() const {
        auto* seg = owner_->segment_[seg_id_];
        return {seg->Key(bucket_id_, slot_id_), seg->Value(bucket_id_, slot_id_)};
    }
    bool is_done() const {
        return owner_ == nullptr;
    }

    bool IsOccupied() const {
        return (seg_id_ < owner_->segment_.size()) &&
            ((owner_->segment_[seg_id_]->IsBusy(bucket_id_, slot_id_)));
    }

    Owner* owner() const {
        return owner_;
    }

    friend bool operator==(const Iterator& lhs, const Iterator& rhs) {
        if (lhs.owner_ == nullptr && rhs.owner_ == nullptr)
            return true;
        return lhs.owner_ == rhs.owner_ && lhs.seg_id_ == rhs.seg_id_ &&
            lhs.bucket_id_ == rhs.bucket_id_ && lhs.slot_id_ == rhs.slot_id_;
    }

    friend bool operator!=(const Iterator& lhs, const Iterator& rhs) {
        return !(lhs == rhs);
    }
private:
    friend class DashTable;
    Iterator(Owner* me, uint32_t seg_id, detail::PhysicalBid bid, uint8_t sid) : 
    owner_(me), 
    seg_id_(seg_id), 
    bucket_id_(bid), 
    slot_id_(sid) { } 

    void Seek2Occupied();

    // 迭代器的位置信息
    Owner* owner_;      // 所属的 DashTable
    uint32_t seg_id_;   // 当前 segment 索引
    detail::PhysicalBid bucket_id_;     // 当前 bucket 索引（0-67）
    uint8_t slot_id_;       // 当前槽位索引（0-13）
    
    // 对于单桶迭代器，bucket_id_ 是固定的
    // 对于全表迭代器，bucket_id_ 会递增
};

template <typename _Key, typename _Value, typename Policy>
struct DashTable<_Key, _Value, Policy>::BucketSet { 
    auto buckets() const {
        bool is_all = limit_ > ids_.size(); // 判断是连续范围还是离散列表
        return std::views::iota(0u, limit_) | //   生成 0..limit_-1 的整数序列 
                std::views::transform([*this, is_all](uint8_t i) {
                uint8_t index = is_all ? i : ids_[i];// 根据模式选择桶 ID
                return bucket_iterator{owner_, seg_id_, index}; // 生成指向该桶的迭代器
            });
    }

    bool operator==(const BucketSet& other) const {
        return owner_ == other.owner_ && seg_id_ == other.seg_id_ && limit_ == other.limit_ &&
            ids_[0] == other.ids_[0] && ids_[1] == other.ids_[1];
    }

private:
    friend class DashTable;

    BucketSet(DashTable* owner, uint32_t seg_id, uint8_t limit, uint8_t ids[2])
        : owner_{owner}, seg_id_{seg_id}, limit_{limit}, ids_{ids[0], ids[1]} {
    }

    DashTable* owner_;
    uint32_t seg_id_;
    uint8_t limit_; // 桶数量限制
    std::array<uint8_t, 2> ids_;
};


template <typename _Key, typename _Value, typename Policy>
DashTable<_Key, _Value, Policy>::DashTable(size_t capacity_log, const Policy& policy,
                                           PMR_NS::memory_resource* mr)
    : Base(capacity_log), policy_(policy), segment_(mr) {
    segment_.resize(unique_segments_);
    for (uint32_t i = 0; i < segment_.size(); ++i) {
        segment_[i] = ConstructSegment(global_depth_, i);  
    }
}
template <typename _Key, typename _Value, typename Policy>
DashTable<_Key, _Value, Policy>::~DashTable() {
    Clear();
    auto* resource = segment_.get_allocator().resource();
    PMR_NS::polymorphic_allocator<SegmentType> pa(resource);
    using alloc_traits = std::allocator_traits<decltype(pa)>;

    IterateDistinct([&](SegmentType* seg) {
        alloc_traits::destroy(pa, seg);
        alloc_traits::deallocate(pa, seg, 1);
        return false;
    });
}



template <typename _Key, typename _Value, typename Policy>
template <typename U>
auto DashTable<_Key, _Value, Policy>::Find(U&& key) const -> const_iterator {
    uint64_t key_hash = DoHash(key);
    uint32_t seg_id = SegmentId(key_hash); 
    if (auto seg_it = segment_[seg_id]->FindIt(key_hash, EqPred(key)); seg_it.found()) {
        return {this, seg_id, seg_it.index, seg_it.slot};
    }
    return {};
}

template <typename _Key, typename _Value, typename Policy>
template <typename Pred>
auto DashTable<_Key, _Value, Policy>::FindFirst(uint64_t key_hash, Pred&& pred) -> iterator {
    uint32_t seg_id = SegmentId(key_hash);
    if (auto seg_it = segment_[seg_id]->FindIt(key_hash, pred); seg_it.found()) {
        return {this, seg_id, seg_it.index, seg_it.slot};
    }
    return {};
}







template <typename _Key, typename _Value, typename Policy>
typename DashTable<_Key, _Value, Policy>::SegmentType* 
DashTable<_Key, _Value, Policy>::ConstructSegment(uint8_t depth, uint32_t id) {
    auto* mr = segment_.get_allocator().resource();
    PMR_NS::polymorphic_allocator<SegmentType> pa(mr);
    SegmentType* res = pa.allocate(1);
    pa.construct(res, depth, id, mr);  
    bucket_count_ += res->num_buckets();
    return res;
}



template <typename _Key, typename _Value, typename Policy>
template <typename Cb>
void DashTable<_Key, _Value, Policy>::IterateDistinct(Cb&& cb) {
    size_t i = 0;
    while (i < segment_.size()) {
        auto* seg = segment_[i];
        size_t next_id = NextSeg(i);
        if (cb(seg))
            break;
        i = next_id;
    }
}

template <typename _Key, typename _Value, typename Policy>
void DashTable<_Key, _Value, Policy>::Clear() {
    auto cb = [this](SegmentType* seg) {
        seg->TraverseAll([this, seg](const SegmentIterator& it) {
            policy_.DestroyKey(seg->Key(it.index, it.slot));
            policy_.DestroyValue(seg->Value(it.index, it.slot));
        });
        seg->Clear();
        return false;
    };

    IterateDistinct(cb);
    size_ = 0;
    if (global_depth_ > initial_depth_) {
        PMR_NS::polymorphic_allocator<SegmentType> pa(segment_.get_allocator());
        using alloc_traits = std::allocator_traits<decltype(pa)>;

        size_t dest = 0, src = 0;
        size_t new_size = (1 << initial_depth_);
        bucket_count_ = 0;
        while (src < segment_.size()) {
        auto* seg = segment_[src];
        size_t next_src = NextSeg(src);  
        if (dest < new_size) {
            seg->set_local_depth(initial_depth_);
            bucket_count_ += seg->num_buckets();
            segment_[dest++] = seg;
        } else {
            alloc_traits::destroy(pa, seg);
            alloc_traits::deallocate(pa, seg, 1);
        }

        src = next_src;
        }

        global_depth_ = initial_depth_;
        unique_segments_ = new_size;
        segment_.resize(new_size);
    }
}

template <typename _Key, typename _Value, typename Policy>
template <typename U, typename V, typename EvictionPolicy>
auto DashTable<_Key, _Value, Policy>::InsertInternal(U&& key, V&& value, EvictionPolicy& ev,
                                                     InsertMode mode) -> std::pair<iterator, bool> {
    uint64_t key_hash = DoHash(key);
    uint32_t target_seg_id = SegmentId(key_hash); // 使用哈希值的高 global_depth_ 位确定目标段

    while (true) {
        assert(target_seg_id < segment_.size());
        SegmentType* target = segment_[target_seg_id];
        __builtin_prefetch(target, 0, 1); // 预取指令 , 预热， !!!!!!!!!!!!!!!!!!!

        typename SegmentType::Iterator it;
        bool res = true;
        unsigned num_buckets = target->num_buckets();

        auto move_cb = [&](uint32_t segment_id, detail::PhysicalBid from, detail::PhysicalBid to) {
            ev.OnMove(Cursor{global_depth_, segment_id, from}, Cursor{global_depth_, segment_id, to});
        };

        if (mode == InsertMode::kForceInsert) {
            it = target->InsertUniq(std::forward<U>(key), std::forward<V>(value), key_hash, true, move_cb); 
            res = it.found();  
        } else {
            std::tie(it, res) = target->Insert(std::forward<U>(key), std::forward<V>(value), key_hash,
                                            EqPred(key), move_cb); 
        }

        if (res) {  // success
            bucket_count_ += (target->num_buckets() - num_buckets);
            ++size_;
            return std::make_pair(iterator{this, target_seg_id, it.index, it.slot}, true);
        }

        if (it.found()) {
            return std::make_pair(iterator{this, target_seg_id, it.index, it.slot}, false);
        }
        if (target->local_depth() == global_depth_) { 
            IncreaseDepth(global_depth_ + 1);

            target_seg_id = SegmentId(key_hash);
            assert(target_seg_id < segment_.size() && segment_[target_seg_id] == target);
        }

        ev.RecordSplit(target);
        Split(target_seg_id, ev);
    }

    return std::make_pair(iterator{}, false);
}


template <typename _Key, typename _Value, typename Policy>
template <typename EvictionPolicy>
void DashTable<_Key, _Value, Policy>::Split(uint32_t seg_id, EvictionPolicy& ev) {
    SegmentType* source = segment_[seg_id];

    uint32_t chunk_size = 1u << (global_depth_ - source->local_depth()); // 该段覆盖的目录项数量
    uint32_t start_idx = seg_id & (~(chunk_size - 1)); // 覆盖范围的起始索引
    assert(segment_[start_idx] == source && segment_[start_idx + chunk_size - 1] == source);
    uint32_t target_id = start_idx + chunk_size / 2; // 新段在目录中的起始位置
    SegmentType* target = ConstructSegment(source->local_depth() + 1, target_id);

    auto hash_fn = [this](const auto& k) { return policy_.HashFn(k); };

    // bucket_count_ 缺少了两次操作

    source->Split(
        std::move(hash_fn), target,
        [&](uint32_t segment_from, detail::PhysicalBid from, uint32_t segment_to,
            detail::PhysicalBid to) {

            ev.OnMove(Cursor{global_depth_, segment_from, from}, Cursor{global_depth_, segment_to, to});
        });
    ++unique_segments_;

    for (size_t i = target_id; i < start_idx + chunk_size; ++i) {
        segment_[i] = target;
    }
}

template <typename _Key, typename _Value, typename Policy>
void DashTable<_Key, _Value, Policy>::IncreaseDepth(unsigned new_depth) {
    assert(!segment_.empty());
    assert(new_depth > global_depth_);
    size_t prev_sz = segment_.size();
    size_t repl_cnt = 1ul << (new_depth - global_depth_);
    segment_.resize(1ul << new_depth);

    for (int i = prev_sz - 1; i >= 0; --i) {
        size_t offs = i * repl_cnt;
        std::fill(segment_.begin() + offs, segment_.begin() + offs + repl_cnt, segment_[i]);
        segment_[i]->set_segment_id(offs);  // update segment id.
    }
    global_depth_ = new_depth;
}



template <typename _Key, typename _Value, typename Policy>
size_t DashTable<_Key, _Value, Policy>::Erase(const Key_t& key) {
    uint64_t key_hash = DoHash(key);
    size_t x = SegmentId(key_hash);
    auto* target = segment_[x];
    auto it = target->FindIt(key_hash, EqPred(key));
    if (!it.found())
        return 0;

    policy_.DestroyKey(target->Key(it.index, it.slot));
    policy_.DestroyValue(target->Value(it.index, it.slot));
    target->Delete(it, key_hash);
    --size_;

    return 1;
}

template <typename _Key, typename _Value, typename Policy>
void DashTable<_Key, _Value, Policy>::Erase(iterator it) {
    auto* target = segment_[it.seg_id_];
    uint64_t key_hash = DoHash(it->first);
    SegmentIterator sit{it.bucket_id_, it.slot_id_};

    policy_.DestroyKey(it->first);
    policy_.DestroyValue(it->second);

    target->Delete(sit, key_hash);
    --size_;
}



template <typename _Key, typename _Value, typename Policy>
template <bool IsConst, bool IsSingleBucket>
void DashTable<_Key, _Value, Policy>::Iterator<IsConst, IsSingleBucket>::Seek2Occupied() {
    if (owner_ == nullptr)
        return;
    assert(seg_id_ < owner_->segment_.size());

    if constexpr (IsSingleBucket) {
        const auto& b = owner_->segment_[seg_id_]->GetBucket(bucket_id_);
        uint32_t mask = b.GetBusy() >> slot_id_;
        if (mask) {
            int slot = __builtin_ctz(mask);
            slot_id_ += slot;
            return;
        }
    } else {
        while (seg_id_ < owner_->segment_.size()) {
            auto seg_it = owner_->segment_[seg_id_]->FindValidStartingFrom(bucket_id_, slot_id_);
            if (seg_it.found()) {
                bucket_id_ = seg_it.index;
                slot_id_ = seg_it.slot;
                return;
            }
            seg_id_ = owner_->NextSeg(seg_id_);
            bucket_id_ = slot_id_ = 0;
        }
    }
    owner_ = nullptr;
}



template <typename _Key, typename _Value, typename Policy>
template <typename Cb>
auto DashTable<_Key, _Value, Policy>::Traverse(Cursor curs, Cb&& cb) -> Cursor {
    uint32_t sid = curs.segment_id(global_depth_);
    uint8_t bid = curs.bucket_id();

    if (bid >= Policy::kBucketNum || sid >= segment_.size())
        return Cursor::end();

    auto hash_fun = [this](const auto& k) { return policy_.HashFn(k); };

    bool fetched = false;
    do {
        SegmentType* s = segment_[sid];
        assert(s);

        auto dt_cb = [&](const SegmentIterator& it) { cb(iterator{this, sid, it.index, it.slot}); };

        fetched = s->TraverseLogicalBucket(bid, hash_fun, std::move(dt_cb));
        sid = NextSeg(sid);
        if (sid >= segment_.size()) {
        sid = 0;
        ++bid;

        if (bid >= Policy::kBucketNum)
            return Cursor::end();
        }
    } while (!fetched);

    return Cursor{global_depth_, sid, bid};
}

















}





















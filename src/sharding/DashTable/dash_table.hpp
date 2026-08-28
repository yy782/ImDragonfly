#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "dash_internal.hpp"
#include "util/mi_memory_resource.hpp"

namespace dfly {
namespace dash {

// ---------------------------------------------------------------------------
// 持久化协议常量
// ---------------------------------------------------------------------------
// 文件布局（崩溃一致性：数据先落盘，提交点最后发布）：
//   [Header]   magic/version/布局参数/entry_count
//   [Data]     entry_count 个 (key, value) 记录
//   [Commit]   提交点：entry_count + checksum（最后写入）
// 恢复时若 Commit 缺失或不匹配，说明快照不完整，判定恢复失败。
static constexpr char kPersistMagic[4] = {'I', 'M', 'D', 'T'};
static constexpr uint16_t kPersistVersion = 1;

// 持久化头：魔数 + 版本 + 布局参数（槽数/桶数）+ 条目数。
// 反序列化时先核对布局参数，保证读进的内存布局与当前编译的模板一致。
struct PersistHeader {
  char magic[4];
  uint16_t version;
  uint16_t slots;
  uint16_t main_buckets;
  uint16_t stash_buckets;
  uint64_t entry_count;
};

// 持久化尾：条目数 + 校验和，用于校验数据完整性。
struct PersistCommit {
  uint64_t entry_count;
  uint64_t checksum;
};

/*
 关于持久化，会遇到一个问题:
 数据不一致:
 比如对于MSET命令涉及到多分片，分片1，分片2，，
 对于分片1，持久化前完成了对MSET在分片1的操作,对于分片2，持久化后才完成了对MSET在分片2的操作，就出现了数据不一致的问题
 解决方案:
 设置全局事务进行调度就可以在MSET命令前面或者后面完成持久化，就可以解决这个问题了

 这就产生了新的问题，如果数据库的数据过多，会严重阻塞线程，导致持久化命令(SAVE)后面的命令严重阻塞
 解决这个问题，需要每次都持久化一部分数据，而不是全部，持续调度，即可，
 但是怎么做到持久化一部分数据呢

 对于DashTable的段而言，段内桶的数据会左右移动的，比如SAVE了桶1的数据，然后继续调度，在执行第二个SAVE前，
 桶2的数据向桶1腾挪了(看Segment::TryInsert),这时，第二次SAVE就会漏了桶2的一部分数据，
 以段为单位进行持久化，主要的问题是扩展哈希目录，在执行第二次SAVE时，物理段数已经改变了，
 已经扫描过的物理段的数据一部分迁移到了新物理段，新物理段是不能扫描的，但是对于执行第二次SAVE时没扫描的物理段呢
 就需要扫描这个没扫描的物理段，以及这个物理段扩展出来的新段，但是这不是最好的办法，会产生反复扫描新段的可能吗



*/

template <typename Key, typename Value, typename Policy>
class DashTable {
  static_assert(Policy::kSlotNum > 0 && Policy::kSlotNum <= kMaxSlotsPerBucket,
                "kSlotNum must be in (0, 16]");
  static_assert(Policy::kBucketNum > 0, "kBucketNum must be > 0");

 public:
  using key_type = Key;
  using value_type = Value;
  using Hash_t = uint64_t;
  using Cursor = uint64_t;

  static constexpr unsigned kSlots = Policy::kSlotNum;
  static constexpr unsigned kMainBuckets = Policy::kBucketNum;
  static constexpr unsigned kStashBuckets = StashBucketNum<Policy>::value;

  using Segment = dfly::dash::Segment<Key, Value, Policy, kSlots, kMainBuckets,
                                      kStashBuckets>;

  template <bool IsConst>
  class IteratorT {
    template <typename, typename, typename>
    friend class DashTable;

   public:
    struct EntryRef {
      const Key& first;
      std::conditional_t<IsConst, const Value&, Value&> second;
      const EntryRef* operator->() const { return this; }
    };

    using OwnerPtr = std::conditional_t<IsConst, const DashTable*, DashTable*>;

    IteratorT() = default;

    bool is_done() const { return done_; }
    explicit operator bool() const { return !done_; }

    OwnerPtr owner() const { return owner_; }

    bool IsOccupied() const {
      return !done_ && seg_ != nullptr &&
             seg_->BucketAt(bid_).IsOccupied(slot_);
    }

    EntryRef operator*() const { return operator->(); }
    EntryRef operator->() const {
      return {seg_->BucketAt(bid_).KeyAt(slot_),
              seg_->BucketAt(bid_).ValAt(slot_)};
    }

    IteratorT& operator++() {
      Advance();
      return *this;
    }
    IteratorT operator++(int) {
      IteratorT tmp = *this;
      Advance();
      return tmp;
    }

    bool operator==(const IteratorT& o) const { return done_ == o.done_; }
    bool operator!=(const IteratorT& o) const { return !(*this == o); }

   private:
    IteratorT(OwnerPtr owner, Segment* seg, uint32_t seg_idx, uint32_t bid,
              uint32_t slot)
        : owner_(owner),
          seg_(seg),
          seg_idx_(seg_idx),
          bid_(bid),
          slot_(slot),
          done_(false) {}

    void Advance() {
      if (done_) return;
      uint32_t bid = bid_;
      uint32_t s = slot_ + 1;
      for (;;) {
        while (bid < Segment::kTotalBuckets) {
          const auto& b = seg_->BucketAt(bid);
          uint16_t mask = b.occupied_ & kSlotMask<kSlots>;
          if (s != 0) mask &= static_cast<uint16_t>(~((1u << s) - 1));
          if (mask != 0) {
            bid_ = bid;
            slot_ = static_cast<uint32_t>(std::countr_zero(mask));
            return;
          }
          ++bid;
          s = 0;
        }
        const uint32_t gd = owner_->GlobalDepth();
        const uint32_t ld = seg_->LocalDepth();
        const uint32_t span = 1u << (gd - ld);
        const uint32_t next = seg_idx_ + span;
        if (next >= owner_->SegmentCount()) {
          done_ = true;
          return;
        }
        seg_ = owner_->segments_[next];
        seg_idx_ = next;
        bid = 0;
        s = 0;
      }
    }

    using SegPtr = std::conditional_t<IsConst, const Segment*, Segment*>;
    OwnerPtr owner_ = nullptr;
    SegPtr seg_ = nullptr;
    uint32_t seg_idx_ = 0;
    uint32_t bid_ = 0;
    uint32_t slot_ = 0;
    bool done_ = true;
  };

  using iterator = IteratorT<false>;
  using const_iterator = IteratorT<true>;

  // DashTable 的创建
  //
  // 术语:
  //   逻辑段: segments_ 的每个元素是一个逻辑段，它是一个指针，指向物理段；
  //           不同逻辑段可能指向同一个物理段
  //   物理段: 真正存储数据的段，即 dash_internal.hpp 的 Segment
  //
  // 构造: global_depth_ = init_segment_log，unique_segments_ =
  // 2^init_segment_log， 表示逻辑段数量。key 需要 global_depth_ 位哈希在目录
  // segments_ 找对应的逻辑段。 例子: init_segment_log = 2
  //   global_depth_ = 2, unique_segments_ = 4
  //   key 前 (global_depth_ = 2) 位为 XX（X 为 0 或 1）:
  //     00 -> 逻辑段1
  //     01 -> 逻辑段2
  //     10 -> 逻辑段3
  //     11 -> 逻辑段4
  //   目录是 2^global_depth_ 项，无论 XX 是什么都能找到对应逻辑段
  explicit DashTable(uint32_t init_segment_log, const Policy& policy = Policy{},
                     std::pmr::memory_resource* mr = nullptr)
      : policy_(policy),
        mr_(mr ? mr : std::pmr::get_default_resource()),
        global_depth_(init_segment_log),
        initial_depth_(init_segment_log),
        unique_segments_(0),
        size_(0) {
    const uint32_t count = 1u << init_segment_log;
    AllocDirectory(count);
    for (uint32_t i = 0; i < count; ++i) {
      segments_[i] = AllocSegment();
      segments_[i]->SetLocalDepth(init_segment_log);
      ++unique_segments_;
    }
  }

  ~DashTable() { DestroyAll(); }

  DashTable(const DashTable&) = delete;
  DashTable& operator=(const DashTable&) = delete;

  DashTable(DashTable&& o) noexcept
      : policy_(o.policy_),
        mr_(o.mr_),
        segments_(std::move(o.segments_)),
        global_depth_(o.global_depth_),
        initial_depth_(o.initial_depth_),
        unique_segments_(o.unique_segments_),
        size_(o.size_) {
    o.global_depth_ = 0;
    o.unique_segments_ = 0;
    o.size_ = 0;
  }

  DashTable& operator=(DashTable&& o) noexcept {
    if (this != &o) {
      DestroyAll();
      policy_ = o.policy_;
      mr_ = o.mr_;
      segments_ = std::move(o.segments_);
      global_depth_ = o.global_depth_;
      initial_depth_ = o.initial_depth_;
      unique_segments_ = o.unique_segments_;
      size_ = o.size_;
      o.global_depth_ = 0;
      o.unique_segments_ = 0;
      o.size_ = 0;
    }
    return *this;
  }

  template <typename K>
  iterator Find(const K& key) {
    return FindByHash(policy_.HashFn(key), key);
  }
  template <typename K>
  const_iterator Find(const K& key) const {
    return FindByHash(policy_.HashFn(key), key);
  }

  template <typename K>
  iterator FindByHash(Hash_t hash, const K& key) {
    Segment* seg = segments_[SegmentId(hash)];
    if (auto pos = seg->FindIn(hash, key)) {
      return iterator(this, seg, BaseOf(SegmentId(hash)), pos->bid, pos->slot);
    }
    return iterator{};
  }
  template <typename K>
  const_iterator FindByHash(Hash_t hash, const K& key) const {
    Segment* seg = segments_[SegmentId(hash)];
    if (auto pos = seg->FindIn(hash, key)) {
      return const_iterator(this, seg, BaseOf(SegmentId(hash)), pos->bid,
                            pos->slot);
    }
    return const_iterator{};
  }

  template <typename K>
  bool Contains(const K& key) const {
    return !FindByHash(policy_.HashFn(key), key).is_done();
  }

  size_t Size() const { return size_; }
  bool Empty() const { return size_ == 0; }

  uint32_t GlobalDepth() const { return global_depth_; }
  uint32_t SegmentCount() const { return 1u << global_depth_; }
  uint32_t UniqueSegmentCount() const { return unique_segments_; }

  size_t Capacity() const {
    return static_cast<size_t>(unique_segments_) * kMainBuckets * kSlots;
  }

  double LoadFactor() const {
    const size_t cap = Capacity();
    return cap == 0 ? 0.0 : static_cast<double>(size_) / cap;
  }

  size_t MemoryUsage() const {
    const size_t seg_bytes = sizeof(Segment);
    return static_cast<size_t>(unique_segments_) * seg_bytes +
           segments_.size() * sizeof(Segment*);
  }

  template <typename K, typename V>
  std::pair<iterator, bool> Insert(K&& key, V&& value) {
    const Hash_t hash = policy_.HashFn(key);
    iterator it = FindByHash(hash, key);
    if (!it.is_done()) return {it, false};
    return {InsertNewByHash(hash, std::forward<K>(key), std::forward<V>(value)),
            true};
  }

  template <typename K, typename V>
  iterator InsertNew(K&& key, V&& value) {
    const Hash_t hash = policy_.HashFn(key);
    return InsertNewByHash(hash, std::forward<K>(key), std::forward<V>(value));
  }

  template <typename K, typename V>
  std::pair<iterator, bool> InsertOrUpdate(K&& key, V&& value) {
    const Hash_t hash = policy_.HashFn(key);
    iterator it = FindByHash(hash, key);
    if (!it.is_done()) {
      it->second = std::forward<V>(value);
      return {it, false};
    }
    return {InsertNewByHash(hash, std::forward<K>(key), std::forward<V>(value)),
            true};
  }

  template <typename K>
  bool Erase(const K& key) {
    const Hash_t hash = policy_.HashFn(key);
    Segment* seg = segments_[SegmentId(hash)];
    if (auto pos = seg->FindIn(hash, key)) {
      seg->EraseAt(pos->bid, pos->slot);
      assert(size_ > 0);
      --size_;
      return true;
    }
    return false;
  }

  bool Erase(iterator it) {
    if (it.is_done()) return false;
    Segment* seg = it.seg_;
    seg->EraseAt(it.bid_, it.slot_);
    assert(size_ > 0);
    --size_;
    return true;
  }

  bool Erase(const_iterator it) {
    if (it.is_done()) return false;
    Segment* seg = const_cast<Segment*>(it.seg_);
    seg->EraseAt(it.bid_, it.slot_);
    assert(size_ > 0);
    --size_;
    return true;
  }

  template <typename F>
  Cursor Traverse(Cursor cursor, F&& cb) {
    return TraverseImpl(cursor, std::forward<F>(cb));
  }
  template <typename F>
  Cursor Traverse(Cursor cursor, F&& cb) const {
    return TraverseImpl(cursor, std::forward<F>(cb));
  }

  template <typename F>
  void ForEach(F&& cb) {
    Cursor c = 0;
    do {
      c = TraverseImpl(c, cb);
    } while (c != 0);
  }
  template <typename F>
  void ForEach(F&& cb) const {
    Cursor c = 0;
    do {
      c = TraverseImpl(c, cb);
    } while (c != 0);
  }

  // ---- 恢复策略（持久化 / 恢复） ----
  //
  // Sink 接口：bool write(const void* data, size_t len);
  // Source 接口：bool read(void* data, size_t len);
  // 键/值字节格式由 Policy::WriteKey/WriteValue/ReadKey/ReadValue 决定。
  //
  // 崩溃一致性协议：
  //   1. 先写 Header（布局参数 + 条目数）；
  //   2. 再写全部 (key, value) 数据；
  //   3. 最后写 Commit（条目数 + 校验和）——提交点。
  // 恢复时若 Commit 缺失或校验失败，返回 nullopt（快照不完整）。
  template <typename Sink>
  bool Serialize(Sink& sink) const {
    PersistHeader h;
    std::memcpy(h.magic, kPersistMagic, sizeof(h.magic));
    h.version = kPersistVersion;
    h.slots = kSlots;
    h.main_buckets = kMainBuckets;
    h.stash_buckets = kStashBuckets;
    h.entry_count = size_;
    if (!sink.write(&h, sizeof(h))) return false;

    uint64_t checksum = 0;
    bool ok = true;
    ForEach([&](const_iterator it) {
      if (!ok) return;
      const Key& k = it->first;
      const Value& v = it->second;
      checksum ^= policy_.HashFn(k) + 0x9e3779b97f4a7c15ull + (checksum << 6) +
                  (checksum >> 2);  // 混合哈希
      if (!Policy::WriteKey(sink, k) || !Policy::WriteValue(sink, v)) {
        ok = false;
      }
    });

    PersistCommit c;
    c.entry_count = size_;
    c.checksum = checksum;
    if (!ok || !sink.write(&c, sizeof(c))) return false;
    return true;
  }

  // 从快照恢复。失败（损坏/不完整/布局不匹配）返回 nullopt。
  template <typename Source>
  static std::optional<DashTable> Deserialize(Source& src, const Policy& policy,
                                              std::pmr::memory_resource* mr) {
    PersistHeader h;
    if (!src.read(&h, sizeof(h))) return std::nullopt;
    if (std::memcmp(h.magic, kPersistMagic, sizeof(h.magic)) != 0 ||
        h.version != kPersistVersion || h.slots != kSlots ||
        h.main_buckets != kMainBuckets || h.stash_buckets != kStashBuckets) {
      return std::nullopt;
    }

    // 按条目数估算初始段数，减少恢复过程中的分裂。
    uint32_t init_log = 0;
    const uint64_t cap_per_seg = static_cast<uint64_t>(kMainBuckets) * kSlots;
    while ((1ull << init_log) * cap_per_seg < h.entry_count) ++init_log;
    if (init_log > 24) init_log = 24;

    DashTable table(init_log, policy, mr);
    uint64_t checksum = 0;
    for (uint64_t i = 0; i < h.entry_count; ++i) {
      Key key;
      Value value;
      if (!Policy::ReadKey(src, &key) || !Policy::ReadValue(src, &value)) {
        return std::nullopt;
      }
      checksum ^= policy.HashFn(key) + 0x9e3779b97f4a7c15ull + (checksum << 6) +
                  (checksum >> 2);
      table.InsertNewByHash(policy.HashFn(key), std::move(key),
                            std::move(value));
    }

    PersistCommit c;
    if (!src.read(&c, sizeof(c))) return std::nullopt;
    if (c.entry_count != h.entry_count || c.checksum != checksum) {
      return std::nullopt;
    }
    return std::optional<DashTable>(std::move(table));
  }

 private:
  uint32_t SegmentId(Hash_t hash) const {
    return static_cast<uint32_t>(hash >> (64 - global_depth_));
  }

  uint32_t BaseOf(uint32_t sid) const {
    const Segment* seg = segments_[sid];
    const uint32_t span = 1u << (global_depth_ - seg->LocalDepth());
    return sid & ~(span - 1);
  }

  void AllocDirectory(uint32_t count) { segments_.resize(count); }

  Segment* AllocSegment() {
    void* p = mr_->allocate(sizeof(Segment), alignof(Segment));
    return new (p) Segment();
  }

  void FreeSegment(Segment* seg) {
    seg->~Segment();
    mr_->deallocate(seg, sizeof(Segment), alignof(Segment));
  }

  void DestroySegment(Segment* seg) {
    for (uint32_t bid = 0; bid < Segment::kTotalBuckets; ++bid) {
      auto& b = seg->BucketAt(bid);
      uint16_t mask = b.occupied_ & kSlotMask<kSlots>;
      while (mask != 0) {
        const unsigned s = static_cast<unsigned>(std::countr_zero(mask));
        b.template Remove<Policy>(s);
        mask &= mask - 1;
      }
    }
  }

  void DestroyAll() {
    if (segments_.empty()) return;
    for (uint32_t i = 0; i < SegmentCount();) {
      Segment* seg = segments_[i];
      const uint32_t ld = seg->LocalDepth();
      DestroySegment(seg);
      FreeSegment(seg);
      i += 1u << (global_depth_ - ld);
    }
  }

  // 原始目录:
  //   00 -> 逻辑段1 -> 物理段1
  //   01 -> 逻辑段2 -> 物理段2
  //   10 -> 逻辑段3 -> 物理段3
  //   11 -> 逻辑段4 -> 物理段4
  // global_depth_ = 2
  // IncreaseDepth: global_depth_ += 1，目录翻倍成 8 项，
  // 每个逻辑段复制两份，即每 2 个逻辑段指向同一个物理段:
  //   000 -> 逻辑段1 -> 物理段1
  //   001 -> 逻辑段2 -> 物理段1
  //   010 -> 逻辑段3 -> 物理段2
  //   011 -> 逻辑段4 -> 物理段2
  //   100 -> 逻辑段5 -> 物理段3
  //   101 -> 逻辑段6 -> 物理段3
  //   110 -> 逻辑段7 -> 物理段4
  //   111 -> 逻辑段8 -> 物理段4
  // 倒序填充，正序会错误覆盖
  void IncreaseDepth() {
    const uint32_t prev = segments_.size();
    segments_.resize(prev * 2);
    for (int i = prev - 1; i >= 0; --i) {
      const size_t offs = 2 * static_cast<size_t>(i);
      std::fill(segments_.begin() + offs, segments_.begin() + offs + 2,
                segments_[i]);
    }
    ++global_depth_;
  }

  Segment* SplitSegment(uint32_t sid, Hash_t hash) {
    Segment* seg = segments_[sid];
    const uint32_t base_old = BaseOf(sid);
    const bool need_grow = seg->LocalDepth() == global_depth_;
    if (need_grow) {  // 判断是否有多余的逻辑段可以给seg扩容
      /*
            情况1: local_depth=2 < global_depth=3
      目录有8项，段占2项，分裂后可以指向新段

      索引:  0   1   2   3   4   5   6   7
            ┌───┬───┬───┬───┬───┬───┬───┬───┐
            │ A │ A │ A │ A │ B │ B │ B │ B │
            └───┴───┴───┴───┴───┴───┴───┴───┘
                  ↑
                段A占2项，分裂后有一半可以给新段
                ✓ 不需要扩展目录


      情况2: local_depth=3 = global_depth=3
      段独占1项，分裂后没地方放新段

      索引:  0   1   2   3   4   5   6   7
            ┌───┬───┬───┬───┬───┬───┬───┬───┐
            │ A │ B │ C │ D │ E │ F │ G │ H │
            └───┴───┴───┴───┴───┴───┴───┴───┘
                  ↑
                段A只占1项，分裂后需要2项
                ✗ 必须扩展目录
       */
      IncreaseDepth();
    }
    const uint32_t base = need_grow ? (base_old << 1) : base_old;
    Segment* new_seg = AllocSegment();
    ++unique_segments_;
    seg->SplitInto(new_seg, [&](const Key& k) { return policy_.HashFn(k); });

    const uint32_t span = 1u << (global_depth_ - seg->LocalDepth());
    for (uint32_t i = base + span; i < base + 2 * span; ++i) {
      segments_[i] = new_seg;
    }
    return ((hash >> (64 - seg->LocalDepth())) & 1u) ? new_seg : seg;
  }

  /* 逻辑段与物理段的映射关系
   *
   * KEY 找逻辑段需要 global_depth_ 位哈希值，key_hash = hash >> (64 -
   * global_depth_); 对应物理段就是 key_hash 的高 local_depth_
   * 位。选高位不选低位， 是因为同一物理段的逻辑段高位相同，分裂好处理（见
   * SplitSegment）。
   *
   * 例子: global_depth_ = 3，各物理段 local_depth_ 都是 1，则物理段只看
   * hash >> (64 - global_depth_) 的最高 1 位（0 或 1），只有 2 个物理段；
   * 若 local_depth_ 都是 2，则看最高 2 位，有 4 个物理段。
   * 目录:
   *   000 -> 逻辑段1 -> 物理段1
   *   001 -> 逻辑段2 -> 物理段1
   *   010 -> 逻辑段3 -> 物理段1
   *   011 -> 逻辑段4 -> 物理段1
   *   100 -> 逻辑段5 -> 物理段2
   *   101 -> 逻辑段6 -> 物理段2
   *   110 -> 逻辑段7 -> 物理段2
   *   111 -> 逻辑段8 -> 物理段2
   *
   * 物理段1 满了分裂：local_depth_ 1 -> 2，新段物理段3 接管后半区，
   * 和物理段1 用第 2 位区分（0 归物理段1，1 归物理段3）:
   *   000 -> 逻辑段1 -> 物理段1
   *   001 -> 逻辑段2 -> 物理段1
   *   010 -> 逻辑段3 -> 物理段3
   *   011 -> 逻辑段4 -> 物理段3
   *   100 -> 逻辑段5 -> 物理段2
   *   101 -> 逻辑段6 -> 物理段2
   *   110 -> 逻辑段7 -> 物理段2
   *   111 -> 逻辑段8 -> 物理段2
   *
   * 若物理段 local_depth_ == global_depth_（只独占 1 个逻辑段，没空位给新段），
   * 先 IncreaseDepth 翻倍目录再分裂。
   */
  template <typename K, typename V>
  iterator InsertNewByHash(Hash_t hash, K&& key, V&& value) {
    uint32_t sid = SegmentId(hash);
    for (;;) {
      Segment* seg = segments_[sid];
      if (auto pos =
              seg->TryInsert(std::forward<K>(key), std::forward<V>(value), hash,
                             /*spread=*/true)) {
        ++size_;
        return iterator(this, seg, BaseOf(sid), pos->bid, pos->slot);
      }
      seg = SplitSegment(sid, hash);
      sid = SegmentId(hash);
    }
  }

  template <typename F>
  Cursor TraverseImpl(Cursor cursor, F&& cb) {
    return TraverseDispatch<false>(cursor, std::forward<F>(cb));
  }
  template <typename F>
  Cursor TraverseImpl(Cursor cursor, F&& cb) const {
    return TraverseDispatch<true>(cursor, std::forward<F>(cb));
  }

  template <bool IsConst, typename F>
  Cursor TraverseDispatch(Cursor cursor, F&& cb) const {
    if (size_ == 0) return 0;
    uint32_t seg_idx = static_cast<uint32_t>(cursor >> 8);
    uint32_t bid = cursor & 0xFFu;
    if (seg_idx >= SegmentCount()) return 0;

    using It = std::conditional_t<IsConst, const_iterator, iterator>;
    using SelfT = std::conditional_t<IsConst, const DashTable*, DashTable*>;
    SelfT self = const_cast<DashTable*>(this);

    for (;;) {
      Segment* seg = segments_[seg_idx];
      const uint32_t base = BaseOf(seg_idx);
      if (seg_idx != base) {
        seg_idx = base;
        bid = 0;
        continue;
      }
      while (bid < Segment::kTotalBuckets) {
        const auto& b = seg->BucketAt(bid);
        uint16_t mask = b.occupied_ & kSlotMask<kSlots>;
        while (mask != 0) {
          const uint32_t s = static_cast<uint32_t>(std::countr_zero(mask));
          cb(It(self, seg, seg_idx, bid, s));
          mask &= mask - 1;
        }
        ++bid;
      }
      const uint32_t span = 1u << (global_depth_ - seg->LocalDepth());
      seg_idx += span;
      if (seg_idx >= SegmentCount()) return 0;
      return (static_cast<Cursor>(seg_idx) << 8) | 0u;
    }
  }

  Policy policy_;
  std::pmr::memory_resource* mr_;
  std::vector<Segment*> segments_;
  uint32_t global_depth_ = 0;
  uint32_t initial_depth_ = 0;
  uint32_t unique_segments_ = 0;
  size_t size_ = 0;
};

}  // namespace dash
}  // namespace dfly

#pragma once

// ============================================================================
// dash_table.hpp — Dash 哈希表：目录（Extendible Hashing）与对外接口
//
// 依据公开论文《Dash: Scalable Hashing on Persistent Memory》
// (Lu et al., VLDB 2020) 从头实现，无 DragonflyDB 代码派生关系。
//
// 本文件负责：
//   * 目录层：global_depth 驱动的指针目录，Segment 按可扩展哈希方式分裂；
//   * 迭代器 / 游标遍历（SCAN 语义，允许遍历期间表结构变化）；
//   * 对外接口：Find / Insert / Erase / Size / MemoryUsage 等；
//   * 恢复策略：Serialize / Deserialize（先持久化数据、后发布提交点，
//     崩溃后可安全恢复，恢复时间与数据量呈线性关系且无需日志重放）。
//
// Policy 契约（模板参数）：
//   enum : uint8_t { kSlotNum, kBucketNum };        // 布局参数
//   static uint64_t HashFn(const Key&);             // 键哈希
//   static uint64_t HashFn(std::string_view);       // 字符串查找（可选）
//   static bool Equal(const Key&, const LookupKey&); // 键比较（LookupKey 可为
//                                                     // Key 或 string_view）
//   static void DestroyKey(Key&);                   // 键销毁通知
//   static void DestroyValue(Value&);               // 值销毁通知
//   // 恢复策略所需（序列化契约）：
//   template <typename S> static bool WriteKey(S&, const Key&);
//   template <typename S> static bool WriteValue(S&, const Value&);
//   template <typename S> static bool ReadKey(S&, Key*);
//   template <typename S> static bool ReadValue(S&, Value*);
// ============================================================================

#include "dash_internal.hpp"
#include "detail/memory_resource.hpp"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <optional>
#include <type_traits>
#include <utility>

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

struct PersistHeader {
  char magic[4];
  uint16_t version;
  uint16_t slots;
  uint16_t main_buckets;
  uint16_t stash_buckets;
  uint64_t entry_count;
};

struct PersistCommit {
  uint64_t entry_count;
  uint64_t checksum;
};

// ---------------------------------------------------------------------------
// DashTable
// ---------------------------------------------------------------------------
template <typename Key, typename Value, typename Policy>
class DashTable {
  static_assert(Policy::kSlotNum > 0 && Policy::kSlotNum <= kMaxSlotsPerBucket,
                "kSlotNum must be in (0, 16]");
  static_assert(Policy::kBucketNum > 0, "kBucketNum must be > 0");

 public:
  using key_type = Key;
  using value_type = Value;
  using Hash_t = uint64_t;
  using Cursor = uint64_t;  // 游标：高 56 位 = 逻辑段索引，低 8 位 = 桶号

  static constexpr unsigned kSlots = Policy::kSlotNum;
  static constexpr unsigned kMainBuckets = Policy::kBucketNum;
  static constexpr unsigned kStashBuckets = StashBucketNum<Policy>::value;

  using Segment = dfly::dash::Segment<Key, Value, Policy, kSlots, kMainBuckets,
                                      kStashBuckets>;

  // ---- 迭代器 ----
  template <bool IsConst>
  class IteratorT {
    template <typename, typename, typename>
    friend class DashTable;

   public:
    // 解引用代理：支持 it->first / it->second / *it。
    struct EntryRef {
      const Key& first;
      std::conditional_t<IsConst, const Value&, Value&> second;
      const EntryRef* operator->() const { return this; }
    };

    using OwnerPtr = std::conditional_t<IsConst, const DashTable*, DashTable*>;

    IteratorT() = default;

    bool is_done() const { return done_; }
    explicit operator bool() const { return !done_; }

    // 迭代器所属的表（供上层延迟清洁 / 跨表操作）。
    OwnerPtr owner() const { return owner_; }

    // 当前指向的槽是否仍被占用（分裂 / 移动 / 删除后可能已空）。
    bool IsOccupied() const {
      return !done_ && seg_ != nullptr &&
             seg_->BucketAt(bid_).IsOccupied(slot_);
    }

    EntryRef operator*() const { return operator->(); }
    EntryRef operator->() const {
      return {seg_->KeyAt(slot_), seg_->ValAt(slot_)};
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

    // 仅用于"是否都为空迭代器"的比较。
    bool operator==(const IteratorT& o) const { return done_ == o.done_; }
    bool operator!=(const IteratorT& o) const { return !(*this == o); }

   private:
    IteratorT(OwnerPtr owner, Segment* seg, uint32_t seg_idx, uint32_t bid,
              uint32_t slot)
        : owner_(owner), seg_(seg), seg_idx_(seg_idx), bid_(bid), slot_(slot),
          done_(false) {}

    void Advance() {
      if (done_) return;
      uint32_t bid = bid_;
      uint32_t s = slot_ + 1;
      for (;;) {
        // 段内：从当前桶当前槽之后找下一个占用槽。
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
        // 跨段：跳到下一个不同物理段（跳过共享同一物理段的逻辑槽）。
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
    uint32_t seg_idx_ = 0;  // 当前物理段在目录中的基地址
    uint32_t bid_ = 0;
    uint32_t slot_ = 0;
    bool done_ = true;
  };

  using iterator = IteratorT<false>;
  using const_iterator = IteratorT<true>;

  // ---- 构造 / 析构 / 移动 ----

  // init_segment_log：初始目录深度（段数 = 2^init_segment_log）。
  explicit DashTable(uint32_t init_segment_log, const Policy& policy = Policy{},
                     PMR_NS::memory_resource* mr = nullptr)
      : policy_(policy),
        mr_(mr ? mr : PMR_NS::get_default_resource()),
        global_depth_(init_segment_log),
        initial_depth_(init_segment_log),
        unique_segments_(0),
        size_(0) {
    const uint32_t count = 1u << init_segment_log;
    AllocDirectory(count);
    for (uint32_t i = 0; i < count; ++i) {
      segments_[i] = AllocSegment();
      ++unique_segments_;
    }
  }

  ~DashTable() {
    DestroyAll();
    FreeDirectory();
  }

  DashTable(const DashTable&) = delete;
  DashTable& operator=(const DashTable&) = delete;

  DashTable(DashTable&& o) noexcept
      : policy_(o.policy_),
        mr_(o.mr_),
        segments_(o.segments_),
        dir_cap_(o.dir_cap_),
        global_depth_(o.global_depth_),
        initial_depth_(o.initial_depth_),
        unique_segments_(o.unique_segments_),
        size_(o.size_) {
    o.segments_ = nullptr;
    o.dir_cap_ = 0;
    o.global_depth_ = 0;
    o.unique_segments_ = 0;
    o.size_ = 0;
  }

  DashTable& operator=(DashTable&& o) noexcept {
    if (this != &o) {
      DestroyAll();
      FreeDirectory();
      policy_ = o.policy_;
      mr_ = o.mr_;
      segments_ = o.segments_;
      dir_cap_ = o.dir_cap_;
      global_depth_ = o.global_depth_;
      initial_depth_ = o.initial_depth_;
      unique_segments_ = o.unique_segments_;
      size_ = o.size_;
      o.segments_ = nullptr;
      o.dir_cap_ = 0;
      o.global_depth_ = 0;
      o.unique_segments_ = 0;
      o.size_ = 0;
    }
    return *this;
  }

  // ---- 查询 ----

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
    if (size_ == 0) return iterator{};
    Segment* seg = segments_[SegmentId(hash)];
    if (auto pos = seg->FindIn(hash, key)) {
      return iterator(this, seg, BaseOf(SegmentId(hash)), pos->bid, pos->slot);
    }
    return iterator{};
  }
  template <typename K>
  const_iterator FindByHash(Hash_t hash, const K& key) const {
    if (size_ == 0) return const_iterator{};
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

  // 段容量（槽位总数）：负载因子 = Size / (UniqueSegmentCount * 段容量)。
  size_t Capacity() const {
    return static_cast<size_t>(unique_segments_) * kMainBuckets * kSlots;
  }

  double LoadFactor() const {
    const size_t cap = Capacity();
    return cap == 0 ? 0.0 : static_cast<double>(size_) / cap;
  }

  // 内存占用：目录 + 全部物理段（不含 Key/Value 堆外部分）。
  size_t MemoryUsage() const {
    const size_t seg_bytes = sizeof(Segment);
    return static_cast<size_t>(unique_segments_) * seg_bytes +
           static_cast<size_t>(dir_cap_) * sizeof(Segment*);
  }

  // ---- 插入 ----

  // 若键已存在返回既有迭代器（false）；否则插入并返回新迭代器（true）。
  template <typename K, typename V>
  std::pair<iterator, bool> Insert(K&& key, V&& value) {
    const Hash_t hash = policy_.HashFn(key);
    iterator it = FindByHash(hash, key);
    if (!it.is_done()) return {it, false};
    return {InsertNewByHash(hash, std::forward<K>(key),
                            std::forward<V>(value)),
            true};
  }

  // 插入，前提是键不存在（调用方须自行保证，否则产生重复键）。
  template <typename K, typename V>
  iterator InsertNew(K&& key, V&& value) {
    const Hash_t hash = policy_.HashFn(key);
    return InsertNewByHash(hash, std::forward<K>(key),
                           std::forward<V>(value));
  }

  // 插入或更新：返回 (迭代器, 是否新建)。
  template <typename K, typename V>
  std::pair<iterator, bool> InsertOrUpdate(K&& key, V&& value) {
    const Hash_t hash = policy_.HashFn(key);
    iterator it = FindByHash(hash, key);
    if (!it.is_done()) {
      it->second = std::forward<V>(value);
      return {it, false};
    }
    return {InsertNewByHash(hash, std::forward<K>(key),
                            std::forward<V>(value)),
            true};
  }

  // ---- 删除 ----

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

  bool Erase(const_iterator it) {
    if (it.is_done()) return false;
    // 删除需要可变访问；const_iterator 仅表示调用方视角的只读视图。
    Segment* seg = const_cast<Segment*>(it.seg_);
    seg->EraseAt(it.bid_, it.slot_);
    assert(size_ > 0);
    --size_;
    return true;
  }

  // ---- 遍历 ----

  // SCAN 语义游标遍历：从 cursor 继续，每轮完整扫描一个物理段并返回
  // 下一物理段的游标；全部扫描完成后返回 0。
  // 遍历期间若发生分裂，游标可能跳过或重复个别条目（与 Redis SCAN 一致）。
  template <typename F>
  Cursor Traverse(Cursor cursor, F&& cb) {
    return TraverseImpl(cursor, std::forward<F>(cb));
  }
  template <typename F>
  Cursor Traverse(Cursor cursor, F&& cb) const {
    return TraverseImpl(cursor, std::forward<F>(cb));
  }

  // 整表遍历（等价于从 0 游标循环至完成）。
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
      checksum ^= policy_.HashFn(k) + 0x9e3779b97f4a7c15ull +
                  (checksum << 6) + (checksum >> 2);  // 混合哈希
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
  static std::optional<DashTable> Deserialize(Source& src,
                                              const Policy& policy,
                                              PMR_NS::memory_resource* mr) {
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
    if (init_log > 24) init_log = 24;  // 防御：条目数异常时限制初始规模

    DashTable table(init_log, policy, mr);
    uint64_t checksum = 0;
    for (uint64_t i = 0; i < h.entry_count; ++i) {
      Key key;
      Value value;
      if (!Policy::ReadKey(src, &key) || !Policy::ReadValue(src, &value)) {
        return std::nullopt;
      }
      checksum ^= policy.HashFn(key) + 0x9e3779b97f4a7c15ull +
                  (checksum << 6) + (checksum >> 2);
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
  // ---- 目录 / 段管理 ----

  uint32_t SegmentId(Hash_t hash) const {
    return global_depth_ != 0
               ? static_cast<uint32_t>(hash >> (64 - global_depth_))
               : 0;
  }

  // 逻辑段索引 → 其物理段在目录中的基地址（连续共享区间的首索引）。
  uint32_t BaseOf(uint32_t sid) const {
    const Segment* seg = segments_[sid];
    const uint32_t span = 1u << (global_depth_ - seg->LocalDepth());
    return sid & ~(span - 1);
  }

  void AllocDirectory(uint32_t count) {
    void* p = mr_->allocate(count * sizeof(Segment*), alignof(Segment*));
    segments_ = static_cast<Segment**>(p);
    dir_cap_ = count;
  }

  void FreeDirectory() {
    if (segments_ != nullptr) {
      mr_->deallocate(segments_, dir_cap_ * sizeof(Segment*),
                      alignof(Segment*));
      segments_ = nullptr;
      dir_cap_ = 0;
    }
  }

  Segment* AllocSegment() {
    void* p = mr_->allocate(sizeof(Segment), alignof(Segment));
    return new (p) Segment();
  }

  void FreeSegment(Segment* seg) {
    seg->~Segment();
    mr_->deallocate(seg, sizeof(Segment), alignof(Segment));
  }

  // 销毁段内全部条目（键值析构）。
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
    if (segments_ == nullptr) return;
    for (uint32_t i = 0; i < SegmentCount();) {
      Segment* seg = segments_[i];
      const uint32_t ld = seg->LocalDepth();
      DestroySegment(seg);
      FreeSegment(seg);
      // 跳到下一个不同物理段（跳过共享区间）；先取深度再释放。
      i += 1u << (global_depth_ - ld);
    }
  }

  // 目录翻倍：每个旧槽复制为两个连续槽，均指向原物理段。
  void IncreaseDepth() {
    const uint32_t prev = dir_cap_;
    const uint32_t next = prev * 2;
    void* np = mr_->allocate(next * sizeof(Segment*), alignof(Segment*));
    Segment** ndir = static_cast<Segment**>(np);
    for (uint32_t i = 0; i < prev; ++i) {
      ndir[2 * i] = segments_[i];
      ndir[2 * i + 1] = segments_[i];
    }
    mr_->deallocate(segments_, prev * sizeof(Segment*), alignof(Segment*));
    segments_ = ndir;
    dir_cap_ = next;
    ++global_depth_;
  }

  // 分裂 sid 对应的物理段；返回 hash 所属的新段。
  Segment* SplitSegment(uint32_t sid, Hash_t hash) {
    Segment* seg = segments_[sid];
    if (seg->LocalDepth() == global_depth_) {
      IncreaseDepth();
    }
    // 用分裂前的局部深度计算基地址（span_old 对齐）。
    const uint32_t base = BaseOf(sid);
    Segment* new_seg = AllocSegment();
    ++unique_segments_;
    seg->SplitInto(new_seg,
                   [&](const Key& k) { return policy_.HashFn(k); });

    // 分裂后局部深度 +1、区间减半：原段保留 [base, base+span)，
    // 新段接管 [base+span, base+2*span)。
    const uint32_t span = 1u << (global_depth_ - seg->LocalDepth());
    for (uint32_t i = base + span; i < base + 2 * span; ++i) {
      segments_[i] = new_seg;
    }
    return ((hash >> (64 - seg->LocalDepth())) & 1u) ? new_seg : seg;
  }

  // 核心插入路径：不断尝试，段满则分裂后重试。
  template <typename K, typename V>
  iterator InsertNewByHash(Hash_t hash, K&& key, V&& value) {
    uint32_t sid = SegmentId(hash);
    for (;;) {
      Segment* seg = segments_[sid];
      if (auto pos = seg->TryInsert(std::forward<K>(key),
                                    std::forward<V>(value), hash,
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

  // 遍历核心：IsConst 决定回调迭代器类型（iterator / const_iterator）。
  // 遍历按"物理段"推进：每轮完整扫描一个物理段并返回下一物理段的
  // 游标；全部扫描完成后返回 0。
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
      // 归一化到物理段基地址（游标可能落在共享区间中间）。
      const uint32_t base = BaseOf(seg_idx);
      if (seg_idx != base) {
        seg_idx = base;
        bid = 0;
        continue;
      }
      // 扫描本段全部桶。
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
      // 跳到下一个物理段（跳过共享区间）。
      const uint32_t span = 1u << (global_depth_ - seg->LocalDepth());
      seg_idx += span;
      if (seg_idx >= SegmentCount()) return 0;
      return (static_cast<Cursor>(seg_idx) << 8) | 0u;
    }
  }

  Policy policy_;
  PMR_NS::memory_resource* mr_;
  Segment** segments_ = nullptr;
  uint32_t dir_cap_ = 0;       // 目录槽数 = 2^global_depth
  uint32_t global_depth_ = 0;  // 目录深度
  uint32_t initial_depth_ = 0;  // 初始深度（扩容下限）
  uint32_t unique_segments_ = 0;
  size_t size_ = 0;
};

}  // namespace dash
}  // namespace dfly

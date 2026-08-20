#pragma once

// ============================================================================
// dash_internal.hpp — Dash 哈希表内部核心（Segment / Bucket / 槽位管理）
//
// 本文件根据公开论文《Dash: Scalable Hashing on Persistent Memory》
// (Lu, Hao, Wang, Lo. VLDB 2020, arXiv:2003.07302) 的设计思想从头实现，
// 与 DragonflyDB 的任何实现代码无派生关系。遵循的公开设计：
//
//   1. 分段结构：Segment 是分裂/扩容的基本单位，段内再划分桶；
//      两级哈希：第一级经目录定位 Segment，第二级定位段内主桶。
//   2. 指纹过滤：每个槽位保存 8 位指纹（hash 低 8 位），查找时先以
//      SIMD 指令整段比对指纹，只有指纹命中的槽位才做完整键比较，
//      负查询无需触碰键值区 cacheline。
//   3. 段内溢出：主桶满时优先在邻居桶及段尾溢出区安置，不跨段；
//      仅当段内彻底无法容纳时才触发分裂。
//   4. 分裂：以 Segment 为粒度，按 Extendible Hashing 的目录增长方式
//      动态扩容，分裂后更新目录映射即可完成容量翻倍。
//
// 本文件的原创性体现在具体布局与操作（论文未规定的实现细节）：
//   * 槽状态使用原生 uint16_t 位图（占用位图 + 探测位图），不使用
//     压缩位图 / 非对齐访问等技巧；
//   * 溢出区条目通过独立的归属表 overflow_home_[] 记录其主桶号，
//     而非在桶内内嵌指针；删除与查找均为 O(1) 定位；
//   * "探测槽"标记表达"该条目归属前一主桶"，保证任意键的查找路径
//     固定且短：主桶 → 邻居桶 → 溢出区，长度与表规模无关；
//   * 插入采用"均衡安置 → 腾挪 → 段尾溢出"三级策略，兼顾负载因子
//     与查找确定性。
// ============================================================================

#include <immintrin.h>

#include <bit>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <optional>
#include <type_traits>
#include <utility>

namespace dfly {
namespace dash {

// ---------------------------------------------------------------------------
// 常量
// ---------------------------------------------------------------------------

// 指纹宽度：8 位（1 字节），指纹 = hash & 0xFF。
static constexpr unsigned kFpBits = 8;
static constexpr uint8_t kFpMask = 0xFFu;

// 每桶槽位数上限：指纹数组须能整载入一个 __m128i（16 字节）。
static constexpr unsigned kMaxSlotsPerBucket = 16;

// 每个主桶在段尾溢出区最多占用的槽位数。溢出区总量有限，
// 该上限防止个别主桶长期独占，保证最坏查找开销有界。
static constexpr unsigned kMaxOverflowPerBucket = 4;

// 溢出槽"归属主桶"的缺省值（槽未占用）。
static constexpr uint8_t kNoHome = 0xFFu;

static_assert(kMaxOverflowPerBucket <= kMaxSlotsPerBucket);

// ---------------------------------------------------------------------------
// 策略 Traits
// ---------------------------------------------------------------------------

// Policy 可通过 kStashNum 指定段尾溢出桶数；缺省为 4。
template <typename Policy, typename = void>
struct StashBucketNum {
  static constexpr unsigned value = 4;
};
template <typename Policy>
struct StashBucketNum<Policy, std::void_t<decltype(Policy::kStashNum)>> {
  static constexpr unsigned value = Policy::kStashNum;
};

// ---------------------------------------------------------------------------
// 槽位掩码
// ---------------------------------------------------------------------------

template <unsigned kSlots>
inline constexpr uint16_t kSlotMask = static_cast<uint16_t>((1u << kSlots) - 1);

// ---------------------------------------------------------------------------
// Bucket：段内的一个桶
// ---------------------------------------------------------------------------
// 元数据区（指纹 + 两张状态位图）与键值区分开排布，整个桶按 cacheline
// 对齐；负查询只需触碰元数据 cacheline，无需加载键值。
template <typename Key, typename Value, unsigned kSlots>
struct alignas(64) Bucket {
  static_assert(kSlots > 0 && kSlots <= kMaxSlotsPerBucket,
                "kSlots must be in (0, 16]");

  // ---- 元数据区 ----
  uint8_t fp_[kMaxSlotsPerBucket];  // 指纹数组；仅前 kSlots 个有效
  uint16_t occupied_;               // bit i == 1 ⇔ 槽 i 被占用
  uint16_t probe_;                  // bit i == 1 ⇔ 槽 i 为探测槽（归属前一主桶）

  // ---- 键值区：kSlots 个键值对，就地构造 ----
  Key keys_[kSlots];
  Value vals_[kSlots];

  Bucket() : occupied_(0), probe_(0) { std::memset(fp_, 0, sizeof(fp_)); }

  Bucket(const Bucket&) = delete;
  Bucket& operator=(const Bucket&) = delete;

  // ---- 元数据访问 ----
  bool IsOccupied(unsigned i) const { return (occupied_ >> i) & 1u; }
  bool IsProbe(unsigned i) const { return (probe_ >> i) & 1u; }
  uint8_t Fp(unsigned i) const { return fp_[i]; }

  Key& KeyAt(unsigned i) { return keys_[i]; }
  const Key& KeyAt(unsigned i) const { return keys_[i]; }
  Value& ValAt(unsigned i) { return vals_[i]; }
  const Value& ValAt(unsigned i) const { return vals_[i]; }

  // 空闲槽位掩码。
  uint16_t FreeMask() const { return (~occupied_) & kSlotMask<kSlots>; }
  bool HasFreeSlot() const { return FreeMask() != 0; }

  // 第一个空闲槽；调用方须先确认存在。
  unsigned FirstFree() const {
    return static_cast<unsigned>(std::countr_zero(FreeMask()));
  }

  // 指纹 SIMD 比对：一次性比对全部槽位，返回指纹命中的槽位掩码。
  uint16_t MatchFp(uint8_t fp) const {
    const __m128i key = _mm_set1_epi8(static_cast<char>(fp));
    const __m128i data = _mm_loadu_si128(reinterpret_cast<const __m128i*>(fp_));
    const __m128i eq = _mm_cmpeq_epi8(data, key);
    const uint16_t m = static_cast<uint16_t>(_mm_movemask_epi8(eq));
    return m & kSlotMask<kSlots>;
  }

  // ---- 槽位操作 ----

  // 就地构造键值并登记状态。
  template <typename K, typename V>
  void Install(unsigned i, uint8_t fp, bool probe, K&& key, V&& value) {
    fp_[i] = fp;
    occupied_ |= static_cast<uint16_t>(1u << i);
    if (probe) probe_ |= static_cast<uint16_t>(1u << i);
    new (&keys_[i]) Key(std::forward<K>(key));
    new (&vals_[i]) Value(std::forward<V>(value));
  }

  // 析构键值（先通知策略回调，再析构）并清空槽位状态。
  template <typename Policy>
  void Remove(unsigned i) {
    Policy::DestroyKey(keys_[i]);
    Policy::DestroyValue(vals_[i]);
    if constexpr (!std::is_trivially_destructible_v<Key>) keys_[i].~Key();
    if constexpr (!std::is_trivially_destructible_v<Value>) vals_[i].~Value();
    occupied_ &= static_cast<uint16_t>(~(1u << i));
    probe_ &= static_cast<uint16_t>(~(1u << i));
  }
};

// ---------------------------------------------------------------------------
// SegPos：段内位置（不依赖表结构，供上层迭代器/游标使用）
// ---------------------------------------------------------------------------

struct SegPos {
  uint32_t bid;   // 桶号：[0, kTotalBuckets)
  uint32_t slot;  // 槽号：[0, kSlots)
};

// ---------------------------------------------------------------------------
// Segment：一个物理段
// ---------------------------------------------------------------------------
// 主桶索引 [0, kMainBuckets)，溢出桶索引 [kMainBuckets, kTotalBuckets)。
// 段是分裂/扩容的最小单位，也是恢复协议中的持久化校验单位。
template <typename Key, typename Value, typename Policy, unsigned kSlots,
          unsigned kMainBuckets, unsigned kStashBuckets>
struct Segment {
  static_assert(kMainBuckets > 0 && kStashBuckets > 0);
  static constexpr unsigned kTotalBuckets = kMainBuckets + kStashBuckets;
  static constexpr unsigned kOverflowSlots = kStashBuckets * kSlots;

  // ---- 布局 ----
  Bucket<Key, Value, kSlots> buckets_[kTotalBuckets];

  // 溢出槽 → 归属主桶。全局溢出槽编号 gs = stb * kSlots + slot。
  // 槽未占用时值为 kNoHome。
  uint8_t overflow_home_[kOverflowSlots];
  // 各主桶在溢出区占用的槽数；查找时计数为 0 可跳过溢出区扫描。
  uint8_t overflow_cnt_[kMainBuckets];

  uint8_t local_depth_ = 0;  // 局部深度（Extendible Hashing）
  uint32_t size_ = 0;        // 段内条目总数

  Segment() {
    std::memset(overflow_home_, kNoHome, sizeof(overflow_home_));
    std::memset(overflow_cnt_, 0, sizeof(overflow_cnt_));
  }

  Segment(const Segment&) = delete;
  Segment& operator=(const Segment&) = delete;

  // ---- 静态辅助 ----

  // 第二级哈希：取指纹位之后的位，按主桶数取模。
  static uint32_t HomeBucket(uint64_t hash) {
    return static_cast<uint32_t>((hash >> kFpBits) % kMainBuckets);
  }

  static uint32_t NextBucket(uint32_t b) {
    return b + 1 < kMainBuckets ? b + 1 : 0;
  }

  static uint32_t PrevBucket(uint32_t b) {
    return b == 0 ? kMainBuckets - 1 : b - 1;
  }

  static uint8_t Fingerprint(uint64_t hash) {
    return static_cast<uint8_t>(hash & kFpMask);
  }

  // ---- 成员访问 ----

  Bucket<Key, Value, kSlots>& BucketAt(uint32_t bid) { return buckets_[bid]; }
  const Bucket<Key, Value, kSlots>& BucketAt(uint32_t bid) const {
    return buckets_[bid];
  }

  bool IsOverflowBid(uint32_t bid) const { return bid >= kMainBuckets; }

  uint8_t LocalDepth() const { return local_depth_; }
  uint32_t Size() const { return size_; }

  // 溢出槽全局编号 <-> 桶内坐标。
  static uint32_t OverflowBidOf(unsigned gs) {
    return kMainBuckets + gs / kSlots;
  }
  static unsigned OverflowSlotOf(unsigned gs) { return gs % kSlots; }
  static unsigned OverflowGlobal(uint32_t bid, unsigned slot) {
    return (bid - kMainBuckets) * kSlots + slot;
  }

  // ---- 查找 ----

  // 在指定桶中按"指纹 + 探测属性"过滤，逐槽精确比较。
  // 返回命中的槽号，未命中返回 -1。
  template <typename K>
  int FindInBucket(const Bucket<Key, Value, kSlots>& b, uint8_t fp,
                   bool probe, const K& key) const {
    uint16_t mask = b.MatchFp(fp) & b.occupied_ & kSlotMask<kSlots>;
    mask &= probe ? b.probe_ : static_cast<uint16_t>(~b.probe_);
    while (mask != 0) {
      const unsigned i = static_cast<unsigned>(std::countr_zero(mask));
      if (Policy::Equal(b.KeyAt(i), key)) {
        return static_cast<int>(i);
      }
      mask &= mask - 1;  // 清除最低位
    }
    return -1;
  }

  // 溢出区扫描：仅检查归属主桶为 home 的溢出槽。
  template <typename K>
  std::optional<SegPos> FindInOverflow(uint32_t home, uint8_t fp,
                                       const K& key) const {
    for (unsigned stb = 0; stb < kStashBuckets; ++stb) {
      const auto& b = buckets_[kMainBuckets + stb];
      uint16_t mask = b.MatchFp(fp) & b.occupied_ & kSlotMask<kSlots>;
      while (mask != 0) {
        const unsigned s = static_cast<unsigned>(std::countr_zero(mask));
        const unsigned gs = stb * kSlots + s;
        if (overflow_home_[gs] == static_cast<uint8_t>(home) &&
            Policy::Equal(b.KeyAt(s), key)) {
          return SegPos{kMainBuckets + stb, s};
        }
        mask &= mask - 1;
      }
    }
    return std::nullopt;
  }

  // 段内完整查找：主桶 → 邻居桶（探测槽）→ 溢出区。
  template <typename K>
  std::optional<SegPos> FindIn(uint64_t hash, const K& key) const {
    const uint8_t fp = Fingerprint(hash);
    const uint32_t home = HomeBucket(hash);
    const uint32_t nxt = NextBucket(home);

    // 1) 主桶：归属本桶的条目（非探测槽）。
    if (const int i = FindInBucket(buckets_[home], fp, false, key); i >= 0) {
      return SegPos{home, static_cast<uint32_t>(i)};
    }
    // 2) 邻居桶：从主桶均衡/腾挪过去的条目（探测槽）。
    if (const int i = FindInBucket(buckets_[nxt], fp, true, key); i >= 0) {
      return SegPos{nxt, static_cast<uint32_t>(i)};
    }
    // 3) 溢出区：归属主桶的条目。
    if (overflow_cnt_[home] != 0) {
      if (auto pos = FindInOverflow(home, fp, key)) {
        return pos;
      }
    }
    return std::nullopt;
  }

  // ---- 插入 ----

  // 尝试把条目放入指定桶的空闲槽，成功返回槽号。
  // 注意：失败（桶满）时不会触碰 key/value，调用方可安全重试。
  template <typename K, typename V>
  std::optional<unsigned> InsertIntoBucket(Bucket<Key, Value, kSlots>& b,
                                           uint8_t fp, bool probe, K&& key,
                                           V&& value) {
    if (!b.HasFreeSlot()) return std::nullopt;
    const unsigned i = b.FirstFree();
    b.Install(i, fp, probe, std::forward<K>(key), std::forward<V>(value));
    return i;
  }

  // 腾挪：把 from 桶中一个 probe==own 的条目移到 to 桶，返回 from 中被
  // 腾出的槽号；无可移动条目或 to 桶已满时返回空。
  std::optional<unsigned> Relocate(uint32_t from_bid, uint32_t to_bid,
                                   bool own) {
    auto& from = buckets_[from_bid];
    auto& to = buckets_[to_bid];

    uint16_t movable = from.occupied_ & kSlotMask<kSlots>;
    movable &= own ? static_cast<uint16_t>(~from.probe_) : from.probe_;
    if (movable == 0 || !to.HasFreeSlot()) return std::nullopt;

    const unsigned fs = static_cast<unsigned>(std::countr_zero(movable));
    const unsigned ts = to.FirstFree();

    // 移动后探测属性随之改变：
    //   own=true  （条目属于 from）：落于 from+1，成为探测槽；
    //   own=false （条目属于 from-1）：迁回其归属桶，恢复普通槽。
    const bool new_probe = own;
    const uint8_t fp = from.Fp(fs);
    to.Install(ts, fp, new_probe, std::move(from.KeyAt(fs)),
               std::move(from.ValAt(fs)));
    from.template Remove<Policy>(fs);
    return fs;
  }

  // 尝试向段尾溢出区写入一条归属 home 的条目。
  // ignore_limit=true 时忽略每桶配额（仅供分裂重分布兜底）。
  template <typename K, typename V>
  std::optional<SegPos> InsertIntoOverflow(uint32_t home, uint8_t fp, K&& key,
                                           V&& value, bool ignore_limit) {
    if (!ignore_limit && overflow_cnt_[home] >= kMaxOverflowPerBucket) {
      return std::nullopt;
    }
    for (unsigned stb = 0; stb < kStashBuckets; ++stb) {
      auto& b = buckets_[kMainBuckets + stb];
      if (!b.HasFreeSlot()) continue;
      const unsigned s = b.FirstFree();
      const unsigned gs = stb * kSlots + s;
      overflow_home_[gs] = static_cast<uint8_t>(home);
      overflow_cnt_[home]++;
      b.Install(s, fp, false, std::forward<K>(key), std::forward<V>(value));
      ++size_;
      return SegPos{kMainBuckets + stb, s};
    }
    return std::nullopt;
  }

  // 段内插入（带溢出配额限制），供正常写路径使用。
  // spread=true 时优先选择主桶与邻居桶中空闲较多者（均衡安置）。
  // 成功返回段内位置；段内已无法容纳时返回空（触发上层分裂）。
  //
  // 安全性说明：InsertIntoBucket/InsertIntoOverflow 在失败时不移动
  // key/value，因此多个"尝试路径"之间对同一 key 的多次 forward 是安全的。
  template <typename K, typename V>
  std::optional<SegPos> TryInsert(K&& key, V&& value, uint64_t hash,
                                  bool spread) {
    const uint8_t fp = Fingerprint(hash);
    const uint32_t home = HomeBucket(hash);
    const uint32_t nxt = NextBucket(home);
    auto& hb = buckets_[home];
    auto& nb = buckets_[nxt];

    // ---- 第 1 级：均衡安置 ----
    if (spread) {
      const unsigned hfree =
          static_cast<unsigned>(std::popcount(hb.FreeMask()));
      const unsigned nfree =
          static_cast<unsigned>(std::popcount(nb.FreeMask()));
      if (hfree >= nfree) {
        if (auto i = InsertIntoBucket(hb, fp, false, std::forward<K>(key),
                                      std::forward<V>(value))) {
          ++size_;
          return SegPos{home, *i};
        }
        if (auto i = InsertIntoBucket(nb, fp, true, std::forward<K>(key),
                                      std::forward<V>(value))) {
          ++size_;
          return SegPos{nxt, *i};
        }
      } else {
        if (auto i = InsertIntoBucket(nb, fp, true, std::forward<K>(key),
                                      std::forward<V>(value))) {
          ++size_;
          return SegPos{nxt, *i};
        }
        if (auto i = InsertIntoBucket(hb, fp, false, std::forward<K>(key),
                                      std::forward<V>(value))) {
          ++size_;
          return SegPos{home, *i};
        }
      }
    } else {
      // 不均衡：优先主桶，其次邻居桶。
      if (auto i = InsertIntoBucket(hb, fp, false, std::forward<K>(key),
                                    std::forward<V>(value))) {
        ++size_;
        return SegPos{home, *i};
      }
      if (auto i = InsertIntoBucket(nb, fp, true, std::forward<K>(key),
                                    std::forward<V>(value))) {
        ++size_;
        return SegPos{nxt, *i};
      }
    }

    // ---- 第 2 级：腾挪 ----
    // a) 把邻居桶 nxt 中一个"归属 nxt"的条目推给 nxt+1，腾出的槽
    //    交给新条目（以探测槽身份寄居 nxt）。
    if (auto fs = Relocate(nxt, NextBucket(nxt), /*own=*/true)) {
      nb.Install(*fs, fp, true, std::forward<K>(key), std::forward<V>(value));
      ++size_;
      return SegPos{nxt, *fs};
    }
    // b) 把主桶 home 中一个"暂存"的探测条目迁回其归属桶 home-1，
    //    腾出的槽交给新条目（以普通槽身份落位 home）。
    if (auto fs = Relocate(home, PrevBucket(home), /*own=*/false)) {
      hb.Install(*fs, fp, false, std::forward<K>(key), std::forward<V>(value));
      ++size_;
      return SegPos{home, *fs};
    }

    // ---- 第 3 级：段尾溢出区 ----
    return InsertIntoOverflow(home, fp, std::forward<K>(key),
                              std::forward<V>(value), /*ignore_limit=*/false);
  }

  // 强制插入：不设溢出配额限制，供分裂重分布使用。
  // 目标段必然为空，正常路径不会触顶；保留兜底以防极端分布。
  template <typename K, typename V>
  SegPos ForceInsert(K&& key, V&& value, uint64_t hash) {
    if (auto pos = TryInsert(std::forward<K>(key), std::forward<V>(value),
                             hash, /*spread=*/true)) {
      return *pos;
    }
    // TryInsert 失败仅可能源于溢出配额：绕过配额重试。
    const uint8_t fp = Fingerprint(hash);
    const uint32_t home = HomeBucket(hash);
    if (auto pos =
            InsertIntoOverflow(home, fp, std::forward<K>(key),
                               std::forward<V>(value), /*ignore_limit=*/true)) {
      return *pos;
    }
    // 不可达：段容量（主桶 + 溢出）保证总能容纳一次插入。
    __builtin_unreachable();
  }

  // ---- 删除 ----

  // 删除段内位置 (bid, slot)，维护溢出归属与计数。
  void EraseAt(uint32_t bid, uint32_t slot) {
    auto& b = buckets_[bid];
    b.template Remove<Policy>(slot);
    if (bid >= kMainBuckets) {
      const unsigned gs = OverflowGlobal(bid, slot);
      const uint8_t home = overflow_home_[gs];
      overflow_home_[gs] = kNoHome;
      if (home != kNoHome && overflow_cnt_[home] > 0) {
        overflow_cnt_[home]--;
      }
    }
    assert(size_ > 0);
    --size_;
  }

  // ---- 分裂 ----

  // 把本段中"属于 dest"的条目迁移到 dest（dest 必须为空段）。
  // hash_fn 由上层传入（与目录深度解耦，便于测试与复用）。
  // 迁移后本段 local_depth_ 增加，与 dest 保持一致。
  //
  // 留在本段的溢出条目会尝试回收回主桶（分裂后主桶负载下降，
  // 回收可降低后续查找触及溢出区的概率）。
  template <typename H>
  void SplitInto(Segment* dest, H&& hash_fn) {
    ++local_depth_;
    dest->local_depth_ = local_depth_;

    // 判断条目的新归属：取 hash 的第 (64 - local_depth) 位。
    const auto belongs_right = [&](uint64_t h) {
      return (h >> (64 - local_depth_)) & 1u;
    };

    // 主桶：逐个检查，属于 dest 的整槽搬走。
    for (uint32_t bid = 0; bid < kMainBuckets; ++bid) {
      auto& b = buckets_[bid];
      uint16_t mask = b.occupied_ & kSlotMask<kSlots>;
      while (mask != 0) {
        const unsigned s = static_cast<unsigned>(std::countr_zero(mask));
        if (belongs_right(hash_fn(b.KeyAt(s)))) {
          const uint64_t h = hash_fn(b.KeyAt(s));  // 移动前先算哈希
          dest->ForceInsert(std::move(b.KeyAt(s)), std::move(b.ValAt(s)), h);
          b.template Remove<Policy>(s);
          assert(size_ > 0);
          --size_;
        }
        mask &= mask - 1;
      }
    }

    // 溢出区：属于 dest 的迁走；留在本段的尝试回收回主桶。
    for (unsigned gs = 0; gs < kOverflowSlots; ++gs) {
      if (overflow_home_[gs] == kNoHome) continue;
      const uint32_t bid = OverflowBidOf(gs);
      const unsigned slot = OverflowSlotOf(gs);
      auto& b = buckets_[bid];
      const uint8_t home = overflow_home_[gs];
      const uint64_t h = hash_fn(b.KeyAt(slot));

      if (belongs_right(h)) {
        dest->ForceInsert(std::move(b.KeyAt(slot)), std::move(b.ValAt(slot)),
                          h);
        b.template Remove<Policy>(slot);
        overflow_home_[gs] = kNoHome;
        assert(overflow_cnt_[home] > 0);
        overflow_cnt_[home]--;
        assert(size_ > 0);
        --size_;
      } else {
        // 留在本段：优先回收回主桶/邻居，失败则保持溢出槽。
        TryReclaimOverflow(gs, bid, slot, h);
      }
    }
  }

 private:
  // 把溢出槽 (bid, slot) 的条目移回其归属主桶或其邻居（二者必有一空）。
  // 分裂后主桶负载下降，回收可降低后续查找触及溢出区的概率。
  void TryReclaimOverflow(unsigned gs, uint32_t bid, unsigned slot,
                          uint64_t hash) {
    auto& b = buckets_[bid];
    const uint8_t home = overflow_home_[gs];
    auto& hb = buckets_[home];
    auto& nb = buckets_[NextBucket(home)];
    if (!hb.HasFreeSlot() && !nb.HasFreeSlot()) return;

    const uint8_t fp = Fingerprint(hash);
    // 优先主桶，其次邻居（成为探测槽）。
    if (auto i = InsertIntoBucket(hb, fp, false, std::move(b.KeyAt(slot)),
                                  std::move(b.ValAt(slot)))) {
      b.template Remove<Policy>(slot);
      overflow_home_[gs] = kNoHome;
      assert(overflow_cnt_[home] > 0);
      overflow_cnt_[home]--;
      return;
    }
    if (auto i = InsertIntoBucket(nb, fp, true, std::move(b.KeyAt(slot)),
                                  std::move(b.ValAt(slot)))) {
      b.template Remove<Policy>(slot);
      overflow_home_[gs] = kNoHome;
      assert(overflow_cnt_[home] > 0);
      overflow_cnt_[home]--;
    }
  }
};

}  // namespace dash
}  // namespace dfly

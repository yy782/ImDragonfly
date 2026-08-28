#pragma once

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

// ═══════════════════════════════════════════════════════════════════
// Dash 哈希表的"段内布局"（Segment 的结构）
//
// 一个 Segment 是目录中被多个下标共享的物理块，内部有三类桶：
//
//   ┌──────────────────────┬─────────────────────────────────────┐
//   │       主桶区          │             溢出区（备胎）           │
//   │  buckets_[0 ..       │  buckets_[kMainBuckets ..           │
//   │   kMainBuckets-1]    │   kTotalBuckets-1]                  │
//   │   普通条目住这里       │   主桶塞不下时"发配"到这里           │
//   └──────────────────────┴─────────────────────────────────────┘
//
//   另外两张辅助表：
//     overflow_home_[gs]   溢出槽 gs 属于哪个主桶（kNoHome = 空闲）
//     overflow_cnt_[home]  主桶 home 已占用的溢出槽数
//
// 每个主桶的条目可能分布在三处：
//   1) 自己桶里的"非 probe 槽"（自己家）
//   2) 右邻桶的"probe 槽"（借宿在邻居家，线性探测）
//   3) 溢出区（被发配到备胎桶）
// 查找按 1 → 2 → 3 的顺序排查；插入则按 均衡 → 腾挪 → 发配 三级兜底。
// ═══════════════════════════════════════════════════════════════════

// 指纹宽度 8 位：哈希低 8 位作为"指纹"。
// 查找时用 SIMD 一次比较 16 个槽位的指纹，粗筛掉绝大多数不匹配的槽，
// 命中后再做全键精确比较。指纹冲突不影响正确性，只是多一次比较。
static constexpr unsigned kFpBits = 8;
static constexpr uint8_t kFpMask = 0xFFu;

// 每个物理桶最多 16 个槽：恰好一个 __m128i（16 字节），
// 指纹数组可用一条 SIMD 指令整体加载、整体比较。
static constexpr unsigned kMaxSlotsPerBucket = 16;

// 每个主桶在溢出区最多占 4 个溢出槽。
// 超过上限说明桶已经极度拥挤，交由上层去分裂（SplitSegment），
// 而不是无限侵占溢出区。
static constexpr unsigned kMaxOverflowPerBucket = 4;

// 溢出槽的归属标记：kNoHome 表示该溢出槽当前空闲；
// 否则存的是所属主桶的下标（uint8_t，能表示 0..255 号主桶）。
static constexpr uint8_t kNoHome = 0xFFu;

static_assert(kMaxOverflowPerBucket <= kMaxSlotsPerBucket);

// 2 的幂取对数（如 kMainBuckets=8 → 3）。
// 依赖"桶数必须是 2 的幂"，这样才能用移位替代乘除。
inline constexpr unsigned Log2u(unsigned n) {
  return static_cast<unsigned>(std::countr_zero(n));
}

template <typename Policy, typename = void>
struct StashBucketNum {
  static constexpr unsigned value = 4;
};
template <typename Policy>
struct StashBucketNum<Policy, std::void_t<decltype(Policy::kStashNum)>> {
  static constexpr unsigned value = Policy::kStashNum;
};

// 槽位掩码：kSlots=4 → 0b1111，kSlots=8 → 0b11111111。
// 用于把位图裁剪到有效槽位数，避免高位的脏位参与运算。
template <unsigned kSlots>
inline constexpr uint16_t kSlotMask = static_cast<uint16_t>((1u << kSlots) - 1);

// ═══════════════════════════════════════════════════════════════════
// 物理桶（Bucket）的内存布局
//
//   ┌───────────────────────────────────────────┐
//   │ fp_[16]     每个槽位的 8 位指纹（可 SIMD） │
//   │ occupied_   位图：第 i 位 = 槽 i 被占用    │
//   │ probe_      位图：第 i 位 = 槽 i 是"借宿"  │
//   │ keys_[kSlots] / vals_[kSlots]  键值本体   │
//   └───────────────────────────────────────────┘
//
// occupied_ 与 probe_ 的配合：
//   - probe 槽里的条目是"上一个主桶"（home）寄存在这里的，
//     所以它不属于本桶，而属于 home；
//   - 查找 home 的条目时，先看本桶的"非 probe 槽"（自己家），
//     再看右邻桶的 "probe 槽"（被邻居收留的那些）。
// ═══════════════════════════════════════════════════════════════════
template <typename Key, typename Value, unsigned kSlots>
struct alignas(64) Bucket {
  static_assert(kSlots > 0 && kSlots <= kMaxSlotsPerBucket,
                "kSlots must be in (0, 16]");

  uint8_t fp_[kMaxSlotsPerBucket];
  uint16_t occupied_;
  uint16_t probe_;

  Key keys_[kSlots];
  Value vals_[kSlots];

  Bucket() : occupied_(0), probe_(0) { std::memset(fp_, 0, sizeof(fp_)); }

  Bucket(const Bucket&) = delete;
  Bucket& operator=(const Bucket&) = delete;

  bool IsOccupied(unsigned i) const { return (occupied_ >> i) & 1u; }
  bool IsProbe(unsigned i) const { return (probe_ >> i) & 1u; }
  uint8_t Fp(unsigned i) const { return fp_[i]; }

  Key& KeyAt(unsigned i) { return keys_[i]; }
  const Key& KeyAt(unsigned i) const { return keys_[i]; }
  Value& ValAt(unsigned i) { return vals_[i]; }
  const Value& ValAt(unsigned i) const { return vals_[i]; }

  uint16_t FreeMask() const { return (~occupied_) & kSlotMask<kSlots>; }
  bool HasFreeSlot() const { return FreeMask() != 0; }

  unsigned FirstFree() const {
    return static_cast<unsigned>(std::countr_zero(FreeMask()));
  }

  uint16_t MatchFp(uint8_t fp) const {
    const __m128i key = _mm_set1_epi8(static_cast<char>(fp));
    const __m128i data = _mm_loadu_si128(reinterpret_cast<const __m128i*>(fp_));
    const __m128i eq = _mm_cmpeq_epi8(data, key);
    const uint16_t m = static_cast<uint16_t>(_mm_movemask_epi8(eq));
    return m & kSlotMask<kSlots>;
  }

  template <typename K, typename V>
  void Install(unsigned i, uint8_t fp, bool probe, K&& key, V&& value) {
    fp_[i] = fp;
    occupied_ |= static_cast<uint16_t>(1u << i);
    if (probe) probe_ |= static_cast<uint16_t>(1u << i);
    new (&keys_[i]) Key(std::forward<K>(key));
    new (&vals_[i]) Value(std::forward<V>(value));
  }

  template <typename Policy>
  void Remove(unsigned i) {
    Policy::DestroyKey(keys_[i]);
    Policy::DestroyValue(vals_[i]);
    keys_[i].~Key();
    vals_[i].~Value();
    occupied_ &= static_cast<uint16_t>(~(1u << i));
    probe_ &= static_cast<uint16_t>(~(1u << i));
  }
};

struct SegPos {
  uint32_t bid;
  uint32_t slot;
};

// ═══════════════════════════════════════════════════════════════════
// Segment：目录中的一个物理块
//
//   buckets_[kTotalBuckets]         kMainBuckets 主桶 + kStashBuckets 溢出桶
//   overflow_home_[kOverflowSlots]  每个溢出槽的归属主桶（kNoHome = 空闲）
//   overflow_cnt_[kMainBuckets]     每个主桶已占用的溢出槽数
//   local_depth_                    局部深度（见下）
//   size_                           本段条目数
//
// 局部深度与目录的关系：
//   目录长度 = 2^global_depth_，一个物理段被目录中连续的
//   1 << (global_depth_ - local_depth_) 个下标共享引用。
//   段越"年轻"（局部深度小），被共享的别名越多；一满就分裂：
//   local_depth_ + 1，别名区间对半切，段数翻倍。
// ═══════════════════════════════════════════════════════════════════
template <typename Key, typename Value, typename Policy, unsigned kSlots,
          unsigned kMainBuckets, unsigned kStashBuckets>
struct Segment {
  static_assert(kMainBuckets > 0 && kStashBuckets > 0);
  static_assert((kMainBuckets & (kMainBuckets - 1)) == 0,
                "kMainBuckets must be a power of two");
  static_assert((kSlots & (kSlots - 1)) == 0, "kSlots must be a power of two");
  static constexpr unsigned kTotalBuckets = kMainBuckets + kStashBuckets;
  static constexpr unsigned kOverflowSlots = kStashBuckets * kSlots;

  Bucket<Key, Value, kSlots> buckets_[kTotalBuckets];

  uint8_t overflow_home_[kOverflowSlots];
  uint8_t overflow_cnt_[kMainBuckets];

  uint8_t local_depth_ = 0;
  uint32_t size_ = 0;

  Segment() {
    std::memset(overflow_home_, kNoHome, sizeof(overflow_home_));
    std::memset(overflow_cnt_, 0, sizeof(overflow_cnt_));
  }

  Segment(const Segment&) = delete;
  Segment& operator=(const Segment&) = delete;

  static uint32_t HomeBucket(uint64_t hash) {
    return static_cast<uint32_t>((hash >> kFpBits) & (kMainBuckets - 1));
  }

  static uint32_t NextBucket(uint32_t b) { return b + 1; }

  static uint32_t PrevBucket(uint32_t b) { return b - 1; }

  static uint8_t Fingerprint(uint64_t hash) {
    return static_cast<uint8_t>(hash & kFpMask);
  }

  Bucket<Key, Value, kSlots>& BucketAt(uint32_t bid) { return buckets_[bid]; }
  const Bucket<Key, Value, kSlots>& BucketAt(uint32_t bid) const {
    return buckets_[bid];
  }

  bool IsOverflowBid(uint32_t bid) const { return bid >= kMainBuckets; }

  uint8_t LocalDepth() const { return local_depth_; }
  void SetLocalDepth(uint8_t d) { local_depth_ = d; }
  uint32_t Size() const { return size_; }

  static uint32_t OverflowBidOf(unsigned gs) {
    return kMainBuckets + (gs >> Log2u(kSlots));
  }
  static unsigned OverflowSlotOf(unsigned gs) { return gs & (kSlots - 1); }
  static unsigned OverflowGlobal(uint32_t bid, unsigned slot) {
    return ((bid - kMainBuckets) << Log2u(kSlots)) + slot;
  }

  // 单个物理桶内的查找：
  //   1) 指纹粗筛（MatchFp）得到候选槽位图
  //   2) 与 occupied_ 相交：只看被占用的槽
  //   3) 按 probe 位过滤：probe=true 只看借宿槽，false 只看自家槽
  //   4) 对每个候选槽做全键精确比较
  // 返回 -1 表示本桶没有该键。
  template <typename K>
  int FindInBucket(const Bucket<Key, Value, kSlots>& b, uint8_t fp, bool probe,
                   const K& key) const {
    uint16_t mask = b.MatchFp(fp) & b.occupied_ & kSlotMask<kSlots>;
    mask &= probe ? b.probe_ : static_cast<uint16_t>(~b.probe_);
    while (mask != 0) {
      const unsigned i = static_cast<unsigned>(std::countr_zero(mask));
      if (Policy::Equal(b.KeyAt(i), key)) {
        return static_cast<int>(i);
      }
      mask &= mask - 1;
    }
    return -1;
  }

  // 溢出区查找：遍历所有溢出桶，指纹粗筛后，
  // 还要验证 overflow_home_[gs] == home（确认这个溢出槽确实是"当前主桶"的）。
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

  // 段内完整查找，按"归属"三级排查：
  //   1) 主桶的非 probe 槽（条目住自己家）
  //   2) 右邻桶的 probe 槽（条目借宿在邻居家）
  //   3) 溢出区（条目被发配到备胎桶）
  // 三级都不命中 → 本段没有该键。
  template <typename K>
  std::optional<SegPos> FindIn(uint64_t hash, const K& key) const {
    const uint8_t fp = Fingerprint(hash);
    const uint32_t home = HomeBucket(hash);
    const uint32_t nxt = NextBucket(home);

    if (const int i = FindInBucket(buckets_[home], fp, false, key); i >= 0) {
      return SegPos{home, static_cast<uint32_t>(i)};
    }
    if (nxt < kMainBuckets) {
      if (const int i = FindInBucket(buckets_[nxt], fp, true, key); i >= 0) {
        return SegPos{nxt, static_cast<uint32_t>(i)};
      }
    }
    if (overflow_cnt_[home] != 0) {
      if (auto pos = FindInOverflow(home, fp, key)) {
        return pos;
      }
    }
    return std::nullopt;
  }

  template <typename K, typename V>
  std::optional<unsigned> InsertIntoBucket(Bucket<Key, Value, kSlots>& b,
                                           uint8_t fp, bool probe, K&& key,
                                           V&& value) {
    if (!b.HasFreeSlot()) return std::nullopt;
    const unsigned i = b.FirstFree();
    b.Install(i, fp, probe, std::forward<K>(key), std::forward<V>(value));
    return i;
  }

  /*
        //腾挪
        //               Relocate(from_bid, to_bid, own)
        //                    │
        //                    ▼
        //       ┌────────────────────────┐
        //       │ 获取 from 桶和 to 桶   │
        //       │ auto& from = buckets_[from_bid] │
        //       │ auto& to = buckets_[to_bid]     │
        //       └────────────────────────┘
        //                    │
        //                    ▼
        //       ┌────────────────────────┐
        //       │ 计算"可搬动"的槽位     │
        //       │ movable = occupied & mask │
        //       └────────────────────────┘
        //                    │
        //                    ▼
        //       ┌────────────────────────┐
        //       │ own=true 还是 false?   │
        //       └────────────────────────┘
        //              /        \
        //           true         false
        //             │            │
        //             ▼            ▼
        //  ┌──────────────────┐ ┌──────────────────┐
        //  │ 搬"自家条目"     │ │ 搬"借宿条目"     │
        //  │ movable &= ~probe│ │ movable &= probe │
        //  └──────────────────┘ └──────────────────┘
        //             │            │
        //             └──────┬─────┘
        //                    ▼
        //       ┌────────────────────────┐
        //       │ 有可搬的 且 to 有空位? │
        //       └────────────────────────┘
        //              /        \
        //            是          否
        //             │           │
        //             ▼           ▼
        //  ┌──────────────────┐ ┌──────────────────┐
        //  │  开始搬移        │ │  返回 nullopt    │
        //  └──────────────────┘ └──────────────────┘
        //             │
        //             ▼
        //  ┌──────────────────┐
        //  │ 选 from 中      │
        //  │ 第一个可搬槽位   │
        //  │ fs = countr_zero(movable) │
        //  └──────────────────┘
        //             │
        //             ▼
        //  ┌──────────────────┐
        //  │ 选 to 中        │
        //  │ 第一个空闲槽位   │
        //  │ ts = to.FirstFree() │
        //  └──────────────────┘
        //             │
        //             ▼
        //  ┌──────────────────┐
        //  │ 计算新条目的     │
        //  │ probe 标记       │
        //  │ new_probe = own  │
        //  └──────────────────┘
        //             │
        //             ▼
        //  ┌──────────────────┐
        //  │ 把条目搬到 to    │
        //  │ to.Install(ts, fp, new_probe, ...) │
        //  └──────────────────┘
        //             │
        //             ▼
        //  ┌──────────────────┐
        //  │ 从 from 删除     │
        //  │ from.Remove(fs)  │
        //  └──────────────────┘
        //             │
        //             ▼
        //  ┌──────────────────┐
        //  │ 返回被搬走的     │
        //  │ 槽位号 fs        │
        //  └──────────────────┘
  */
  std::optional<unsigned> Relocate(uint32_t from_bid, uint32_t to_bid,
                                   bool own) {
    auto& from = buckets_[from_bid];
    auto& to = buckets_[to_bid];

    uint16_t movable = from.occupied_ & kSlotMask<kSlots>;
    movable &= own ? static_cast<uint16_t>(~from.probe_) : from.probe_;
    if (movable == 0 || !to.HasFreeSlot()) return std::nullopt;

    const unsigned fs = static_cast<unsigned>(std::countr_zero(movable));
    const unsigned ts = to.FirstFree();

    const bool new_probe = own;
    const uint8_t fp = from.Fp(fs);
    to.Install(ts, fp, new_probe, std::move(from.KeyAt(fs)),
               std::move(from.ValAt(fs)));
    from.template Remove<Policy>(fs);
    return fs;
  }

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

  /*
        //                 开始插入
        //                    │
        //                    ▼
        //       ┌────────────────────────┐
        //       │ 计算 home, nxt, fp     │
        //       │ home = HomeBucket(hash)│
        //       │ nxt = home + 1         │
        //       └────────────────────────┘
        //                    │
        //                    ▼
        //       ┌────────────────────────┐
        //       │   spread = true?       │
        //       │   (均衡安置模式)        │
        //       └────────────────────────┘
        //              /        \
        //            是          否
        //             │           │
        //             ▼           ▼
        //  ┌──────────────────┐ ┌──────────────────┐
        //  │  比较 home 和    │ │  优先插 home     │
        //  │  nxt 谁更空      │ │  再插 nxt        │
        //  └──────────────────┘ └──────────────────┘
        //             │              │
        //             └──────┬───────┘
        //                    ▼
        //       ┌────────────────────────┐
        //       │  插进去了吗？           │
        //       └────────────────────────┘
        //              /        \
        //            是          否
        //             │           │
        //             ▼           ▼
        //       ┌─────────┐ ┌─────────────────┐
        //       │ 成功!   │ │  第2级: 腾挪    │
        //       │ 返回pos │ └─────────────────┘
        //       └─────────┘          │
        //                            ▼
        //              ┌─────────────────────────┐
        //              │  方案a: 把 nxt 的       │
        //              │  自家条目挪到 nxt+1     │
        //              │  Relocate(nxt, nxt+1,   │
        //              │           own=true)     │
        //              └─────────────────────────┘
        //                            │
        //              ┌─────────────┴─────────────┐
        //              │ 成功?                      │
        //              └─────────────┬─────────────┘
        //             成功           │           失败
        //              │             │             │
        //              ▼             │             ▼
        //       ┌─────────────┐      │  ┌─────────────────────┐
        //       │ nxt 腾出空位│      │  │ 方案b: 把 home 的   │
        //       │ 插入到 nxt  │      │  │ 借宿条目挪到 home-1 │
        //       │ (标记probe) │      │  │ Relocate(home,      │
        //       └─────────────┘      │  │  home-1, own=false) │
        //              │             │  └─────────────────────┘
        //              ▼             │           │
        //       ┌─────────────┐      │  ┌────────┴────────┐
        //       │  成功!      │      │  │ 成功?           │
        //       │  返回pos    │      │  └────────┬────────┘
        //       └─────────────┘      │    成功   │  失败
        //              ▲             │     │     │   │
        //              │             │     ▼     │   ▼
        //              └─────────────┘ ┌────────┐│ ┌─────────────┐
        //                              │home腾出││ │ 第3级:      │
        //                              │空位    ││ │ 发配溢出区  │
        //                              │插入home││ └─────────────┘
        //                              └────────┘│      │
        //                                 │      │      ▼
        //                                 ▼      │ ┌─────────────┐
        //                              ┌────────┐│ │ 成功?       │
        //                              │ 成功!  ││ └─────────────┘
        //                              │ 返回pos││    /        \
        //                              └────────┘│  是          否
        //                                 ▲      │   │           │
        //                                 │      │   ▼           ▼
        //                                 └──────┘ ┌────────┐ ┌──────────┐
        //                                           │ 成功!  │ │ 返回     │
        //                                           │ 返回pos│ │ nullopt  │
        //                                           └────────┘ └──────────┘
  */
  template <typename K, typename V>
  std::optional<SegPos> TryInsert(K&& key, V&& value, uint64_t hash,
                                  bool spread) {
    const uint8_t fp = Fingerprint(hash);
    const uint32_t home = HomeBucket(hash);
    const uint32_t nxt = NextBucket(home);
    const bool has_nxt = nxt < kMainBuckets;
    auto& hb = buckets_[home];
    Bucket<Key, Value, kSlots>* nb = has_nxt ? &buckets_[nxt] : nullptr;

    if (spread) {
      if (has_nxt) {
        const unsigned hfree =
            static_cast<unsigned>(std::popcount(hb.FreeMask()));
        const unsigned nfree =
            static_cast<unsigned>(std::popcount(nb->FreeMask()));
        if (hfree >= nfree) {
          if (auto i = InsertIntoBucket(hb, fp, false, std::forward<K>(key),
                                        std::forward<V>(value))) {
            ++size_;
            return SegPos{home, *i};
          }
          if (auto i = InsertIntoBucket(*nb, fp, true, std::forward<K>(key),
                                        std::forward<V>(value))) {
            ++size_;
            return SegPos{nxt, *i};
          }
        } else {
          if (auto i = InsertIntoBucket(*nb, fp, true, std::forward<K>(key),
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
        if (auto i = InsertIntoBucket(hb, fp, false, std::forward<K>(key),
                                      std::forward<V>(value))) {
          ++size_;
          return SegPos{home, *i};
        }
      }
    } else {
      if (auto i = InsertIntoBucket(hb, fp, false, std::forward<K>(key),
                                    std::forward<V>(value))) {
        ++size_;
        return SegPos{home, *i};
      }
      if (has_nxt) {
        if (auto i = InsertIntoBucket(*nb, fp, true, std::forward<K>(key),
                                      std::forward<V>(value))) {
          ++size_;
          return SegPos{nxt, *i};
        }
      }
    }

    if (has_nxt && NextBucket(nxt) < kMainBuckets) {
      if (auto fs = Relocate(nxt, NextBucket(nxt), /*own=*/true)) {
        nb->Install(*fs, fp, true, std::forward<K>(key),
                    std::forward<V>(value));
        ++size_;
        return SegPos{nxt, *fs};
      }
    }
    if (PrevBucket(home) < kMainBuckets) {
      if (auto fs = Relocate(home, PrevBucket(home), /*own=*/false)) {
        hb.Install(*fs, fp, false, std::forward<K>(key),
                   std::forward<V>(value));
        ++size_;
        return SegPos{home, *fs};
      }
    }

    return InsertIntoOverflow(home, fp, std::forward<K>(key),
                              std::forward<V>(value), /*ignore_limit=*/false);
  }

  template <typename K, typename V>
  SegPos ForceInsert(K&& key, V&& value, uint64_t hash) {
    if (auto pos = TryInsert(std::forward<K>(key), std::forward<V>(value), hash,
                             /*spread=*/true)) {
      return *pos;
    }
    const uint8_t fp = Fingerprint(hash);
    const uint32_t home = HomeBucket(hash);
    if (auto pos =
            InsertIntoOverflow(home, fp, std::forward<K>(key),
                               std::forward<V>(value), /*ignore_limit=*/true)) {
      return *pos;
    }
    __builtin_unreachable();
  }

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

  /*
                      SplitInto(dest, hash_fn)
                           │
                           ▼
              ┌────────────────────────┐
              │ local_depth_++         │
              │ dest->local_depth_ =   │
              │   local_depth_         │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │ 定义 belongs_right     │
              │ 检查哈希值的新一位     │
              │ (第 64-local_depth 位) │
              └────────────────────────┘
                           │
                           ▼
         ┌─────────────────────────────────┐
         │  第一阶段: 处理主桶区           │
         │  for bid in 0..kMainBuckets-1  │
         └─────────────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │ 遍历桶内所有槽位        │
              │ mask = occupied        │
              └────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │ belongs_right(hash)?   │
              └────────────────────────┘
                     /        \
                   是          否
                    │           │
                    ▼           ▼
         ┌──────────────────┐ ┌──────────────────┐
         │ 搬到 dest        │ │ 留在原段         │
         │ ForceInsert      │ │ (不动)           │
         │ 从原段删除       │ │                  │
         │ size_--         │ │                  │
         └──────────────────┘ └──────────────────┘
                    │            │
                    └──────┬─────┘
                           ▼
         ┌─────────────────────────────────┐
         │  第二阶段: 处理溢出区           │
         │  for gs in 0..kOverflowSlots-1 │
         └─────────────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │ overflow_home_[gs]     │
              │ == kNoHome?           │
              └────────────────────────┘
                     /        \
                   是          否
                    │           │
                    ▼           ▼
              ┌─────────┐ ┌─────────────────┐
              │ 跳过    │ │ belongs_right?  │
              └─────────┘ └─────────────────┘
                            /        \
                          是          否
                           │           │
                           ▼           ▼
                ┌──────────────────┐ ┌──────────────────┐
                │ 搬到 dest        │ │ TryReclaimOverflow│
                │ ForceInsert      │ │ 尝试回收回主桶区  │
                │ 清理溢出辅助表   │ │                  │
                │ size_--         │ │                  │
                └──────────────────┘ └──────────────────┘
  */
  template <typename H>
  void SplitInto(Segment* dest, H&& hash_fn) {
    ++local_depth_;
    dest->local_depth_ = local_depth_;

    const auto belongs_right = [&](uint64_t h) {
      return (h >> (64 - local_depth_)) & 1u;
    };

    for (uint32_t bid = 0; bid < kMainBuckets; ++bid) {
      auto& b = buckets_[bid];
      uint16_t mask = b.occupied_ & kSlotMask<kSlots>;
      while (mask != 0) {
        const unsigned s = static_cast<unsigned>(std::countr_zero(mask));
        if (belongs_right(hash_fn(b.KeyAt(s)))) {
          const uint64_t h = hash_fn(b.KeyAt(s));
          dest->ForceInsert(std::move(b.KeyAt(s)), std::move(b.ValAt(s)), h);
          b.template Remove<Policy>(s);
          assert(size_ > 0);
          --size_;
        }
        mask &= mask - 1;
      }
    }

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
        TryReclaimOverflow(gs, bid, slot, h);
      }
    }
  }

 private:
  /*
                TryReclaimOverflow(gs, bid, slot, hash)
                            │
                            ▼
               ┌────────────────────────┐
               │ 获取溢出桶 b           │
               │ home = overflow_home_[gs] │
               │ hb = buckets_[home]    │
               │ nxt = home + 1         │
               └────────────────────────┘
                            │
                            ▼
               ┌────────────────────────┐
               │ home 或 nxt 有空位?    │
               └────────────────────────┘
                      /        \
                    是          否
                     │           │
                     ▼           ▼
          ┌──────────────────┐ ┌──────────────────┐
          │  尝试回收        │ │  直接返回        │
          └──────────────────┘ │  (继续留在溢出区)│
                     │         └──────────────────┘
                     ▼
          ┌──────────────────┐
          │ 第1步: 尝试搬回   │
          │ home (自己家)    │
          │ 标记为自家       │
          │ (probe=false)    │
          └──────────────────┘
                     │
          ┌──────────┴──────────┐
          │ 成功?               │
          └──────────┬──────────┘
         成功        │         失败
          │          │          │
          ▼          │          ▼
     ┌─────────┐     │   ┌──────────────────┐
     │ 清理    │     │   │ 第2步: 尝试搬到   │
     │ 溢出区  │     │   │ nxt (邻居家)     │
     │ 返回    │     │   │ 标记为借宿       │
     └─────────┘     │   │ (probe=true)     │
                     │   └──────────────────┘
                     │           │
                     │   ┌───────┴───────┐
                     │   │ 成功?          │
                     │   └───────┬───────┘
                     │   成功    │   失败
                     │    │      │    │
                     │    ▼      │    ▼
                     │ ┌────────┐│ ┌──────────┐
                     │ │ 清理   ││ │ 留在溢出 │
                     │ │ 溢出区 ││ │ 区       │
                     │ │ 返回   ││ └──────────┘
                     │ └────────┘│
                     └─────┬─────┘
                           │
                           ▼
                     ┌──────────────┐
                     │ 函数结束      │
                     └──────────────┘
  */
  void TryReclaimOverflow(unsigned gs, uint32_t bid, unsigned slot,
                          uint64_t hash) {
    auto& b = buckets_[bid];
    const uint8_t home = overflow_home_[gs];
    auto& hb = buckets_[home];
    const uint32_t nxt = NextBucket(home);
    const bool has_nxt = nxt < kMainBuckets;
    if (!hb.HasFreeSlot() && (!has_nxt || !buckets_[nxt].HasFreeSlot())) {
      return;
    }

    const uint8_t fp = Fingerprint(hash);
    if (auto i = InsertIntoBucket(hb, fp, false, std::move(b.KeyAt(slot)),
                                  std::move(b.ValAt(slot)))) {
      b.template Remove<Policy>(slot);
      overflow_home_[gs] = kNoHome;
      assert(overflow_cnt_[home] > 0);
      overflow_cnt_[home]--;
      return;
    }
    if (has_nxt) {
      if (auto i = InsertIntoBucket(buckets_[nxt], fp, true,
                                    std::move(b.KeyAt(slot)),
                                    std::move(b.ValAt(slot)))) {
        b.template Remove<Policy>(slot);
        overflow_home_[gs] = kNoHome;
        assert(overflow_cnt_[home] > 0);
        overflow_cnt_[home]--;
      }
    }
  }
};

}  // namespace dash
}  // namespace dfly

# DashTable：基于分段的高性能哈希表设计

> **参考论文**：[Dash: Scalable Hashing on Persistent Memory](https://arxiv.org/abs/2003.07302) — VLDB 2020
> **原始实现**：DragonflyDB

---

## 1. 设计背景

传统链式哈希的每个节点独立分配在堆上，通过指针串联。当 Key 不命中时，CPU 需要追逐指针链，每跳一次都可能触发 cache miss。对于内存数据库而言，数据访问本身只需几十纳秒，但一次 cache miss 就要上百纳秒——指针追逐成了性能瓶颈。

DashTable 用三个设计解决这个问题：

- **开放寻址**：数据连续存储在 Bucket 数组里，线性探测替代指针链
- **分段管理**：Segment 作为扩容/缩容的基本单位，类似 Extendible Hashing 的目录映射
- **指纹过滤**：Hash 值低 8 位作为指纹，用 SIMD 指令一次性比对，跳过完整 Key 比较

---

## 2. 总体架构

```
┌───────────────────────────────────────────────────────────────────┐
│                    Directory (segment_ 数组)                       │
│                                                                   │
│  segment_[0]──→Seg0   segment_[2]──→Seg2   segment_[4]──→Seg4   ......│
│  segment_[1]──→Seg0   segment_[3]──→Seg2   segment_[5]──→Seg4   │
│       ↑ 逻辑段               ↑ 逻辑段                              │
│       可以指向               可以指向                               │
│       同一个物理段           同一个物理段                            │
└───────────────────────────────────────────────────────────────────┘
         │                      │                      │
         ▼                      ▼                      ▼
  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
  │   Segment 0  │     │   Segment 2  │     │   Segment 4  │
  │  (物理段)     │     │  (物理段)     │     │  (物理段)     │
  │              │     │              │     │              │
  │ Bucket[0..63]│     │ Bucket[0..63]│     │ Bucket[0..63]│
  │ Stash[0..3]  │     │ Stash[0..3]  │     │ Stash[0..3]  │
  └──────────────┘     └──────────────┘     └──────────────┘
```

**逻辑段 vs 物理段**：`segment_` 数组的每个元素是一个"逻辑段"——它是一个指针，指向真正存储数据的"物理段"（`Segment` 对象）。不同的逻辑段可以指向同一个物理段，这是扩容机制的核心。

**层次**：

| 层级 | 说明 |
|------|------|
| Directory | 可动态扩展的指针数组 `segment_`，大小 = `2^global_depth_` |
| Segment | 64 个主桶 + 4 个 Stash 桶，扩容时的基本单位 |
| Bucket | 12 个 Key/Value 槽位 + 元数据 |
| Slot | 单个 Key/Value 对 |

---

## 3. Bucket 内部结构

```
Bucket 布局:
┌────────────────────────────────────────────────────────────────┐
│ slotb_      │ finger_arr_ │ stash_arr_ │ stash 元数据 │ KV Slots │
│ bitmap (8B) │ 12 bytes    │ 4 bytes    │ 3 bytes      │ 12 × ... │
│             │             │            │              │          │
│ busy位图     │ 槽位指纹     │ Stash指纹   │ busy/pos/    │ key[12]  │
│ probe位图    │             │            │ overflow_cnt │ value[12]│
└────────────────────────────────────────────────────────────────┘
```

### 3.1 SlotBitmap — 两张位图

每个 Bucket 用两个 `uint32_t` 管理 12 个槽位（受限于 `__builtin_ctz` 在 32 位上的高效操作）：

```
第一个 uint32_t  busy 位图:
  Bit 0-11: 每个槽位是否被占用
  Bit 12-31: 未使用

第二个 uint32_t  probe 位图:
  Bit 0-11: 该槽位是否属于邻居桶的探测条目
  Bit 12-31: 未使用
```

源码注释：

```cpp
// 超过 28 个槽位，单个 uint32_t（32 位）存不下所有状态
static_assert(NUM_SLOTS > 0 && NUM_SLOTS <= 28);
```

### 3.2 Fingerprint — 指纹数组

`finger_arr_` 有 12 个字节，每个槽位对应 1 字节指纹。指纹 = `hash & 0xFF`（8 位）。

查找时用 `_mm_cmpeq_epi8` 一条 SIMD 指令同时比对 16 个字节：

```cpp
uint32_t CompareFP(uint8_t fp) const {
    const __m128i key_data = _mm_set1_epi8(fp);           // 广播指纹到 16 字节
    __m128i seg_data = _mm_loadu_si128(                    // 加载指纹数组
        reinterpret_cast<const __m128i*>(finger_arr_.data()));
    __m128i rv_mask = _mm_cmpeq_epi8(seg_data, key_data); // 并行比较
    int mask = _mm_movemask_epi8(rv_mask);                // 16 位结果掩码
    return mask;
}
```

指纹匹配的槽位才进入完整 Key 比较。负查询在指纹阶段就能确定 Key 不存在，完全避免了加载和比较 Key 值的开销。

### 3.3 Stash 元数据 — 溢出引用管理

Bucket 内嵌 4 字节的 Stash 指纹数组，加上 3 个控制字段：

| 字段 | 含义 |
|------|------|
| `stash_busy_` | 低 4 位：哪些 Stash 指纹槽被占用；bit 4：HasStash 标记 |
| `stash_pos_` | 每 2 位编码一个 Stash 桶 ID（0-3），4 个槽位共 8 位 |
| `stash_probe_mask_` | 标记哪些 Stash 条目来自 probe（邻居探测） |
| `overflow_count_` | 记录有多少个 Stash 引用溢出到了邻居桶中 |

`SetStashPtr` 的注释：

```cpp
// 遍历 Stash 指针并查找匹配指纹
template <typename F>
std::pair<unsigned, SlotId> IterateStash(uint8_t fp, bool is_probe, F&& func) const {
    unsigned om = is_probe ? stash_probe_mask_ : ~stash_probe_mask_;
    unsigned ob = stash_busy_;
    for (unsigned i = 0; i < kStashFpLen; ++i) {
        if ((ob & 1) && (stash_arr_[i] == fp) && (om & 1)) {
            unsigned pos = (stash_pos_ >> (i * 2)) & 3;
            // 从 stash_pos_ 中提取当前 Stash 指针的 2 位，获得溢出桶 ID
            ...
        }
        ob >>= 1; om >>= 1;
    }
}
```

---

## 4. 目录与扩容机制

DashTable 的扩容借鉴了 Extendible Hashing 的目录映射设计。

### 4.1 核心概念

```cpp
// dash_table.hpp 源码注释
/*
    术语:
        逻辑段: segment_每一个元素为一个逻辑段，逻辑段为一个指针,
                指向物理段，不同的逻辑段可能指向相同的物理段
        物理段: 逻辑段为一个指针,指向真正存储数据的物理段

    参数：unique_segments_, initial_depth_, global_depth_;
    构造: initial_depth_ = global_depth_ = capacity_log,
          unique_segments_ = 2^capacity_log
*/
```

- `global_depth_`：决定目录大小 = `2^global_depth_`；Key 需要用 `global_depth_` 位哈希值来定位逻辑段
- `local_depth_`：每个 Segment 的局部深度；决定有多少个目录槽指向同一个物理段
- 一个物理段覆盖的目录项数量 = `2^(global_depth_ - local_depth_)`

### 4.2 初始状态

以 `capacity_log = 2` 为例（`global_depth_ = 2`）：

```
Hash 值高 2 位作为 Segment ID：
  key_hash = Hash(key)
  seg_id = key_hash >> (64 - 2)   // 取最高 2 位

初始目录（4 个逻辑段 → 4 个物理段，一一对应）：
  00 → Seg_0 (local_depth=2)
  01 → Seg_1 (local_depth=2)
  10 → Seg_2 (local_depth=2)
  11 → Seg_3 (local_depth=2)
```

此时 `local_depth_ == global_depth_`，每个物理段独占一个目录槽。

### 4.3 Segment Split — 段分裂

当某个 Segment 插满且 Stash 也满时触发 Split。

假设 `Seg_0`（目录槽 `00`）满了。由于 `local_depth_ == global_depth_`，必须先扩大目录：

```
IncreaseDepth(3): global_depth_ 从 2 变为 3

扩大后目录（每个物理段被 2 个逻辑段共享）：
  000 → Seg_0   001 → Seg_0
  010 → Seg_1   011 → Seg_1
  100 → Seg_2   101 → Seg_2
  110 → Seg_3   111 → Seg_3
```

现在 `Seg_0` 的 `local_depth_=2`，而 `global_depth_=3`，它被两个逻辑段（`000` 和 `001`）引用。此时可以 Split：

```
Split Seg_0(local_depth=2 → 3):

  原始：000 → Seg_0   001 → Seg_0

  分裂后：
    Seg_0 的 local_depth_ += 1  →  3
    新建 Seg_4 的 local_depth_ = 3

    对 Seg_0 中所有 Key 重新哈希，看 hash >> (64-3) 的最低位：
      bit = 0  → 留在 Seg_0（目录槽 000→Seg_0）
      bit = 1  → 迁移到 Seg_4（目录槽 001→Seg_4）

  最终：
    000 → Seg_0 (local_depth=3)
    001 → Seg_4 (local_depth=3)
    010 → Seg_1
    011 → Seg_1
    ...
```

**Split 源码核心逻辑**：

```cpp
// dash_internal.hpp Segment::Split
void Segment::Split(HFunc&& hfn, Segment* dest_right, MoveCb&& on_move_cb) {
    ++local_depth_;                         // 原段深度 +1
    dest_right->local_depth_ = local_depth_; // 新段深度相同

    // 判断 Key 属于哪个段：hash >> (64 - local_depth_) 的最低位
    auto is_mine = [this](Hash_t hash) {
        return (hash >> (64 - local_depth_) & 1) == 0;  // bit=0 留在原段
    };

    // 遍历所有主桶，不属于自己的 Key 迁移到 dest_right
    for (unsigned i = 0; i < kBucketNum; ++i) {
        bucket_[i].ForEachSlot([&](...) {
            if (!is_mine(hfn(key))) {
                dest_right->InsertUniq(key, value, hash, false, ...);
                invalid_mask |= (1u << slot);
            }
        });
        bucket_[i].ClearSlots(invalid_mask);
    }

    // Stash 桶同理：属于新段的迁移，留在原段的尝试搬回主桶
    ...
}
```

### 4.4 完整扩容演进示例

从一个空的 DashTable (`capacity_log=0`) 开始，逐步插入直到触发多次 Split：

```
阶段 1：初始状态
  global_depth_ = 0, 目录大小 = 1
  0 → Seg_0 (local_depth=0)

阶段 2：Seg_0 满了，需要 Split
  但 local_depth_ == global_depth_ == 0，先 IncreaseDepth(1)
  global_depth_ = 1, 目录大小 = 2
  0 → Seg_0   1 → Seg_0  (共享)
  Split Seg_0: local_depth_ → 1
  0 → Seg_0   1 → Seg_1

阶段 3：Seg_0 又满了
  local_depth_(1) == global_depth_(1)，先 IncreaseDepth(2)
  global_depth_ = 2, 目录大小 = 4
  00 → Seg_0   01 → Seg_0
  10 → Seg_1   11 → Seg_1
  Split Seg_0: local_depth_ → 2
  00 → Seg_0   01 → Seg_2
  10 → Seg_1   11 → Seg_1

阶段 4：Seg_1 满了
  local_depth_(1) < global_depth_(2), 直接 Split, 不需要 IncreaseDepth
  Split Seg_1: local_depth_ → 2
  00 → Seg_0   01 → Seg_2
  10 → Seg_1   11 → Seg_3

阶段 5：继续插入，某个 Seg 满了...
  local_depth_ == global_depth_ == 2，IncreaseDepth(3)
  global_depth_ = 3, 目录大小 = 8
  000 001 → Seg_0   010 011 → Seg_2
  100 101 → Seg_1   110 111 → Seg_3
  Split: local_depth_ → 3，分裂出 Seg_4
  000 → Seg_0   001 → Seg_4
  ...
```

**关键代码**（确定 Segment ID）：

```cpp
// dash_table.hpp
uint32_t SegmentId(size_t hash) const {
    if (global_depth_) {
        return hash >> (64 - global_depth_);  // 取高 global_depth_ 位
    }
    return 0;
}

// 确定逻辑段在目录中的下一跳（跳过共享同一物理段的连续槽位）
size_t NextSeg(size_t sid) const {
    size_t delta = (1u << (global_depth_ - segment_[sid]->local_depth()));
    return sid + delta;
}
```

### 4.5 IncreaseDepth — 目录扩展

```cpp
// dash_table.hpp  IncreaseDepth 源码注释
/*
    原始目录 (global_depth_=2):
      00 -> 逻辑段1 -> 物理段1
      01 -> 逻辑段2 -> 物理段2
      10 -> 逻辑段3 -> 物理段3
      11 -> 逻辑段4 -> 物理段4

    IncreaseDepth(3) 后:
      repl_cnt = 2^(3-2) = 2  表示每个物理段被2个逻辑段引用

      000 -> 逻辑段1 -> 物理段1
      001 -> 逻辑段2 -> 物理段1  ← 新增
      010 -> 逻辑段3 -> 物理段2
      011 -> 逻辑段4 -> 物理段2  ← 新增
      100 -> 逻辑段5 -> 物理段3
      101 -> 逻辑段6 -> 物理段3  ← 新增
      110 -> 逻辑段7 -> 物理段4
      111 -> 逻辑段8 -> 物理段4  ← 新增
*/
```

从后往前填充，避免覆盖未处理的数据：

```cpp
for (int i = prev_sz - 1; i >= 0; --i) {         // 从后往前
    size_t offs = i * repl_cnt;
    std::fill(segment_.begin() + offs,            // 连续 repl_cnt 个槽
              segment_.begin() + offs + repl_cnt, // 都指向同一个物理段
              segment_[i]);
}
```

---

## 5. 插入流程

`InsertUniq` 实现了四级递进的插入策略：

### 5.1 完整流程

```
InsertUniq(key, value, key_hash, spread=true):
  │
  │  bid = HomeIndex(key_hash)        // hash >> 8 % 64
  │  nid = NextBid(bid)               // 邻居桶: bid+1 (63 的下一个是 0)
  │  meta_hash = key_hash & 0xFF      // 指纹
  │
  ├─[第1级] 均衡插入 (spread=true)
  │   比较 target(bid) 和 neighbor(nid) 的空闲槽位数
  │   选空闲更多的桶插入
  │   → 成功: 返回
  │
  ├─[第2级] 位移 (Displacement)
  │   尝试 MoveToOther(true, nid, NextBid(nid))
  │   将 nid 桶中一个"属于自己"的条目移到 nid+1
  │   腾出的位置插入新 Key
  │   → 成功: 返回
  │
  │   尝试 MoveToOther(false, bid, PrevBid(bid))
  │   将 bid 桶中一个"来自邻居"的条目移回前一个桶
  │   → 成功: 返回
  │
  ├─[第3级] Stash 溢出
  │   遍历 4 个 Stash 桶:
  │     stash_pos = (bid + i) % 4
  │     插入到 bucket_[64 + stash_pos]
  │     调用 SetStashPtr 在主桶/邻居桶建立反向引用
  │   → 成功: 返回
  │
  └─[第4级] 返回空 Iterator → 触发 Segment Split
```

### 5.2 源码注释关键点

```cpp
// HomeIndex: 计算主桶位置
static LogicalBid HomeIndex(Hash_t hash) {
    return (hash >> kFingerBits) % kBucketNum;   // hash >> 8, 取模 64
}

// NextBid: 线性探测下一个桶（环形）
static LogicalBid NextBid(LogicalBid bid) {
    return bid < kBucketNum - 1 ? bid + 1 : 0;    // 63 → 0
}

// InsertUniq 参数说明
Iterator InsertUniq(U&& key, V&& value, Hash_t key_hash,
    bool spread,  /*
        spread true:  选择负载较小的桶（主桶或邻居桶）做均衡插入
        spread false: 优先选择主桶
    */
    OnMoveCb&& on_move_cb);  // 条目移动时的回调（用于通知淘汰策略）
```

### 5.3 MoveToOther — 位移操作细节

```cpp
// MoveToOther 参数说明
int MoveToOther(
    bool own_items,  /*
        true:  移动自己的条目（非探测槽位，即属于 from_bid 本身的）
        false: 移动别人的条目（探测槽位，即通过 probe 暂存在 from_bid 的）
    */
    unsigned from_bid,
    unsigned to_bid); /*
        桶满时将一个条目从当前桶移动到另一个桶，为新条目腾出空间
    */

// 源码逻辑
auto& src = bucket_[from_bid];
uint32_t mask = src.GetProbe(!own_items);  // 找到可移动的条目
int src_slot = __builtin_ctz(mask);        // 取第一个匹配的槽位

// 尝试插入到目标桶
int dst_slot = bucket_[to_bid].TryInsertToBucket(
    src.key[src_slot], src.value[src_slot], src.Fp(src_slot), own_items);

if (dst_slot >= 0) {
    src.Delete(src_slot);  // 从原桶删除
    return src_slot;       // 返回腾出的槽位
}
```

---

## 6. 查找流程

### 6.1 完整路径

```
FindIt(key_hash, pred):
  │
  │  bid = HomeIndex(key_hash)          // 主桶索引
  │  fp_hash = key_hash & 0xFF          // 指纹
  │
  ├─[1] 查主桶
  │    target = bucket_[bid]
  │    sid = target.FindByFp(fp_hash, probe=false, pred)
  │    // FindByFp 内部：
  │    //   mask = CompareFP(fp_hash) & GetBusy() & GetProbe(false)
  │    //   指纹匹配 + 槽位占用 + 非探测条目
  │    //   逐槽调用 pred(key) 做完整比较
  │    → 找到: 返回 Iterator(bid, sid)
  │
  ├─[2] 查邻居桶
  │    nid = NextBid(bid)
  │    probe = bucket_[nid]
  │    sid = probe.FindByFp(fp_hash, probe=true, pred)
  │    // 查 probe=true 的条目（被均衡插入放到邻居桶的）
  │    → 找到: 返回 Iterator(nid, sid)
  │
  ├─[3] Stash 查找
  │    if !target.HasStash(): 返回空
  │
  │    if target.HasStashOverflow():  // 溢出计数 > 0
  │        遍历全部 4 个 Stash 桶的每一个槽位
  │        → 找到: 返回
  │
  │    else:  // 正常 Stash，目标明确
  │        IterateStash(fp_hash, false) 查主桶 Stash 引用
  │        IterateStash(fp_hash, true)  查邻居桶 Stash 引用
  │        → 找到: 返回
  │
  └─ 返回空 Iterator
```

### 6.2 为什么"最多查两个桶"

每个 Key 的"家"只有两个候选位置——主桶 `bid = hash >> 8 % 64` 和邻居桶 `nid = bid + 1`。均衡插入会在这两个桶之间选择，位移操作也严格限制在这两个桶的邻域内。所以查找永远只需要检查这两个桶（加上可能的 Stash），保证了 O(1) 的确定性查找。

---

## 7. Key/Value 存储格式：CompactObj

ImDragonfly 使用 Tagged Union 存储 Key 和 Value，避免为每种类型分配独立内存：

```cpp
// compact_obj.hpp
enum Tag : uint8_t {
    EMPTY = 0,
    INT_TAG = 1,      // int64_t 内联存储
    STR_TAG = 2,      // std::string
    ROBJ_TAG = 3,     // 复杂对象指针（List/Hash/Set/ZSet）
    TTL_STR_TAG = 4,  // 带过期时间的字符串
};

union CompactU {
    int64_t ival_;
    std::string str_;
    TtlString ttl_;    // {std::string val; uint64_t exp_ms;}
    Robj robj_;        // {void* ptr; CompactObjType type;}
};
```

**Key 存储 (CompactKey)**：支持带过期标记的字符串，通过 `TTL_STR_TAG` 存 `{value, expire_ms}` 对。

**Value 存储 (CompactValue)**：小整数和短字符串内联在 Bucket slot 内。复杂对象（List/Hash/Set/ZSet）使用 mimalloc 分配并用 `ROBJ_TAG` 引用，析构时自动释放。

---

## 8. Segment 遍历：TraverseLogicalBucket

遍历一个"逻辑桶"的所有条目——包括分散在主桶、邻居桶和 Stash 桶中的：

```cpp
bool Segment::TraverseLogicalBucket(LogicalBid bid, HashFn&& hfun, Cb&& cb) const {
    const Bucket& b = bucket_[bid];

    // 1. 主桶中属于 bid 的条目（probe=false）
    if (b.GetProbe(false)) {
        b.ForEachSlot([&](auto* bucket, SlotId slot, bool probe) {
            if (!probe) cb(Iterator{bid, slot});
        });
    }

    // 2. 邻居桶中属于 bid 的条目（probe=true，说明是从 bid 溢出过去的）
    const Bucket& next = GetBucket(NextBid(bid));
    if (next.GetProbe(true)) {
        next.ForEachSlot([&](auto* bucket, SlotId slot, bool probe) {
            if (probe && HomeIndex(hfun(bucket->key[slot])) == bid)
                cb(Iterator{NextBid(bid), slot});
        });
    }

    // 3. Stash 桶中属于 bid 的条目
    if (b.HasStash()) {
        for (uint8_t j = kBucketNum; j < kTotalBuckets; ++j) {
            stashb.ForEachSlot([&](auto* bucket, SlotId slot, ...) {
                if (HomeIndex(hfun(bucket->key[slot])) == bid)
                    cb(Iterator{j, slot});
            });
        }
    }
}
```

---

## 参考文献

1. Lu, B., Hao, X., Wang, T., & Lo, E. (2020). **Dash: Scalable Hashing on Persistent Memory**. *Proceedings of the VLDB Endowment*, 13(8), 1147-1161. [arXiv:2003.07302](https://arxiv.org/abs/2003.07302)

2. DragonflyDB. [GitHub Repository](https://github.com/dragonflydb/dragonfly). BSD License.

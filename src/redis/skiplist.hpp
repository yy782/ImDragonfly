#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>
#include <random>
#include <vector>

// ============================================================================
// 跳表（SkipList）原理图解
// ============================================================================
//
// 跳表 = 多层链表。底层（Level 0）存所有节点，上层是"快速通道"跳过中间节点。
// 查找时从最高层往右走，走不动就下降一层，像走楼梯一样快速逼近目标。
//
// ── 数据结构 ──────────────────────────────────────────────────────────────
//
//   SkipListBase          ← 哨兵基类（header_ 用，不存 data）
//   ├─ forward: vector<Base*>   长度 = kMaxLevel = 32
//   └─ span:    vector<unsigned> 长度 = 32
//
//   SkipListNode<T> : Base   ← 数据节点（继承 Base + 加 data）
//   └─ data: T
//
//   header_ : SkipListBase      ← 跳表唯一哨兵，固定 32 层
//   level_  : 当前实际用到的最高层（动态 1..32，初始 1）
//
// ── 一个例子 ─────────────────────────────────────────────────────────────
//
//   插入 1, 3, 5, 7, 9 后（每个节点的 level 由 RandomLevel 随机决定）：
//
//                        header_ (SkipListBase，不存 data)
//                        │
//                        │ forward[i] / span[i]
//                        │
//   Level 3 (i=3)  ──────┼────► 9 (SkipListNode，level=4)
//                        │      span=? (跨到底层第 5 个节点)
//                        │
//   Level 2 (i=2)  ──────┼────► 5 ──────► 9
//                        │      span=2    span=1
//                        │
//   Level 1 (i=1)  ──────┼────► 3 ─► 5 ─► 7 ─► 9
//                        │      span=1 ... （每跳 1 个）
//                        │
//   Level 0 (i=0)  ──────┼────► 1 ─► 3 ─► 5 ─► 7 ─► 9
//                               span=1  （底层每跳 span=1）
//
//   说明：
//   * header_ 是同一个节点，它在 32 层都有 forward/span 槽位。
//     图中"同一个 header_ 在每一层都有一个出口箭头"，不是 4 个不同的 header。
//   * 实际只用到 level_ = 4 层（当前跳表最高层），header_.forward[4..31] 都是
//   nullptr。
//   * 每个数据节点（1,3,5,7,9）是 SkipListNode<T>，level 由 RandomLevel 决定：
//       - 节点 1：level=1（只在 Level 0）
//       - 节点 3：level=2（在 Level 0,1）
//       - 节点 5：level=3（在 Level 0,1,2）
//       - 节点 7：level=2（在 Level 0,1）
//       - 节点 9：level=4（在 Level 0,1,2,3）
//
// ── span 的含义 ──────────────────────────────────────────────────────────
//
//   span[i] = "第 i 层这条出边跨过多少个 Level 0 节点"
//
//   例：header_ → 5 在 Level 2，跨过 1,3,5 三个底层节点，span=3。
//       5 → 9 在 Level 2，跨过 7,9 两个底层节点，span=2。
//
//   作用：Rank(x) = 从 header_ 沿高层走到 x，累加 span。无需遍历底层。
//         例：Rank(5) = header_.span[2] = 3（直接从 Level 2 一跳到位）
//         Rank(9) = header_.span[3] = 5（Level 3 一跳，跨过全部 5 个节点）
//
// ── 查找流程（找 7）──────────────────────────────────────────────────────
//
//   1. x=header_, i=3: forward[3]=9, 9>7 不走，下降到 i=2
//   2. x=header_, i=2: forward[2]=5, 5<7 走，x=5, rank+=span[2]=3
//   3. x=5,      i=1: forward[1]=7, 7>=7 不走，下降到 i=0
//   4. x=5,      i=0: forward[0]=7, 找到！返回 rank=3
//
//   只走 1 跳（header→5），O(log N)。
//
// ── 插入流程（插 6，假设 RandomLevel 给 level=2）──────────────────────────
//
//   1. 从最高层下降找插入点，记录每层最后一个 < 6 的节点到 update[i]
//   2. 新节点 6 的 forward[i] = update[i].forward[i]  （接上后面）
//   3. update[i].forward[i] = 6                       （前面接上 6）
//   4. 更新 span：新节点 span = update.span - (rank[0]-rank[i])
//                 update.span = (rank[0]-rank[i]) + 1
//
//   结果：6 挂在 Level 0,1 上，span 自动维护。
//
// ============================================================================

namespace dfly {

// 哨兵节点基类：只含 forward/span，不含 data。
// 数据节点 SkipListNode<T> 继承它并加 data 字段。
// 这样 header_ 和数据节点用同一个基类指针遍历，类型统一，
// data 字段只在数据节点访问，header 不涉及 data，无 UB。
struct SkipListBase {
  std::vector<SkipListBase*> forward;
  std::vector<unsigned> span;  // forward[i] 这一跳跨越的底层节点数

  explicit SkipListBase(int level) {
    forward.assign(level, nullptr);
    span.assign(level, 0);
  }
};

// 数据节点：继承 SkipListBase，加 data 字段。
// 内存布局：forward/span 在前，data 在后（继承保证基类先布局）。
template <typename T>
struct SkipListNode : public SkipListBase {
  T data;

  SkipListNode(int level, const T& d) : SkipListBase(level), data(d) {}
};

template <typename T, typename Compare = std::less<T>>
class SkipList {
 public:
  using Node = SkipListNode<T>;
  using Base = SkipListBase;

  // header_ 直接用基类 SkipListBase，不需要 T 默认构造，也不涉及 data。
  explicit SkipList(Compare comp = Compare())
      : comp_(comp),
        level_(1),
        length_(0),
        rng_(0x12345678),
        header_(kMaxLevel) {}

  ~SkipList() {
    Base* node = header_.forward[0];
    while (node != nullptr) {
      Base* next = node->forward[0];
      delete static_cast<Node*>(node);
      node = next;
    }
  }

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  bool Insert(const T& value) {
    Base* update[kMaxLevel];
    unsigned rank[kMaxLevel];
    Base* x = &header_;

    for (int i = level_ - 1; i >= 0; --i) {
      rank[i] = (i == level_ - 1) ? 0 : rank[i + 1];
      while (x->forward[i] != nullptr &&
             comp_(static_cast<Node*>(x->forward[i])->data, value)) {
        rank[i] += x->span[i];
        x = x->forward[i];
      }
      update[i] = x;
    }

    x = x->forward[0];
    if (x != nullptr && !comp_(value, static_cast<Node*>(x)->data) &&
        !comp_(static_cast<Node*>(x)->data, value)) {
      return false;
    }

    int new_level = RandomLevel();
    if (new_level > level_) {
      for (int i = level_; i < new_level; ++i) {
        rank[i] = 0;
        update[i] = &header_;
        update[i]->span[i] = static_cast<unsigned>(length_);
      }
      level_ = new_level;
    }

    Node* new_node = new Node(new_level, value);
    for (int i = 0; i < new_level; ++i) {
      new_node->forward[i] = update[i]->forward[i];
      update[i]->forward[i] = new_node;

      new_node->span[i] = update[i]->span[i] - (rank[0] - rank[i]);
      update[i]->span[i] = (rank[0] - rank[i]) + 1;
    }

    for (int i = new_level; i < level_; ++i) {
      ++update[i]->span[i];
    }

    ++length_;
    return true;
  }

  bool Remove(const T& value) {
    Base* update[kMaxLevel];
    Base* x = &header_;

    for (int i = level_ - 1; i >= 0; --i) {
      while (x->forward[i] != nullptr &&
             comp_(static_cast<Node*>(x->forward[i])->data, value)) {
        x = x->forward[i];
      }
      update[i] = x;
    }

    x = x->forward[0];
    if (x == nullptr || comp_(value, static_cast<Node*>(x)->data) ||
        comp_(static_cast<Node*>(x)->data, value)) {
      return false;
    }

    for (int i = 0; i < level_; ++i) {
      if (update[i]->forward[i] != x) break;
      update[i]->forward[i] = x->forward[i];
      update[i]->span[i] += x->span[i] - 1;
    }

    delete static_cast<Node*>(x);

    while (level_ > 1 && header_.forward[level_ - 1] == nullptr) {
      --level_;
    }

    --length_;
    return true;
  }

  std::optional<Node*> Find(const T& value) const {
    Base* x = &header_;
    for (int i = level_ - 1; i >= 0; --i) {
      while (x->forward[i] != nullptr &&
             comp_(static_cast<Node*>(x->forward[i])->data, value)) {
        x = x->forward[i];
      }
    }
    x = x->forward[0];
    if (x != nullptr && !comp_(value, static_cast<Node*>(x)->data) &&
        !comp_(static_cast<Node*>(x)->data, value)) {
      return static_cast<Node*>(x);
    }
    return std::nullopt;
  }

  int64_t Rank(const T& value) const {
    int64_t rank = 0;
    Base* x = &header_;
    for (int i = level_ - 1; i >= 0; --i) {
      while (x->forward[i] != nullptr &&
             comp_(static_cast<Node*>(x->forward[i])->data, value)) {
        rank += x->span[i];
        x = x->forward[i];
      }
    }
    x = x->forward[0];
    if (x != nullptr && !comp_(value, static_cast<Node*>(x)->data) &&
        !comp_(static_cast<Node*>(x)->data, value)) {
      return rank;
    }
    return -1;
  }

  std::optional<Node*> GetByRank(int64_t rank) const {
    if (rank < 0 || static_cast<size_t>(rank) >= length_) {
      return std::nullopt;
    }
    int64_t target = rank + 1;
    int64_t traversed = 0;
    Base* x = &header_;
    for (int i = level_ - 1; i >= 0; --i) {
      while (x->forward[i] != nullptr && traversed + x->span[i] <= target) {
        traversed += x->span[i];
        x = x->forward[i];
      }
    }
    if (x == &header_) return std::nullopt;
    return static_cast<Node*>(x);
  }

  std::vector<Node*> Range(int64_t start, int64_t end) const {
    std::vector<Node*> result;
    if (length_ == 0) return result;

    int64_t s = NormalizeIndex(start);
    int64_t e = NormalizeIndex(end);
    if (s < 0 || s >= static_cast<int64_t>(length_) || s > e) return result;
    if (e >= static_cast<int64_t>(length_))
      e = static_cast<int64_t>(length_) - 1;

    auto start_node = GetByRank(s);
    if (!start_node) return result;
    Node* x = *start_node;
    for (int64_t i = s; i <= e; ++i) {
      result.push_back(x);
      x = static_cast<Node*>(x->forward[0]);
    }
    return result;
  }

  std::vector<Node*> RevRange(int64_t start, int64_t end) const {
    std::vector<Node*> result;
    if (length_ == 0) return result;

    int64_t s = NormalizeIndex(start);
    int64_t e = NormalizeIndex(end);
    if (s < 0 || s >= static_cast<int64_t>(length_) || s > e) return result;
    if (e >= static_cast<int64_t>(length_))
      e = static_cast<int64_t>(length_) - 1;

    auto start_node = GetByRank(s);
    if (!start_node) return result;
    Node* x = *start_node;
    for (int64_t i = s; i <= e; ++i) {
      result.push_back(x);
      x = static_cast<Node*>(x->forward[0]);
    }
    std::reverse(result.begin(), result.end());
    return result;
  }

  size_t Length() const { return length_; }
  bool Empty() const { return length_ == 0; }

 private:
  static constexpr int kMaxLevel = 32;
  static constexpr int kBranchingBits = 2;  // 1/4 概率

  int64_t NormalizeIndex(int64_t idx) const {
    return idx < 0 ? idx + static_cast<int64_t>(length_) : idx;
  }

  int RandomLevel() {
    int level = 1;
    while ((rng_() & ((1u << kBranchingBits) - 1)) == 0 && level < kMaxLevel) {
      ++level;
    }
    return level;
  }

  Compare comp_;
  int level_;
  size_t length_;
  std::mt19937 rng_;
  mutable SkipListBase header_;
};

}  // namespace dfly

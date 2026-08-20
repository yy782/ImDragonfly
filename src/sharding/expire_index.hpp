// ExpireIndex —— 过期索引：按绝对到期时间（ms）组织的小顶堆，伴生于
// DbTable 键空间，是"过期"这一事实的唯一真相源。
//
// 设计思想：
//   * AddExpire：O(log n) 入堆。同一键的 TTL 被覆盖时直接再入一条新记录，
//     旧记录不主动删除——残留条目待其浮到堆顶时做"延迟验证"淘汰。
//   * RemoveExpire：O(1)。只清键上的 TTL（由上层在键空间内完成），索引
//     不做精确删除，残留条目弹出时验证淘汰。
//   * 验证规则（弹出时，由上层回调完成，因为只有上层能访问键空间）：
//       - 键不存在           -> 残留（已被删除），丢弃
//       - 键已无 TTL         -> 残留（TTL 被移除），丢弃
//       - 键 TTL != 记录 exp -> 残留（TTL 被覆盖，新记录已入堆），丢弃
//       - 键 TTL == 记录 exp -> 有效，且 exp <= now（堆序保证），删除
//     由于 exp 是绝对时间戳，记录 TTL 与键当前 TTL 相等即同一到期时刻，
//     不会误删"被重新设置过但恰好同时刻到期"的键。
//   * 内存：每次 AddExpire 至多留下一条堆内记录；残留记录在弹出时自然
//     回收。反复 SET/EXPIRE 的键，堆内至多积压其历史 TTL 条数（O(1) 级）。
//   * 遍历方向：从论文《Dash: Scalable Hashing on Persistent Memory》
//     (Lu et al., VLDB 2020) 的"先落数据、后发布提交点"思想引申——
//     TTL 是延迟提交的元数据，堆顶就是最近提交点，逐条弹出即可。

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace dfly {

class ExpireIndex {
 public:
  struct Entry {
    uint64_t exp_ms = 0;  // 绝对到期时间（ms since epoch）
    uint64_t hash = 0;    // 键哈希（与键空间 HashFn 一致），用于快速重查
    std::string key;      // 键拷贝，生命周期独立于键空间
  };

  bool Empty() const { return heap_.empty(); }
  size_t Size() const { return heap_.size(); }

  // 当前最早到期时刻；空索引返回 UINT64_MAX。
  uint64_t MinExpireTime() const {
    return heap_.empty() ? UINT64_MAX : heap_.front().exp_ms;
  }

  // O(log n)：入堆。同一键的新 TTL 直接再入一条，旧条目弹出时淘汰。
  void AddExpire(std::string_view key, uint64_t hash, uint64_t exp_ms);

  // 弹出堆顶（最小 exp_ms）记录。
  Entry Pop();

  // 弹出所有 exp_ms <= now_ms 的记录，逐条交给 on_entry。
  // on_entry 返回 true 表示已从键空间删除（计入返回值），false 表示残留
  // 记录（键已删 / TTL 已移除 / TTL 已覆盖），仅丢弃不计。
  // 注意：回调内禁止再对本索引做 Add/Pop（会破坏堆序）；验证与删除在
  // 键空间内完成。
  template <typename F>
  size_t PopExpired(uint64_t now_ms, F&& on_entry) {
    size_t removed = 0;
    while (!heap_.empty() && heap_.front().exp_ms <= now_ms) {
      Entry e = Pop();
      if (on_entry(e)) ++removed;
    }
    return removed;
  }

  void Reserve(size_t n) { heap_.reserve(n); }
  void Clear() { heap_.clear(); }

  // 近似内存：堆容量 × 条目大小 + 各键串容量（不含 std::string 自身头）。
  size_t MemoryUsage() const {
    size_t s = heap_.capacity() * sizeof(Entry);
    for (const Entry& e : heap_) s += e.key.capacity();
    return s;
  }

 private:
  void SiftUp(size_t pos);
  void SiftDown(size_t pos);

  std::vector<Entry> heap_;
};

}  // namespace dfly

#pragma once
// SPSC 分片段队列
#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <memory_resource>
#include <new>
#include <type_traits>
#include <utility>

#include "detail/common_types.hpp"

namespace dfly {

#if defined(__GNUC__) || defined(__clang__)
inline void PrefetchRead(const void* addr) { __builtin_prefetch(addr, 0, 1); }
#else
inline void PrefetchRead(const void* addr) { (void)addr; }
#endif

// SPSC 分片段队列：每个队列（main 队列 / 分片 j 的队列）只有一个环形缓冲，
// 缓冲按"生产者分片"划分为 shard_num 段，每段都是单写单读（SPSC）。
//
//   capacity  = 环形缓冲总容量（2 的幂），整个队列只分配这一份内存；
//   shard_num = 段数（= 分片数 h，2 的幂）；
//   段 i 的槽位范围 = [i * seg_cap, (i + 1) * seg_cap)，
//   seg_cap = capacity / shard_num（两个 2 的幂相除，天然整除）。
//
// 段 i 是"生产者 i"的专属写段，读端固定为队列主人：
//   - main 队列：段 i 写者 = main（TryPostFromMain(i)，广播时
//   TryBroadcastFromMain
//     写所有段）；读者 = 分片 i（TryDrainSeg(max, 自身分片 id)）。
//   - 分片 j 队列：段 i（i != j）写者 = 分片 i 线程（PostShard(j, i, ...)
//   分片间投递）；
//     段 j（own 段）写者 = main（TryAddForSeg(j) / TryAddForAllSeg）；
//     所有段读者 = 分片 j（TryDrain 遍历自身队列全部段）。
//
// 每段都是单写单读（SPSC），全程无 CAS：
//   - head：写位置，仅对应生产者线程更新（relaxed 自读，release 发布数据）；
//   - tail：读位置，仅队列主人线程更新（relaxed 自读，release 发布空间）；
//   - 生产者 acquire 读 tail 判满，消费者 acquire 读 head 判空。
//
// 线程约束（调用方约定，Enqueue 仅断言 producer < shard_num）：
//   - TryAdd(producer, ...) 仅由分片 producer 的线程调用（分片间投递）；
//   - TryAddForSeg(seg, ...) 仅由 main 线程调用（投递单个分片）；
//   - TryAddForAllSeg(...)   仅由 main 线程调用（向所有分片广播）；
//   - TryDrain(max, seg)/Empty() 仅由对应段的主人线程调用。
template <typename T>
class spsc_shard_queue {
 public:
  spsc_shard_queue() = default;

  void Init(size_t capacity, size_t shard_num,
            std::pmr::memory_resource* mr = std::pmr::get_default_resource()) {
    assert(buffer_ == nullptr && "spsc_shard_queue: Init called twice");
    assert(capacity >= 2 && (capacity & (capacity - 1)) == 0 &&
           "spsc_shard_queue: capacity must be a power of two");
    assert(shard_num >= 1 && (shard_num & (shard_num - 1)) == 0 &&
           "spsc_shard_queue: shard_num must be a power of two");
    // assert(owner_id < shard_num && "spsc_shard_queue: owner id out of
    // range");
    capacity_ = capacity;
    shard_num_ = shard_num;
    mr_ = mr;
    seg_cap_ = capacity / shard_num;
    assert(seg_cap_ >= 2 && "spsc_shard_queue: per-segment capacity too small");
    buffer_ = static_cast<Slot*>(
        mr->allocate(capacity * sizeof(Slot), alignof(Slot)));
    segs_ = static_cast<SegmentCounters*>(mr->allocate(
        shard_num * sizeof(SegmentCounters), alignof(SegmentCounters)));
    for (size_t i = 0; i < shard_num; ++i) {
      ::new (static_cast<void*>(&segs_[i])) SegmentCounters();
      segs_[i].base = i * seg_cap_;  // 预计算段基址，热路径免乘法
    }
  }

  spsc_shard_queue(const spsc_shard_queue&) = delete;
  spsc_shard_queue& operator=(const spsc_shard_queue&) = delete;

  ~spsc_shard_queue() {
    if (buffer_ == nullptr) return;

    for (size_t i = 0; i < shard_num_; ++i) {
      size_t tail = segs_[i].tail.load(std::memory_order_relaxed);
      size_t head = segs_[i].head.load(std::memory_order_relaxed);
      size_t n = head - tail;
      for (size_t k = 0; k < n; ++k) {
        size_t j = tail + k;
        Slot& slot = buffer_[i * seg_cap_ + (j & (seg_cap_ - 1))];
        std::launder(reinterpret_cast<T*>(&slot.storage))->~T();
      }
    }
    mr_->deallocate(segs_, shard_num_ * sizeof(SegmentCounters),
                    alignof(SegmentCounters));
    mr_->deallocate(buffer_, capacity_ * sizeof(Slot), alignof(Slot));
    buffer_ = nullptr;
    segs_ = nullptr;
  }

  template <typename U>
  bool TryAdd(ShardId producer, U&& data) {
    assert(producer < shard_num_ && "spsc_shard_queue: producer out of range");
    return Enqueue(producer, std::forward<U>(data));
  }

  template <typename U>
  bool TryAddForAllSeg(
      U&& data) {  // 主线程调用，向自己的所有段都投放一个任务，用于广播所有分片
    static_assert(
        std::is_copy_constructible_v<std::decay_t<U>>,
        "TryAddForAllSeg: 广播要把同一任务复制到每个段，任务必须可拷贝");
    bool ok = true;
    for (size_t i = 0; i < shard_num_; ++i) {
      if (!Enqueue(i, data)) ok = false;
    }
    return ok;
  }

  template <typename U>
  bool TryAddForSeg(ShardId SegId, U&& data) {  // 主线程调用, 向特定段加任务
    return Enqueue(SegId, data);
  }

  bool TryDrain(uint32_t MaxTaskNum, ShardId SegId) {
    SegmentCounters& seg = segs_[SegId];
    size_t tail = seg.tail.load(std::memory_order_relaxed);
    size_t head_c = seg.head_cache;
    if (tail >= head_c) [[unlikely]] {
      // 缓存失效：回源读真实 head（acquire），并刷新本地缓存
      head_c = seg.head.load(std::memory_order_acquire);
      seg.head_cache = head_c;
      if (tail >= head_c) [[unlikely]]
        return false;
    }
    const size_t mask = seg_cap_ - 1;
    const size_t base = seg.base;
    size_t cnt = 0;
    // head 只回源一次：本次最多消费到快照 head_c，剩余留到下次 TryDrain
    // （外层 while(TryDrain) 驱动，语义不变）。循环内 tail 是本地变量，
    // 无原子读；逐槽 release 发布空间，生产者可及时续投。
    while (cnt < MaxTaskNum && tail < head_c) {
      Slot& slot = buffer_[base + (tail & mask)];
      T* p = std::launder(reinterpret_cast<T*>(&slot.storage));
      T item = std::move(*p);
      p->~T();
      seg.tail.store(tail + 1, std::memory_order_release);
      ++tail;
      ++cnt;
      PrefetchRead(&buffer_[base + (tail & mask)].storage);  // 预取下一槽
      item();
    }
    return cnt > 0;
  }

  bool Empty() const {
    for (size_t i = 0; i < shard_num_; ++i) {
      if (segs_[i].tail.load(std::memory_order_relaxed) !=
          segs_[i].head.load(std::memory_order_acquire)) {
        return false;
      }
    }
    return true;
  }

  ShardId ShardNum() const { return shard_num_; }
  size_t Capacity() const { return capacity_; }

 private:
  struct Slot {
    alignas(T) unsigned char storage[sizeof(T)];
  };

  // 每段 2 个缓存行，head 与 tail 分置，杜绝伪共享：
  //   head 行：head（生产者独占写）+ tail_cache/base（生产者本地）
  //   tail 行：tail（消费者独占写）+ head_cache（消费者本地）
  // 生产者写 head 与消费者写 tail 永不共享同一条缓存行；
  // tail_cache/head_cache 缓存对端位置，让判满/判空多数时候只碰本地行、
  // 不发 acquire 读，仅当缓存失效才回源（Vyukov SPSC 风格）。
  // sizeof = 128，段间同样 64B 对齐、互不共享缓存行。
  struct SegmentCounters {
    alignas(64) std::atomic<size_t> head{0};
    size_t tail_cache = 0;  // 生产者对 tail 的本地缓存（<= 真实 tail）
    size_t base = 0;        // 本段槽位基址 = seg_id * seg_cap_
    alignas(64) std::atomic<size_t> tail{0};
    size_t head_cache = 0;  // 消费者对 head 的本地缓存（<= 真实 head）
  };
  static_assert(alignof(SegmentCounters) == 64 &&
                    sizeof(SegmentCounters) == 128,
                "SegmentCounters: 必须恰好占 2 个 64B 缓存行");
  static_assert(offsetof(SegmentCounters, tail) == 64,
                "SegmentCounters: tail 必须从第二个缓存行开始");

  template <typename U>
  bool Enqueue(ShardId seg_id, U&& data) {
    assert(segs_ != nullptr &&
           "spsc_shard_queue: Init() must be called before enqueue");
    SegmentCounters& seg = segs_[seg_id];
    size_t head = seg.head.load(std::memory_order_relaxed);
    size_t tail_c = seg.tail_cache;
    if (head - tail_c >= seg_cap_) [[unlikely]] {
      // 缓存失效：回源读真实 tail（acquire），并刷新本地缓存
      tail_c = seg.tail.load(std::memory_order_acquire);
      seg.tail_cache = tail_c;
      if (head - tail_c >= seg_cap_) [[unlikely]]
        return false;
    }
    Slot& slot = buffer_[seg.base + (head & (seg_cap_ - 1))];
    ::new (static_cast<void*>(&slot.storage)) T(std::forward<U>(data));
    seg.head.store(head + 1, std::memory_order_release);
    return true;
  }

  Slot* buffer_ = nullptr;
  SegmentCounters* segs_ = nullptr;
  size_t capacity_ = 0;
  size_t seg_cap_ = 0;
  size_t shard_num_ = 0;
  std::pmr::memory_resource* mr_ = std::pmr::get_default_resource();
};

}  // namespace dfly

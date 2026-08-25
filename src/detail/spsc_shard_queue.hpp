#pragma once

#include <cassert>
#include <cstddef>
#include <memory>
#include <memory_resource>
#include <new>
#include <utility>

#include "detail/common_types.hpp"

namespace dfly {

// SPSC 分片段队列：一个分片（属主）只有一个环形队列，环形缓冲按段划分。
//
//   capacity  = 环形缓冲总容量（2 的幂），整个队列只分配这一份内存；
//   shard_num = 段数（= 分片数 h，2 的幂）；
//   段 i 的槽位范围 = [i * seg_cap, (i + 1) * seg_cap)，
//   seg_cap = capacity / shard_num（两个 2 的幂相除，天然整除）。
//
//   段 i（i != owner_id） ：写者 = 分片 i 线程（仅分片间投递）
//   段 owner_id（main 段）：写者 = main 线程（main 向本分片投递）
//
// 每段都是单写单读（SPSC），全程无 CAS：
//   - head：写位置，仅对应生产者线程更新（relaxed 自读，release 发布数据）；
//   - tail：读位置，仅属主消费者线程更新（relaxed 自读，release 发布空间）；
//   - 生产者 acquire 读 tail 判满，消费者 acquire 读 head 判空。
//
// 线程约束：
//   - TryAdd(producer, ...) 仅由分片 producer 的线程调用；
//   - 分片不能投递给自己：producer == owner_id 会被断言拦截
//     （段 owner_id 是 main 的段，分片 j 不写段 j）；
//   - TryAddFromMain(...) 仅由 main 线程调用；
//   - TryDrain()/Empty() 仅由属主分片线程调用。
template <typename T>
class spsc_shard_queue {
 public:
  // 每段单轮最大消费数：限制单段消费量，轮流遍历各段防止饿死其他生产者
  //（md 要求 1：设置一次消费一个分片任务队列的上限，达到上限后遍历别的段）。
  static constexpr size_t kMaxPerSegment = 64;

  // 默认构造：空队列，需 Init() 之后才能使用。TaskQueue 以栈成员持有本
  // 队列，容量 / 分片数在 ShardPool::Init 时才确定，故延迟初始化。
  spsc_shard_queue() = default;

  // capacity: 环形缓冲总容量（2 的幂）；owner_id: 属主分片 id；
  // shard_num: 段数（= 分片数 h，2 的幂）。只允许在空状态调用一次。
  void Init(size_t capacity, ShardId owner_id, size_t shard_num,
            std::pmr::memory_resource* mr =
                std::pmr::get_default_resource()) {
    assert(buffer_ == nullptr && "spsc_shard_queue: Init called twice");
    assert(capacity >= 2 && (capacity & (capacity - 1)) == 0 &&
           "spsc_shard_queue: capacity must be a power of two");
    assert(shard_num >= 1 && (shard_num & (shard_num - 1)) == 0 &&
           "spsc_shard_queue: shard_num must be a power of two");
    assert(owner_id < shard_num && "spsc_shard_queue: owner id out of range");
    capacity_ = capacity;
    shard_num_ = shard_num;
    owner_id_ = owner_id;
    mr_ = mr;
    seg_cap_ = capacity / shard_num;
    assert(seg_cap_ >= 2 && "spsc_shard_queue: per-segment capacity too small");
    buffer_ = static_cast<Slot*>(
        mr->allocate(capacity * sizeof(Slot), alignof(Slot)));
    segs_ = static_cast<SegmentCounters*>(
        mr->allocate(shard_num * sizeof(SegmentCounters),
                     alignof(SegmentCounters)));
    for (size_t i = 0; i < shard_num; ++i) {
      ::new (static_cast<void*>(&segs_[i])) SegmentCounters();
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
    assert(producer != owner_id_ &&
           "spsc_shard_queue: 分片不能投递给自己（段 owner_id 是 main 的段）");
    return Enqueue(producer, std::forward<U>(data));
  }


  template <typename U>
  bool TryAddFromMain(U&& data) {
    return Enqueue(owner_id_, std::forward<U>(data));
  }

  bool TryDrain() {
    size_t total = 0;
    bool progress = false;
    do {
      progress = false;
      for (size_t i = 0; i < shard_num_; ++i) {
        SegmentCounters& seg = segs_[i];
        size_t cnt = 0;
        while (cnt < kMaxPerSegment) {
          size_t tail = seg.tail.load(std::memory_order_relaxed);
          if (tail >= seg.head.load(std::memory_order_acquire)) break;  
          Slot& slot = buffer_[i * seg_cap_ + (tail & (seg_cap_ - 1))];
          T* p = std::launder(reinterpret_cast<T*>(&slot.storage));
          T item = std::move(*p);
          p->~T();
          seg.tail.store(tail + 1, std::memory_order_release);
          item();  
          ++cnt;
        }
        if (cnt != 0) {
          total += cnt;
          progress = true;
        }
      }
    } while (progress);
    return total > 0;
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

 private:
  struct Slot {
    alignas(T) unsigned char storage[sizeof(T)];
  };

  struct alignas(64) SegmentCounters {
    std::atomic<size_t> head{0}; 
    std::atomic<size_t> tail{0}; 
  };

  template <typename U>
  bool Enqueue(ShardId seg_id, U&& data) {
    assert(segs_ != nullptr &&
           "spsc_shard_queue: Init() must be called before enqueue");
    SegmentCounters& seg = segs_[seg_id];
    size_t head = seg.head.load(std::memory_order_relaxed);
    if (head - seg.tail.load(std::memory_order_acquire) >= seg_cap_) {
      return false; 
    }
    size_t slot = seg_id * seg_cap_ + (head & (seg_cap_ - 1));
    ::new (static_cast<void*>(&buffer_[slot].storage)) T(std::forward<U>(data));
    seg.head.store(head + 1, std::memory_order_release);
    return true;
  }

  Slot* buffer_ = nullptr;
  SegmentCounters* segs_ = nullptr;
  ShardId owner_id_ = 0;
  size_t capacity_ = 0;
  size_t seg_cap_ = 0;
  size_t shard_num_ = 0;
  std::pmr::memory_resource* mr_ = std::pmr::get_default_resource();
};

}  // namespace dfly

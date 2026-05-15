#pragma once

#include <cstdint>
#include <functional>
#include <variant>
#include <vector>

namespace dfly {

class Transaction;

class TxQueue {
  void Link(uint32_t p, uint32_t n) {
    uint32_t next = vec_[p].next;
    vec_[n].next = next;
    vec_[n].prev = p;
    vec_[p].next = n;
    vec_[next].prev = n;
  }

 public:
  using ValueType = std::variant<Transaction*, uint64_t>;
  using Iterator = uint32_t;
  enum { kEnd = Iterator(-1) };

  TxQueue(std::function<uint64_t(const Transaction*)> score_fun = nullptr)
      : score_fun_(std::move(score_fun)) {}

  Iterator Insert(Transaction* t);
  Iterator Insert(uint64_t val);
  void Remove(Iterator it);

  ValueType At(Iterator it) const {
    switch (vec_[it].tag) {
      case TRANS_TAG:
        return vec_[it].u.trans;
      case UINT_TAG:
        return vec_[it].u.uval;
    }
    return 0u;
  }

  ValueType Front() const {
    return At(head_);
  }

  void PopFront() {
    Remove(head_);
  }

  size_t size() const {
    return size_;
  }

  bool Empty() const {
    return size_ == 0;
  }

  uint64_t TailScore() const {
    return Rank(vec_[vec_[head_].prev]);
  }

  uint64_t HeadScore() const {
    return Rank(vec_[head_]);
  }

  Iterator Head() const {
    return head_;
  }

  Iterator Next(Iterator it) const {
    return vec_[it].next;
  }

 private:
  enum { TRANS_TAG = 0, UINT_TAG = 11, FREE_TAG = 12 };

  void Grow();
  void LinkFree(uint64_t rank);

  struct QRecord {
    union {
      Transaction* trans;
      uint64_t uval;
    } u;

    uint32_t tag : 8;
    uint32_t next : 24;
    uint32_t prev;

    QRecord() : tag(FREE_TAG), prev(kEnd) {}
  };

  static_assert(sizeof(QRecord) == 16, "");

  uint64_t Rank(const QRecord& r) const;

  std::function<uint64_t(const Transaction*)> score_fun_;
  std::vector<QRecord> vec_;
  uint32_t next_free_ = 0, head_ = kEnd;
  size_t size_ = 0;

  TxQueue(const TxQueue&) = delete;
};

inline uint64_t TxQueue::Rank(const QRecord& r) const {
  if (r.tag == TRANS_TAG && score_fun_) {
    return score_fun_(r.u.trans);
  }
  return r.u.uval;
}

inline void TxQueue::Grow() {
  size_t old_size = vec_.size();
  vec_.resize(old_size * 2);
  for (size_t i = old_size; i < vec_.size() - 1; ++i) {
    vec_[i].next = i + 1;
    vec_[i].prev = kEnd;
  }
  vec_.back().next = next_free_;
  next_free_ = old_size;
}

inline void TxQueue::LinkFree(uint64_t) {}

inline TxQueue::Iterator TxQueue::Insert(Transaction* t) {
  if (next_free_ == kEnd) {
    Grow();
  }

  uint32_t idx = next_free_;
  next_free_ = vec_[idx].next;

  vec_[idx].u.trans = t;
  vec_[idx].tag = TRANS_TAG;

  if (head_ == kEnd) {
    vec_[idx].next = idx;
    vec_[idx].prev = idx;
    head_ = idx;
  } else {
    Link(vec_[head_].prev, idx);
  }

  ++size_;
  return idx;
}

inline TxQueue::Iterator TxQueue::Insert(uint64_t val) {
  if (next_free_ == kEnd) {
    Grow();
  }

  uint32_t idx = next_free_;
  next_free_ = vec_[idx].next;

  vec_[idx].u.uval = val;
  vec_[idx].tag = UINT_TAG;

  if (head_ == kEnd) {
    vec_[idx].next = idx;
    vec_[idx].prev = idx;
    head_ = idx;
  } else {
    Link(vec_[head_].prev, idx);
  }

  ++size_;
  return idx;
}

inline void TxQueue::Remove(Iterator it) {
  if (vec_[it].tag == FREE_TAG)
    return;

  uint32_t prev = vec_[it].prev;
  uint32_t next = vec_[it].next;

  vec_[prev].next = next;
  vec_[next].prev = prev;

  if (head_ == it) {
    head_ = (size_ == 1) ? kEnd : next;
  }

  vec_[it].tag = FREE_TAG;
  vec_[it].next = next_free_;
  next_free_ = it;

  --size_;
}

}  // namespace dfly
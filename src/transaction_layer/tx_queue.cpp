#include "core/tx_queue.h"

namespace dfly {

void TxQueue::Grow() {
  uint32_t old_cap = vec_.size();
  uint32_t new_cap = old_cap * 2;
  if (new_cap == 0)
    new_cap = 8;

  vec_.resize(new_cap);
  for (uint32_t i = old_cap; i + 1 < new_cap; i++) {
    vec_[i].tag = FREE_TAG;
    vec_[i].next = i + 1;
  }
  vec_[new_cap - 1].tag = FREE_TAG;
  vec_[new_cap - 1].next = kEnd;
  next_free_ = old_cap;
}

uint64_t TxQueue::Rank(const QRecord& r) const {
  if (r.tag == UINT_TAG)
    return r.u.uval;
  return score_fun_(r.u.trans);
}

void TxQueue::LinkFree(uint64_t rank) {
  if (size_ == 0) {
    head_ = next_free_;
  } else {
    uint32_t curr = head_;
    while (Rank(vec_[curr]) <= rank) {
      if (vec_[curr].next == head_) {
        break;
      }
      curr = vec_[curr].next;
    }

    if (Rank(vec_[curr]) <= rank) {
      Link(curr, next_free_);
    } else {
      Link(vec_[curr].prev, next_free_);
      head_ = next_free_;
    }
  }

  uint32_t free_next = vec_[next_free_].next;
  Link(next_free_, head_);
  next_free_ = free_next;
}

TxQueue::Iterator TxQueue::Insert(Transaction* t) {
  if (next_free_ == kEnd)
    Grow();

  uint32_t res = next_free_;
  vec_[res].u.trans = t;
  vec_[res].tag = TRANS_TAG;
  LinkFree(score_fun_(t));
  size_++;
  return res;
}

TxQueue::Iterator TxQueue::Insert(uint64_t val) {
  if (next_free_ == kEnd)
    Grow();

  uint32_t res = next_free_;
  vec_[res].u.uval = val;
  vec_[res].tag = UINT_TAG;
  LinkFree(val);
  size_++;
  return res;
}

void TxQueue::Remove(Iterator it) {
  uint32_t prev = vec_[it].prev;
  uint32_t next = vec_[it].next;

  vec_[prev].next = next;
  vec_[next].prev = prev;

  if (head_ == it) {
    head_ = (next == it) ? kEnd : next;
  }

  vec_[it].tag = FREE_TAG;
  vec_[it].next = next_free_;
  next_free_ = it;
  size_--;
}

}  // namespace dfly

#include "tx_queue.hpp"

#include <cassert>
#include "transaction_layer/transaction.hpp"
namespace dfly {

void TxQueue::Grow() {
    uint32_t old_size = vec_.size();
    uint32_t new_size = old_size == 0 ? 16 : old_size * 2;
    vec_.resize(new_size);

    for (uint32_t i = old_size; i <= new_size - 1; ++i) {
      vec_[i].next = i + 1;
      vec_[i].prev = i - 1;
    }
    vec_[new_size - 1].next = kEnd;
}

TxQueue::Iterator TxQueue::AllocateNode() {
    if (free_head_ == kEnd) {
      Grow();
    }

    uint32_t idx = free_head_;
    free_head_ = vec_[idx].next;
    vec_[idx].next = kEnd;
    vec_[idx].prev = kEnd;
    return idx;
}

TxQueue::Iterator TxQueue::Push(Transaction* t) {
  Iterator new_node = AllocateNode();
  vec_[new_node].trans = t;
  Iterator it = tail_;
  while (it != kEnd) {
    if (vec_[it].trans->txid() <= t->txid()) {
      break;
    }
    it = vec_[it].prev;
  }
  if (it == kEnd) {
    if (head_ == kEnd) {
      head_ = new_node;
      tail_ = new_node;
    } else {
      vec_[head_].prev = new_node;
      vec_[new_node].next = head_;
      head_ = new_node;
    }
  } else {
    Iterator next = vec_[it].next;
    vec_[it].next = new_node;
    vec_[new_node].prev = it;
    vec_[new_node].next = next;
    if (next != kEnd) {
      vec_[next].prev = new_node;
    } else {
      tail_ = new_node;
    }
  }
  return new_node;
}

void TxQueue::Pop(Iterator it) {
  if (it == kEnd) {
    return;
  }
  if (vec_[it].prev != kEnd) {
      vec_[vec_[it].prev].next = vec_[it].next;
  } else {
      head_ = vec_[it].next;  
  }

  if (vec_[it].next != kEnd) {
      vec_[vec_[it].next].prev = vec_[it].prev;
  } else {
      tail_ = vec_[it].prev;  
  }
  vec_[it].next = free_head_;
  vec_[it].prev = kEnd;
  vec_[it].trans = nullptr;
  free_head_ = it;
}

Transaction* TxQueue::Front() {
  if (head_ == kEnd) {
      return nullptr;
  }
  return vec_[head_].trans;
}

Transaction* TxQueue::Back() {
  if (tail_ == kEnd) {
      return nullptr;
  }
  return vec_[tail_].trans;
}

size_t TxQueue::Size() const {
  uint32_t sz = 0;
  uint32_t it = head_;
  while(it != kEnd) {
    ++sz;
    it = vec_[it].next;
  }
  return sz;
}

}  // namespace dfly
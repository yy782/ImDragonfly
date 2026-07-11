#include "tx_queue.hpp"
#include <cassert>
namespace dfly {

void TxQueue::Grow() {
  uint32_t old_size = vec_.size();
  uint32_t new_size = old_size == 0 ? 16 : old_size * 2;
  vec_.resize(new_size);

  // 将新节点加入空闲链表
  for (uint32_t i = old_size; i < new_size - 1; ++i) {
    vec_[i].next = i + 1;
    vec_[i].used = false;
  }
  vec_[new_size - 1].next = kEnd;
  vec_[new_size - 1].used = false;

  next_free_ = old_size;
}

TxQueue::Iterator TxQueue::AllocateNode() {
  if (next_free_ == kEnd) {
    Grow();
  }

  uint32_t idx = next_free_;
  next_free_ = vec_[idx].next;
  vec_[idx].used = true;
  vec_[idx].next = kEnd;
  vec_[idx].prev = kEnd;
  return idx;
}

void TxQueue::FreeNode(Iterator it) {
  if (!vec_[it].used) {
    return;
  }

  vec_[it].used = false;
  vec_[it].trans = nullptr;
  vec_[it].next = next_free_;
  vec_[it].prev = kEnd;
  next_free_ = it;
}

TxQueue::Iterator TxQueue::Insert(Iterator it, Transaction* t) {
  assert(it == kEnd);
  Iterator new_node = AllocateNode();
  vec_[new_node].trans = t;

  if (head_ == kEnd) {
    // 空链表
    head_ = tail_ = new_node;
  } else {
    // 链接到头部
    vec_[new_node].next = head_;
    vec_[head_].prev = new_node;
    head_ = new_node;
  }

  ++size_;
  return new_node;
}







void TxQueue::Remove(Iterator it) {
  if (it == kEnd || !vec_[it].used) {
    return;
  }

  // 从链表中移除
  if (vec_[it].prev != kEnd) {
    vec_[vec_[it].prev].next = vec_[it].next;
  } else {
    head_ = vec_[it].next;  // 移除的是头节点
  }

  if (vec_[it].next != kEnd) {
    vec_[vec_[it].next].prev = vec_[it].prev;
  } else {
    tail_ = vec_[it].prev;  // 移除的是尾节点
  }

  FreeNode(it);
  --size_;
}



}  // namespace dfly
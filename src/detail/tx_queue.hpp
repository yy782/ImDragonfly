#pragma once

#include <cstdint>
#include <vector>

namespace dfly {

class Transaction;

class TxQueue {
 public:
  using Iterator = uint32_t;
  enum { kEnd = Iterator(-1) };

  TxQueue() = default;
  

  Iterator Insert(Iterator it, Transaction* t);
  void Remove(Iterator it);
  Transaction* At(Iterator it) const {
    return vec_[it].trans;
  }
  Transaction* Front() const { return At(head_); }
  Transaction* Back() const { return At(tail_); }
  void PopFront() { Remove(head_); }
  void PopBack() { Remove(tail_); }
  size_t Size() const { return size_; }
  bool Empty() const { return size_ == 0; }
  Iterator Head() const { return head_; }
  Iterator Tail() const { return tail_; }
  Iterator Next(Iterator it) const { return vec_[it].next; }
  Iterator Prev(Iterator it) const { return vec_[it].prev; }

 private:
  void Grow();
  Iterator AllocateNode();
  void FreeNode(Iterator it);

  struct Node {
    Transaction* trans = nullptr;
    uint32_t next = kEnd;
    uint32_t prev = kEnd;
    bool used = false;  // 标记节点是否在使用
  };

  std::vector<Node> vec_;
  uint32_t next_free_ = kEnd;  // 空闲链表头
  uint32_t head_ = kEnd;       // 链表头
  uint32_t tail_ = kEnd;       // 链表尾
  size_t size_ = 0;

  TxQueue(const TxQueue&) = delete;
};

}  // namespace dfly
#pragma once

#include <cstdint>
#include <vector>
#include "stateless_alloceator.hpp"
namespace dfly {

class Transaction;

class TxQueue {
public:
    using Iterator = uint32_t;
    enum { kEnd = Iterator(-1) };

    TxQueue() = default;
    

    Iterator Push(Transaction* t);
    void Pop(Iterator it);
    void Pop() {Pop(head_);}
    Transaction* Front();
    Transaction* Back();
    size_t Size() const;
    bool Empty() const { return head_ == kEnd; }
 private:
  void Grow();
  Iterator AllocateNode();
  void FreeNode(Iterator it);
  struct Node {
      Transaction* trans = nullptr;
      Iterator next = kEnd;
      Iterator prev = kEnd;
  };

  std::vector<Node, PMR_NS::polymorphic_allocator<Node>> vec_;
  uint32_t tail_ = kEnd;
  uint32_t head_ = kEnd;
  uint32_t free_head_ = kEnd;
  TxQueue(const TxQueue&) = delete;
};

}  // namespace dfly
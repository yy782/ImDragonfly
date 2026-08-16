#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "stateless_alloceator.hpp"
#include "util/intrusive_ptr.hpp"
namespace dfly {

class Transaction;

class TxQueue {
 public:
  using Iterator = uint32_t;
  enum { kEnd = Iterator(-1) };

  explicit TxQueue(PMR_NS::memory_resource* mr = PMR_NS::get_default_resource())
      : vec_(mr) {}
  ~TxQueue();

  Iterator Push(util::intrusive_ptr<Transaction> t);
  void Pop(Iterator& it);
  void Pop() {
    Iterator it = head_;
    Pop(it);
  }
  util::intrusive_ptr<Transaction> Front();
  util::intrusive_ptr<Transaction> Back();
  size_t Size() const;
  bool Empty() const { return head_ == kEnd; }
  bool IsInFreeList(Iterator it) const;
  bool IsInUsedList(Iterator it) const;
  // friend std::ostream& operator<<(std::ostream& os, const TxQueue& queue);
  std::string PrintTxLock() const;
  std::string PrintFreeList() const;
  std::string PrintUsedList() const;

 private:
  void Grow();
  Iterator AllocateNode();
  void FreeNode(Iterator it);
  struct Node {
    util::intrusive_ptr<Transaction> trans;
    Iterator next = kEnd;
    Iterator prev = kEnd;
    ~Node();
  };

  std::vector<Node, PMR_NS::polymorphic_allocator<Node>> vec_;
  uint32_t tail_ = kEnd;
  uint32_t head_ = kEnd;
  uint32_t free_head_ = kEnd;
  TxQueue(const TxQueue&) = delete;
};

}  // namespace dfly
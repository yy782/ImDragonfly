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
  free_head_ = old_size;
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

TxQueue::Iterator TxQueue::Push(std::shared_ptr<Transaction> t) {
  // auto* e = EngineShard::tlocal();
  //  auto sid = e->shard_id();
  //  auto& tx_it = t->GetPos(sid);
  //  assert(tx_it == kEnd);
  //  if (IsInUsedList(tx_it)) {
  //   LOG(INFO) << PrintUsedList();
  //   LOG(INFO) << PrintFreeList();
  //   assert(false);
  //  }
  //  if (!IsInFreeList(tx_it)) {
  //   LOG(INFO) << PrintFreeList();
  //   LOG(INFO) << PrintUsedList();
  //   assert(false);
  //  }

  Iterator new_node = AllocateNode();
  vec_[new_node].trans = std::move(t);
  Iterator it = tail_;
  while (it != kEnd) {
    if (vec_[it].trans->txid() <= vec_[new_node].trans->txid()) {
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

void TxQueue::Pop(Iterator& it) {
  if (it == kEnd) {
    return;
  }

  if (!IsInUsedList(it)) {
    LOG(INFO) << "Pop: " << it << " is not in used list";
    LOG(INFO) << PrintUsedList();
    LOG(INFO) << PrintFreeList();
    assert(false);
  }
  if (IsInFreeList(it)) {
    LOG(INFO) << "Pop: " << it << " is not in free list";
    LOG(INFO) << PrintFreeList();
    LOG(INFO) << PrintUsedList();
    assert(false);
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
  vec_[it].trans.reset();
  free_head_ = it;
  it = kEnd;
}

std::shared_ptr<Transaction> TxQueue::Front() {
  if (head_ == kEnd) {
    return nullptr;
  }
  auto t = vec_[head_].trans;
  assert(t);
  return t;
}

std::shared_ptr<Transaction> TxQueue::Back() {
  if (tail_ == kEnd) {
    return nullptr;
  }
  auto t = vec_[tail_].trans;
  assert(t);
  return t;
}

size_t TxQueue::Size() const {
  uint32_t sz = 0;
  uint32_t it = head_;
  while (it != kEnd) {
    ++sz;
    it = vec_[it].next;
  }
  return sz;
}

bool TxQueue::IsInFreeList(Iterator it) const {
  if (it == kEnd)
    return true;  // 这里kEnd是空闲链表的哨兵，所以kEnd在空闲链表中,还是要留意一下
  Iterator cur = free_head_;
  while (cur != kEnd) {
    if (cur == it) return true;
    cur = vec_[cur].next;
  }
  return false;
}

std::string TxQueue::PrintFreeList() const {
  std::string str;
  str += "free_list: ";
  uint32_t it = free_head_;
  while (it != kEnd) {
    str += std::to_string(it) + " ";
    it = vec_[it].next;
  }
  str += "\n";
  return str;
}

std::string TxQueue::PrintUsedList() const {
  std::string str;
  str += "used_list: ";
  uint32_t it = head_;
  while (it != kEnd) {
    str += std::to_string(it) + " ";
    it = vec_[it].next;
  }
  str += "\n";
  return str;
}

bool TxQueue::IsInUsedList(Iterator it) const {
  if (it == kEnd) return false;
  Iterator cur = head_;
  while (cur != kEnd) {
    if (cur == it) return true;
    cur = vec_[cur].next;
  }
  return false;
}

std::string TxQueue::PrintTxLock() const {
  std::string str;
  auto* e = EngineShard::tlocal();
  auto sid = e->shard_id();
  str += "\n====== TxQueue ======\n";
  str += "shard_id: " + std::to_string(sid) + "\n";
  str += "size: " + std::to_string(Size()) + "\n";
  str += "head: " + std::to_string(head_) + "\n";
  str += "tail: " + std::to_string(tail_) + "\n";
  str += "free_list: " + std::to_string(free_head_) + "\n";
  str += "detail for used list:\n";

  uint32_t it = head_;
  std::string lock_str;
  while (it != kEnd) {
    auto& t = vec_[it].trans;
#ifdef UNIT_TESTS
    lock_str += "事务: ";
    lock_str += std::to_string(t->id);
#endif
    // lock_str += t->PrintLock(sid);
    it = vec_[it].next;
  }
  str += lock_str;
  str += "=====================\n";
  return str;
}

}  // namespace dfly
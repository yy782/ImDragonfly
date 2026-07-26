#pragma once

#include <cstdint>
#include <list>
#include <string>
#include "stateless_alloceator.hpp"

namespace dfly {

class Transaction;

class TxQueue {
public:
    using Iterator = std::list<Transaction*>::iterator;
    static const Iterator kEnd;  // 在 cpp 中定义

    TxQueue() = default;

    Iterator Push(Transaction* t);
    void Pop(Iterator& it);
    void Pop() { 
        if (!queue_.empty()) {
            queue_.pop_front(); 
        }
    }
    
    Transaction* Front();
    Transaction* Back();
    size_t Size() const { return queue_.size(); }
    bool Empty() const { return queue_.empty(); }
    bool IsInUsedList(Iterator it) const;
    bool IsInFreeList(Iterator it) const { return false; }  // 不再使用空闲链表
    
    // 调试函数
    std::string PrintTxLock() const;
    std::string PrintFreeList() const { return "free_list: (not used)\n"; }
    std::string PrintUsedList() const;

private:
    std::list<Transaction*> queue_;  // 按 txid 升序排列
    
    TxQueue(const TxQueue&) = delete;
};

}  // namespace dfly
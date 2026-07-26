#include "tx_queue.hpp"

#include <cassert>
#include <algorithm>
#include "transaction_layer/transaction.hpp"
#include "sharding/engine_shard.hpp"

namespace dfly {

// 定义静态成员
const TxQueue::Iterator TxQueue::kEnd = std::list<Transaction*>::iterator();

TxQueue::Iterator TxQueue::Push(Transaction* t) {
    if (!t) {
        return kEnd;
    }
    
    // 按 txid 升序查找插入位置
    auto it = queue_.begin();
    while (it != queue_.end()) {
        if ((*it)->txid() > t->txid()) {
            break;
        }
        ++it;
    }
    
    // 插入到合适位置
    auto inserted_it = queue_.insert(it, t);
    return inserted_it;
}

void TxQueue::Pop(Iterator& it) {
    if (it == kEnd) {
        return;
    }
    
    // 验证迭代器是否属于当前队列
    bool found = false;
    for (auto iter = queue_.begin(); iter != queue_.end(); ++iter) {
        if (iter == it) {
            found = true;
            break;
        }
    }
    
    if (!found) {
        // 迭代器不属于当前队列，重置
        it = kEnd;
        return;
    }
    
    // 删除节点，it 指向下一个元素或 end()
    it = queue_.erase(it);
}

bool TxQueue::IsInUsedList(Iterator it) const {
    if (it == kEnd) {
        return false;
    }
    
    for (auto iter = queue_.begin(); iter != queue_.end(); ++iter) {
        if (iter == it) {
            return true;
        }
    }
    return false;
}

Transaction* TxQueue::Front() {
    if (queue_.empty()) {
        return nullptr;
    }
    return queue_.front();
}

Transaction* TxQueue::Back() {
    if (queue_.empty()) {
        return nullptr;
    }
    return queue_.back();
}

std::string TxQueue::PrintUsedList() const {
    std::string str = "used_list: ";
    for (auto* t : queue_) {
        if (t) {
            str += std::to_string(t->txid()) + " ";
        } else {
            str += "null ";
        }
    }
    str += "\n";
    return str;
}

std::string TxQueue::PrintTxLock() const {
    std::string str;
    auto* e = EngineShard::tlocal();
    if (!e) {
        str = "Error: no EngineShard\n";
        return str;
    }
    
    auto sid = e->shard_id();
    str += "\n====== TxQueue ======\n";
    str += "shard_id: " + std::to_string(sid) + "\n";
    str += "size: " + std::to_string(Size()) + "\n";
    str += "detail for used list:\n";
    
    for (auto* t : queue_) {
        if (!t) {
            str += "事务: null\n";
            continue;
        }
        
        str += "事务: ";
#ifdef UNIT_TESTS
        str += std::to_string(t->id) + " ";
#else
        str += std::to_string(t->txid()) + " ";
#endif
        str += t->PrintLock(sid);
    }
    
    str += "=====================\n";
    return str;
}

}  // namespace dfly
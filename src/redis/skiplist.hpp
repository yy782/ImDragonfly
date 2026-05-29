#pragma once

#include <vector>
#include <optional>
#include <functional>
#include <cstdlib>

namespace dfly {

template <typename T>
struct SkipListNode {
    T data;
    SkipListNode** forward;

    SkipListNode(int level, const T& d) : data(d) {
        forward = new SkipListNode*[level];
        for (int i = 0; i < level; ++i) {
            forward[i] = nullptr;
        }
    }

    ~SkipListNode() {
        delete[] forward;
    }
};

template <typename T, typename Compare = std::less<T>>
class SkipList {
public:
    using Node = SkipListNode<T>;

    explicit SkipList(Compare comp = Compare()) 
        : level_(1), length_(0), comp_(comp), header_(new Node(kMaxLevel, T{})) {
        for (int i = 0; i < kMaxLevel; ++i) {
            header_->forward[i] = nullptr;
        }
    }

    ~SkipList() {
        Node* node = header_->forward[0];
        while (node != nullptr) {
            Node* next = node->forward[0];
            delete node;
            node = next;
        }
        delete header_;
    }

    bool Insert(const T& value) {
        Node* update[kMaxLevel];
        Node* x = header_;

        for (int i = level_ - 1; i >= 0; --i) {
            while (x->forward[i] != nullptr && comp_(x->forward[i]->data, value)) {
                x = x->forward[i];
            }
            update[i] = x;
        }

        x = x->forward[0];

        if (x != nullptr && !comp_(value, x->data) && !comp_(x->data, value)) {
            return false;
        }

        int new_level = RandomLevel();
        if (new_level > level_) {
            for (int i = level_; i < new_level; ++i) {
                update[i] = header_;
            }
            level_ = new_level;
        }

        Node* new_node = new Node(new_level, value);
        for (int i = 0; i < new_level; ++i) {
            new_node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = new_node;
        }
        length_++;
        return true;
    }

    bool Remove(const T& value) {
        Node* update[kMaxLevel];
        Node* x = header_;

        for (int i = level_ - 1; i >= 0; --i) {
            while (x->forward[i] != nullptr && comp_(x->forward[i]->data, value)) {
                x = x->forward[i];
            }
            update[i] = x;
        }

        x = x->forward[0];
        if (x == nullptr || comp_(value, x->data) || comp_(x->data, value)) {
            return false;
        }

        for (int i = 0; i < level_; ++i) {
            if (update[i]->forward[i] != x) {
                break;
            }
            update[i]->forward[i] = x->forward[i];
        }

        while (level_ > 1 && header_->forward[level_ - 1] == nullptr) {
            level_--;
        }

        delete x;
        length_--;
        return true;
    }

    std::optional<Node*> Find(const T& value) const {
        Node* x = header_;
        for (int i = level_ - 1; i >= 0; --i) {
            while (x->forward[i] != nullptr && comp_(x->forward[i]->data, value)) {
                x = x->forward[i];
            }
        }
        x = x->forward[0];
        if (x != nullptr && !comp_(value, x->data) && !comp_(x->data, value)) {
            return x;
        }
        return std::nullopt;
    }

    int64_t Rank(const T& value) const {
        int64_t rank = 0;
        Node* x = header_->forward[0];
        while (x != nullptr) {
            if (!comp_(value, x->data) && !comp_(x->data, value)) {
                return rank;
            }
            rank++;
            x = x->forward[0];
        }
        return -1;
    }

    std::optional<Node*> GetByRank(int64_t rank) const {
        if (rank < 0 || static_cast<size_t>(rank) >= length_) {
            return std::nullopt;
        }
        Node* x = header_->forward[0];
        for (int64_t i = 0; i < rank; ++i) {
            x = x->forward[0];
        }
        return x;
    }

    std::vector<Node*> Range(int64_t start, int64_t end) const {
        std::vector<Node*> result;
        if (length_ == 0) {
            return result;
        }

        int64_t s = start < 0 ? start + static_cast<int64_t>(length_) : start;
        int64_t e = end < 0 ? end + static_cast<int64_t>(length_) : end;

        if (s < 0 || s >= static_cast<int64_t>(length_) || s > e) {
            return result;
        }

        if (e >= static_cast<int64_t>(length_)) {
            e = static_cast<int64_t>(length_) - 1;
        }

        Node* x = header_->forward[0];
        for (int64_t i = 0; i < s; ++i) {
            x = x->forward[0];
        }

        for (int64_t i = s; i <= e; ++i) {
            result.push_back(x);
            x = x->forward[0];
        }

        return result;
    }

    std::vector<Node*> RevRange(int64_t start, int64_t end) const {
        std::vector<Node*> result;
        if (length_ == 0) {
            return result;
        }

        int64_t s = start < 0 ? start + static_cast<int64_t>(length_) : start;
        int64_t e = end < 0 ? end + static_cast<int64_t>(length_) : end;

        if (s < 0 || s >= static_cast<int64_t>(length_) || s > e) {
            return result;
        }

        if (e >= static_cast<int64_t>(length_)) {
            e = static_cast<int64_t>(length_) - 1;
        }

        std::vector<Node*> nodes;
        Node* x = header_->forward[0];
        while (x != nullptr) {
            nodes.push_back(x);
            x = x->forward[0];
        }

        for (int64_t i = e; i >= s; --i) {
            result.push_back(nodes[static_cast<size_t>(i)]);
        }

        return result;
    }

    size_t Length() const { return length_; }
    bool Empty() const { return length_ == 0; }

private:
    static constexpr int kMaxLevel = 32;
    static constexpr double kProbability = 0.25;

    int RandomLevel() {
        int level = 1;
        while (std::rand() / (RAND_MAX + 1.0) < kProbability && level < kMaxLevel) {
            level++;
        }
        return level;
    }

    int level_;
    size_t length_;
    Compare comp_;
    Node* header_;
};

}  // namespace dfly
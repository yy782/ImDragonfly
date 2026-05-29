#pragma once 

#define OBJ_STRING 0U 
#define OBJ_LIST 1U      /* List object. */
#define OBJ_SET 2U       /* Set object. */
#define OBJ_ZSET 3U      /* Sorted set object. */
#define OBJ_HASH 4U      /* Hash object. */

#include <deque>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace dfly {

class ListObject {
public:
    ListObject() = default;
    ~ListObject() = default;

    void PushFront(const std::string& value) {
        data_.push_front(value);
    }

    void PushBack(const std::string& value) {
        data_.push_back(value);
    }

    std::string PopFront() {
        if (data_.empty()) return "";
        std::string val = data_.front();
        data_.pop_front();
        return val;
    }

    std::string PopBack() {
        if (data_.empty()) return "";
        std::string val = data_.back();
        data_.pop_back();
        return val;
    }

    size_t Length() const {
        return data_.size();
    }

    bool Empty() const {
        return data_.empty();
    }

    std::deque<std::string>& Data() {
        return data_;
    }

    const std::deque<std::string>& Data() const {
        return data_;
    }

    std::string GetElement(int64_t index) const {
        if (data_.empty()) return "";
        if (index < 0) {
            index += data_.size();
        }
        if (index < 0 || static_cast<size_t>(index) >= data_.size()) {
            return "";
        }
        return data_[static_cast<size_t>(index)];
    }

    bool SetElement(int64_t index, const std::string& value) {
        if (data_.empty()) return false;
        if (index < 0) {
            index += data_.size();
        }
        if (index < 0 || static_cast<size_t>(index) >= data_.size()) {
            return false;
        }
        data_[static_cast<size_t>(index)] = value;
        return true;
    }

    std::vector<std::string> GetRange(int64_t start, int64_t end) const {
        std::vector<std::string> result;
        if (data_.empty()) return result;

        size_t size = data_.size();
        
        if (start < 0) {
            start += size;
        }
        if (end < 0) {
            end += size;
        }

        if (start < 0) start = 0;
        if (end >= static_cast<int64_t>(size)) end = size - 1;
        if (start > end) return result;

        for (int64_t i = start; i <= end; ++i) {
            result.push_back(data_[static_cast<size_t>(i)]);
        }
        return result;
    }

    size_t Remove(int64_t count, const std::string& value) {
        size_t removed = 0;

        if (count > 0) {
            auto it = data_.begin();
            while (it != data_.end() && removed < static_cast<size_t>(count)) {
                if (*it == value) {
                    it = data_.erase(it);
                    ++removed;
                } else {
                    ++it;
                }
            }
        } else if (count < 0) {
            auto it = data_.rbegin();
            while (it != data_.rend() && removed < static_cast<size_t>(-count)) {
                if (*it == value) {
                    it = decltype(it)(data_.erase(std::next(it).base()));
                    ++removed;
                } else {
                    ++it;
                }
            }
        } else {
            auto it = data_.begin();
            while (it != data_.end()) {
                if (*it == value) {
                    it = data_.erase(it);
                    ++removed;
                } else {
                    ++it;
                }
            }
        }
        return removed;
    }

    bool InsertBefore(const std::string& pivot, const std::string& value) {
        auto it = std::find(data_.begin(), data_.end(), pivot);
        if (it == data_.end()) return false;
        data_.insert(it, value);
        return true;
    }

    bool InsertAfter(const std::string& pivot, const std::string& value) {
        auto it = std::find(data_.begin(), data_.end(), pivot);
        if (it == data_.end()) return false;
        data_.insert(std::next(it), value);
        return true;
    }

private:
    std::deque<std::string> data_;
};

class HashObject {
public:
    HashObject() = default;
    ~HashObject() = default;

    void Set(const std::string& field, const std::string& value) {
        data_[field] = value;
    }

    std::string Get(const std::string& field) const {
        auto it = data_.find(field);
        if (it == data_.end()) return "";
        return it->second;
    }

    bool Exists(const std::string& field) const {
        return data_.contains(field);
    }

    size_t Del(const std::string& field) {
        return data_.erase(field);
    }

    size_t Length() const {
        return data_.size();
    }

    bool Empty() const {
        return data_.empty();
    }

    std::unordered_map<std::string, std::string>& Data() {
        return data_;
    }

    const std::unordered_map<std::string, std::string>& Data() const {
        return data_;
    }

private:
    std::unordered_map<std::string, std::string> data_;
};

class SetObject {
public:
    SetObject() = default;
    ~SetObject() = default;

    size_t Add(const std::string& value) {
        auto [it, inserted] = data_.insert(value);
        return inserted ? 1 : 0;
    }

    size_t Remove(const std::string& value) {
        return data_.erase(value);
    }

    bool Contains(const std::string& value) const {
        return data_.contains(value);
    }

    size_t Length() const {
        return data_.size();
    }

    bool Empty() const {
        return data_.empty();
    }

    std::unordered_set<std::string>& Data() {
        return data_;
    }

    const std::unordered_set<std::string>& Data() const {
        return data_;
    }

private:
    std::unordered_set<std::string> data_;
};

}  // namespace dfly
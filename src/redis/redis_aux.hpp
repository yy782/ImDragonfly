#pragma once

#define OBJ_STRING 0U
#define OBJ_HASH 4U /* Hash object. */

#include <string>
#include <unordered_map>

namespace dfly {

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

  bool Exists(const std::string& field) const { return data_.contains(field); }

  size_t Del(const std::string& field) { return data_.erase(field); }

  size_t Length() const { return data_.size(); }

  bool Empty() const { return data_.empty(); }

  std::unordered_map<std::string, std::string>& Data() { return data_; }

  const std::unordered_map<std::string, std::string>& Data() const {
    return data_;
  }

 private:
  std::unordered_map<std::string, std::string> data_;
};

}  // namespace dfly

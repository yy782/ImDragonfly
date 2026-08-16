#include "util/json_config.hpp"

#include <cctype>
#include <cstdlib>
#include <fstream>
#include <sstream>

namespace util {

bool JsonConfig::LoadFromFile(const std::string& path, std::string* error) {
  std::ifstream ifs(path);
  if (!ifs.is_open()) {
    if (error) *error = "无法打开配置文件: " + path;
    return false;
  }
  std::stringstream ss;
  ss << ifs.rdbuf();
  return Parse(ss.str(), error);
}

bool JsonConfig::Parse(const std::string& content, std::string* error) {
  values_.clear();

  size_t i = 0;
  while (i < content.size()) {
    while (i < content.size() &&
           (isspace(content[i]) || content[i] == '{' || content[i] == '}'))
      i++;
    if (i >= content.size()) break;

    if (content[i] != '"') {
      if (error) *error = "JSON 格式错误: 期望 key 字符串";
      return false;
    }
    ++i;
    std::string key;
    while (i < content.size() && content[i] != '"') {
      if (content[i] == '\\' && i + 1 < content.size()) {
        key += content[i + 1];
        i += 2;
      } else {
        key += content[i++];
      }
    }
    if (i >= content.size()) {
      if (error) *error = "JSON 格式错误: key 未闭合";
      return false;
    }
    ++i;
    while (i < content.size() && (isspace(content[i]) || content[i] == ':'))
      i++;
    if (i >= content.size()) {
      if (error) *error = "JSON 格式错误: 缺少值";
      return false;
    }

    std::string value;
    if (content[i] == '"') {
      ++i;
      while (i < content.size() && content[i] != '"') {
        if (content[i] == '\\' && i + 1 < content.size()) {
          value += content[i + 1];
          i += 2;
        } else {
          value += content[i++];
        }
      }
      ++i;
    } else {  // 数值 / bool / null，读到逗号或空白为止
      while (i < content.size() && content[i] != ',' && content[i] != '}') {
        if (!isspace(content[i])) value += content[i];
        ++i;
      }
    }

    if (!SetValue(key, value, error)) {
      return false;
    }

    // 跳过分隔符
    while (i < content.size() && (isspace(content[i]) || content[i] == ','))
      i++;
  }
  return true;
}

bool JsonConfig::SetValue(const std::string& key, const std::string& raw,
                          std::string* error) {
  if (key.empty()) {
    if (error) *error = "JSON 格式错误: 空 key";
    return false;
  }
  values_[key] = raw;
  return true;
}

bool JsonConfig::Has(const std::string& key) const {
  return values_.count(key) > 0;
}

std::string JsonConfig::GetString(const std::string& key,
                                  const std::string& def) const {
  auto it = values_.find(key);
  if (it == values_.end()) return def;
  return it->second;
}

int64_t JsonConfig::GetInt(const std::string& key, int64_t def) const {
  auto it = values_.find(key);
  if (it == values_.end()) return def;
  return std::strtoll(it->second.c_str(), nullptr, 10);
}

double JsonConfig::GetDouble(const std::string& key, double def) const {
  auto it = values_.find(key);
  if (it == values_.end()) return def;
  return std::strtod(it->second.c_str(), nullptr);
}

bool JsonConfig::GetBool(const std::string& key, bool def) const {
  auto it = values_.find(key);
  if (it == values_.end()) return def;
  const std::string& v = it->second;
  if (v == "true" || v == "1") return true;
  if (v == "false" || v == "0") return false;
  return def;
}

}  // namespace util

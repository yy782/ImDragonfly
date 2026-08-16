#pragma once
// 极简 JSON 配置解析器（仅支持扁平键值）
// 示例: { "name": "gateway", "port": 8000, "debug": true }

#include <cstdint>
#include <string>
#include <unordered_map>

namespace util {

// JSON 配置：解析为扁平键值对，支持 string / int64 / double / bool
class JsonConfig {
 public:
  // 从文件加载，失败返回 false 并填充 error
  bool LoadFromFile(const std::string& path, std::string* error = nullptr);

  // 从字符串加载
  bool Parse(const std::string& content, std::string* error = nullptr);

  bool Has(const std::string& key) const;

  std::string GetString(const std::string& key,
                        const std::string& def = "") const;
  int64_t GetInt(const std::string& key, int64_t def = 0) const;
  double GetDouble(const std::string& key, double def = 0.0) const;
  bool GetBool(const std::string& key, bool def = false) const;

 private:
  bool SetValue(const std::string& key, const std::string& raw_value,
                std::string* error);

  std::unordered_map<std::string, std::string> values_;
};

}  // namespace util

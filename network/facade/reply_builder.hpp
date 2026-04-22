// reply_builder.h
#pragma once
#include <string>
#include <vector>

namespace dfly{

inline std::string BuildNull() {
    return "$-1\r\n";
}

inline std::string BuildString(const std::string& s) {
    return "+" + s + "\r\n";
}

inline std::string BuildError(const std::string& err) {
    return "-ERR " + err + "\r\n";
}

inline std::string BuildInteger(int64_t n) {
    return ":" + std::to_string(n) + "\r\n";
}

inline std::string BuildBulkString(const std::string& s) {
    if (s.empty()) return "$-1\r\n";
    return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}

inline std::string BuildArray(const std::vector<std::string>& items) {
    std::string res = "*" + std::to_string(items.size()) + "\r\n";
    for (const auto& item : items) {
        res += BuildString(item);
    }
    return res;
}
}
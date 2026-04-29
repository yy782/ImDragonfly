// resp_parser.h
#pragma once

#include <string>
#include <vector>
#include <cctype>
#include <sstream>

#include "parsed_command.hpp"

namespace dfly{


inline BackedArguments ParseRESP(/* data */) {
    std::vector<std::string> result;
    if (data.empty() || data[0] != '*') {
        return result;
    }
    
    size_t pos = 1;
    // 解析数组长度
    size_t array_len = 0;
    while (pos < data.size() && isdigit(data[pos])) {
        array_len = array_len * 10 + (data[pos] - '0');
        pos++;
    }
    
    // 跳过 \r\n
    if (pos < data.size() && data[pos] == '\r') pos++;
    if (pos < data.size() && data[pos] == '\n') pos++;
    
    // 解析每个元素
    for (size_t i = 0; i < array_len && pos < data.size(); i++) {
        if (data[pos] != '$') break;
        pos++;
        
        // 解析字符串长度
        size_t str_len = 0;
        while (pos < data.size() && isdigit(data[pos])) {
            str_len = str_len * 10 + (data[pos] - '0');
            pos++;
        }
        
        // 跳过 \r\n
        if (pos < data.size() && data[pos] == '\r') pos++;
        if (pos < data.size() && data[pos] == '\n') pos++;
        
        // 读取字符串内容
        if (pos + str_len <= data.size()) {
            result.push_back(data.substr(pos, str_len));
            pos += str_len;
        }
        
        // 跳过 \r\n
        if (pos < data.size() && data[pos] == '\r') pos++;
        if (pos < data.size() && data[pos] == '\n') pos++;
    }
    
    return BackedArguments(result.begin(), result.end(), result.size());
}

}
   
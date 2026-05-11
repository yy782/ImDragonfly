
#include "base/io_buf.hpp"
#include <cctype>
namespace dfly{

class RESP_Buf : public base::IoBuf{
using base::IoBuf::IoBuf;
public:


std::vector<std::string_view> ParseRESP() {
    std::vector<std::string_view> result;
    
    char* data = begin();
    size_t size = write_index_;
    size_t pos = p_;
    
    if (pos >= size) {
        return result;
    }
    
    // 必须以 '*' 开头
    if (data[pos] != '*') {
        return result;
    }
    pos++;
    
    // 读取数组元素个数
    int64_t num_elements = 0;
    while (pos < size && std::isdigit(data[pos])) {
        num_elements = num_elements * 10 + (data[pos] - '0');
        pos++;
    }
    
    if (num_elements <= 0 || pos + 2 > size) {
        return result;
    }
    
    // 检查 \r\n
    if (data[pos] != '\r' || data[pos+1] != '\n') {
        return result;
    }
    pos += 2;
    
    // 解析每个元素
    for (int64_t i = 0; i < num_elements; i++) {
        if (pos >= size) return result;
        
        // 每个元素必须是批量字符串
        if (data[pos] != '$') {
            return result;
        }
        pos++;
        
        // 读取字符串长度
        int64_t len = 0;
        while (pos < size && std::isdigit(data[pos])) {
            len = len * 10 + (data[pos] - '0');
            pos++;
        }
        
        if (len < 0 || pos + 2 > size) {
            return result;
        }
        
        // 检查 \r\n
        if (data[pos] != '\r' || data[pos+1] != '\n') {
            return result;
        }
        pos += 2;
        
        // 检查数据是否足够
        if (pos + len + 2 > size) {
            return result;
        }
        
        // 添加字符串视图
        result.emplace_back(data + pos, len);
        
        // 跳过数据和结尾的 \r\n
        pos += len;
        if (pos + 2 > size || data[pos] != '\r' || data[pos+1] != '\n') {
            return result;
        }
        pos += 2;
    }
    
    // 解析成功，更新 p_ 并消费数据
    p_ = pos;
    return result;
}


private:

size_t getNum() {
    assert(std::isdigit(*(begin() + p_)));
    char* start = begin() + p_;
    char* end = start;
    do {
        ++CommandByte_;
        ++end;
    } while (*end != '\r');
    
    std::string_view Num(start, end - start);
    size_t num = 0;
    for (char c : Num) {
        num = num * 10 + (c - '0');
    }
    p_ = end - begin();  // 移动到 \r 位置，外部会再调整
    return num;
}

    char* getChar() {
        return &buffer_[p_];
    }

    size_t p_ = read_index_;
    ssize_t CommandByte_ = 0;
    size_t index_ = 0; // for Command_
    std::vector<std::string_view> Command_; 
};    

}
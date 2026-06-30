
#include "base/io_buf.hpp"
#include <cctype>
namespace dfly{

class RESP_Buf : public base::IoBuf{
using base::IoBuf::IoBuf;
public:
    std::vector<std::string_view>& ParseRESP() {
        result.clear();
        char* data = begin();
        size_t size = write_index_;
        size_t pos = read_index_;  
        if (pos >= size) {
            return result;
        }

        if (data[pos] != '*') {
            return result;
        }
        pos++;

        int64_t num_elements = 0;
        while (pos < size && std::isdigit(data[pos])) {
            num_elements = num_elements * 10 + (data[pos] - '0');
            pos++;
        }
        
        if (num_elements <= 0 || pos + 2 > size) {
            return result;
        }

        if (data[pos] != '\r' || data[pos+1] != '\n') {
            return result;
        }
        pos += 2;

        for (int64_t i = 0; i < num_elements; i++) {
            if (pos >= size) return result;

            if (data[pos] != '$') {
                return result;
            }
            pos++;

            int64_t len = 0;
            while (pos < size && std::isdigit(data[pos])) {
                len = len * 10 + (data[pos] - '0');
                pos++;
            }
            
            if (len < 0 || pos + 2 > size) {
                return result;
            }

            if (data[pos] != '\r' || data[pos+1] != '\n') {
                return result;
            }
            pos += 2;

            if (pos + len + 2 > size) {
                return result;
            }
            result.emplace_back(data + pos, len);
            pos += len;
            
            if (pos + 2 > size || data[pos] != '\r' || data[pos+1] != '\n') {
                return result;
            }
            pos += 2;
        }
        size_t consumed = pos - read_index_;
        consume(consumed);  
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
        p_ = end - begin();  
        return num;
    }

    char* getChar() {
        return &buffer_[p_];
    }

    size_t p_ = read_index_;
    ssize_t CommandByte_ = 0;
    std::vector<std::string_view> result; 
};    

}
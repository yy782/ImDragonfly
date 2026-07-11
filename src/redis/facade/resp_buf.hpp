#include "YY/net/TcpBuffer.h"
#include <cctype>
namespace dfly{

class RESP_Buf {
public:
    std::vector<std::string_view>& ParseRESP(yy::net::TcpBuffer& buf) {
        result.clear();
        char* data = buf.begin();
        size_t size = buf.write_index_;
        size_t pos = buf.read_index_;  
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
        size_t consumed = pos - buf.read_index_;
        buf.consume(consumed);  
        return result;
    }
private:
    size_t p_ = 0;
    ssize_t CommandByte_ = 0;
    std::vector<std::string_view> result; 
};    

}


#include <cctype>
namespace dfly{

struct ParseRESP {
    std::vector<std::string_view>& Parse(const char* data, int bytes) {
        result.clear();
        size_t pos = 0;  
        if (pos >= bytes) {
            return result;
        }

        if (data[pos] != '*') {
            return result;
        }
        pos++;

        int64_t num_elements = 0;
        while (pos < bytes && std::isdigit(data[pos])) {
            num_elements = num_elements * 10 + (data[pos] - '0');
            pos++;
        }
        
        if (num_elements <= 0 || pos + 2 > bytes) {
            return result;
        }

        if (data[pos] != '\r' || data[pos+1] != '\n') {
            return result;
        }
        pos += 2;

        for (int64_t i = 0; i < num_elements; i++) {
            if (pos >= bytes) return result;

            if (data[pos] != '$') {
                return result;
            }
            pos++;

            int64_t len = 0;
            while (pos < bytes && std::isdigit(data[pos])) {
                len = len * 10 + (data[pos] - '0');
                pos++;
            }
            
            if (len < 0 || pos + 2 > bytes) {
                return result;
            }

            if (data[pos] != '\r' || data[pos+1] != '\n') {
                return result;
            }
            pos += 2;

            if (pos + len + 2 > bytes) {
                return result;
            }
            result.emplace_back(data + pos, len);
            pos += len;
            
            if (pos + 2 > bytes || data[pos] != '\r' || data[pos+1] != '\n') {
                return result;
            }
            pos += 2;
        }  
        return result;
    }
    std::vector<std::string_view> result;
};
   

}
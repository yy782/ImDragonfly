
#include "base/io_buf.hpp"
#include <cctype>
namespace dfly{

class RESP_Buf : base::IoBuf{
using base::IoBuf::IoBuf;
public:
    std::vector<std::string_view> ParseRESP() {

        std::vector<std::string_view> res;

        while (p_ != write_index_) {
            if (p_ == '*') {
                retrieve(CommandByte_);
                CommandByte_ = 1;
                index_ = 0;

                res = Command_;
                Command_.clear();
            }else if (std::isdigit(p_)){
                size_t num = getNum();
                if (CommandByte_ == 1) {
                    Command_.resize(num);
                }else {
                    p_ += 2;
                    char* end = p_+num;
                    std::string_view param(p_, end);
                    Command_[index++] = param;

                    CommandByte_ += end - p_ + 2;
                    p_ = end;
                }
            }else if (p_ == '\r' || p == '\n' || p == '$') {
                
            }else {
                // TODO 记录完整信息
            }
            ++p_;
        }

        return res;
    }


private:

    size_t getNum() {
        assert(std::isdigit(p_));
        char* end = p_;
        do{
            ++CommandByte_;
            ++end;
        }while (end != '\r');
        std::string_view Num(p_,end);
        size_t num = std::atoi(Num);
        p_ = end;
        return num;
    }

    char* p_ = read_index_;
    ssize_t CommandByte_ = 0;
    size_t index_ = 0; // for Command_
    std::vector<std::string_view> Command_; 
}    

}
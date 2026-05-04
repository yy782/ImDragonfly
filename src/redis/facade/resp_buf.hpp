
#include "base/io_buf.hpp"
#include <cctype>
namespace dfly{

class RESP_Buf : public base::IoBuf{
using base::IoBuf::IoBuf;
public:


    std::vector<std::string_view> ParseRESP() {

        std::vector<std::string_view> res;

        while (p_ != write_index_) {
            if (*getChar() == '*') {
                retrieve(CommandByte_);
                CommandByte_ = 1;
                index_ = 0;

                res = Command_;
                Command_.clear();
            }else if (std::isdigit(*getChar())){
                size_t num = getNum();
                if (CommandByte_ == 1) {
                    Command_.resize(num);
                }else {
                    p_ += 2;
                    char* end = getChar()+num;
                    std::string_view param(getChar(), end);
                    Command_[index_++] = param;

                    CommandByte_ += end - getChar() + 2;
                    p_ += num;
                }
            }else if (*getChar() == '\r' || *getChar() == '\n' || *getChar() == '$') {
                
            }else {
                // TODO 记录完整信息
            }
            ++p_;
        }

        return res;
    }


private:

    size_t getNum() {
        assert(std::isdigit(*getChar()));
        char* end = getChar();
        do{
            ++CommandByte_;
            ++end;
        }while (*end != '\r');
        std::string_view Num(getChar(), end);
        size_t num = std::atoi(Num.data());
        p_ += end - getChar();
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
#include "command_id.hpp"

namespace facade{

CommandId::CommandId(const char* name, size_t keys_start, size_t keys_nums, size_t keys_offset)
    : name_(name),
      keys_start_(keys_start),
      keys_nums_(keys_nums),
      keys_offset_(keys_offset){
}

}
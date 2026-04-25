#include "command_id.hpp"

namespace facade{

CommandId::CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key,
                     int8_t last_key)
    : name_(name),
      opt_mask_(mask),
      arity_(arity),
      first_key_(first_key),
      last_key_(last_key){
}

}
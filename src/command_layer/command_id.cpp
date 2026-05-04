#include "command_id.hpp"

namespace facade{

CommandId::CommandId(const char* name, int8_t arity, int8_t first_key,
                     int8_t last_key)
    : name_(name),
      arity_(arity),
      first_key_(first_key),
      last_key_(last_key){
}

}
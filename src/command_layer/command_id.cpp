#include "command_id.hpp"

namespace facade {

CommandId::CommandId(const char* name, uint32_t mask, int8_t first_key, int8_t last_key)
    : name_(name), opt_mask_(mask), first_key_(first_key), last_key_(last_key) {}

}  // namespace facade

namespace dfly {

CommandId::CommandId(const char* name, uint32_t mask, int8_t first_key, int8_t last_key)
    : facade::CommandId(name, mask, first_key, last_key) {
  if (name_ == "MSET") {
    interleave_step_ = 2;
  }
}

// bool CommandId::IsTransactional() const {
//   return first_key_ > 0 || (opt_mask_ & CO::GLOBAL_TRANS) ||
//          (opt_mask_ & CO::NO_KEY_TRANSACTIONAL);
// }



}  // namespace dfly

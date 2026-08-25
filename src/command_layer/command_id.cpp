#include "command_layer/command_id.hpp"

#include <glog/logging.h>

#include "command_layer/cmd_support.hpp"
#include "command_layer/command_registry.hpp"

namespace dfly {

CommandId::CommandId(const CommandSpec& spec)
    : name_(spec.name),
      opt_mask_(spec.mask),
      first_key_(spec.first_key),
      last_key_(spec.last_key),
      key_step_(spec.step),
      handler_(spec.handler) {}

// last 为 -1 表示"到参数末尾"
CommandId::KeyRange CommandId::Keys(::dfly::CmdArgList args) const {
  if (opt_mask_ & (CO::GLOBAL_TRANS | CO::NO_KEY_TRANSACTIONAL))
    return KeyRange{KeyIterator(args, 0, 0, 1), KeyIterator(args, 0, 0, 1)};
  DCHECK(first_key_ > 0);
  unsigned start = unsigned(first_key_);
  unsigned end = last_key_ > 0 ? unsigned(last_key_ + 1)
                               : unsigned(int(args.size()) + last_key_ + 1);
  unsigned step = unsigned(key_step_);

  return KeyRange{KeyIterator(args, start, end, step),
                  KeyIterator(args, end, end, step)};
}

cmd::CoroTask CommandId::Invoke(CommandContext* cmd_cntx,
                                CmdArgList args) const {
  return handler_(cmd_cntx, args);
}

}  // namespace dfly

#include "command_registry.hpp"

#include <glog/logging.h>

namespace dfly {

CommandRegistry::CommandRegistry() {}

void CommandRegistry::Register(std::span<const CommandSpec> specs) {
  for (const CommandSpec& s : specs) {
    auto [it, inserted] = cmd_map_.emplace(s.name, CommandId{s});
    (void)it;
    CHECK(inserted) << "duplicate command: " << s.name;
  }
}

}  // namespace dfly

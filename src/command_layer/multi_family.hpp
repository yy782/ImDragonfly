// #pragma once

// #include "command_layer/cmn_types.hpp"
// #include "command_layer/command_registry.hpp"
// #include "detail/conn_context.hpp"

// namespace dfly {

// using ::cmn::CmdArgList;

// class MultiFamily {
// public:
//     static void Register(CommandRegistry* registry);

// private:
//     static void Multi(CommandContext* cmd_cntx, CmdArgList args);
//     static void Exec(CommandContext* cmd_cntx, CmdArgList args);
//     static void Discard(CommandContext* cmd_cntx, CmdArgList args);
//     static void Watch(CommandContext* cmd_cntx, CmdArgList args);
//     static void Unwatch(CommandContext* cmd_cntx, CmdArgList args);
// };

// void RegisterMulti(CommandRegistry* registry);

// }  // namespace dfly
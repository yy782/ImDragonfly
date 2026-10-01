#include "server/command_line.hpp"

#include <cstdlib>

namespace dfly {

std::unordered_map<std::string, CommandHandler> g_command_handlers = {
    {"port",
     [](const std::string& v) {
       redis_port = static_cast<uint16_t>(std::atoi(v.c_str()));
     }},
    {"shards", [](const std::string& v) { shards = std::atoi(v.c_str()); }},
    {"config", [](const std::string& v) { config_path = v; }},
    {"use_raft",
     [](const std::string& v) {
       use_raft = (v == "1" || v == "true" || v == "True" || v == "TRUE");
     }},
    {"log_dir", [](const std::string& v) { log_dir = v; }},
};

bool ParseCommandLine(int argc, char* argv[], std::string* error) {
  for (int i = 1; i < argc; ++i) {
    const std::string arg(argv[i]);
    const size_t eq = arg.find('=');
    if (eq == std::string::npos) {
      if (error) {
        *error = "命令行参数缺少 '=': " + arg;
      }
      return false;
    }
    const std::string key = arg.substr(0, eq);
    const std::string value = arg.substr(eq + 1);
    const auto it = g_command_handlers.find(key);
    if (it == g_command_handlers.end()) {
      if (error) {
        *error = "未知命令行参数: " + key;
      }
      return false;
    }
    it->second(value);
  }
  return true;
}

}  // namespace dfly

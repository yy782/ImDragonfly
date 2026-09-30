#pragma once

#include <functional>
#include <string>
#include <unordered_map>

#include "detail/conflig.hpp"

namespace dfly {

// 命令行处理函数表：key（'=' 前的字符串）-> 处理函数（参数为 '=' 后的值）。
using CommandHandler = std::function<void(const std::string&)>;
extern std::unordered_map<std::string, CommandHandler> g_command_handlers;

// 遍历 argv，按 "key=value" 拆分并调用对应 handler。
// 全部成功返回 true；遇到不含 '=' 的参数或未知 key 时返回 false 并写入 error。
bool ParseCommandLine(int argc, char* argv[], std::string* error);

}  // namespace dfly

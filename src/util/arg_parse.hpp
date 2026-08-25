#pragma once

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <system_error>

namespace util {

inline bool ParseInt(std::string_view s, int64_t& out) {
  auto res = std::from_chars(s.data(), s.data() + s.size(), out);
  return res.ec == std::errc() && res.ptr == s.data() + s.size();
}

inline bool ParseDouble(std::string_view s, double& out) {
  auto res = std::from_chars(s.data(), s.data() + s.size(), out);
  return res.ec == std::errc() && res.ptr == s.data() + s.size();
}

inline bool ParseOne(std::string_view s, std::string_view& out) {
  out = s;
  return true;
}

inline bool ParseOne(std::string_view s, int64_t& out) {
  return ParseInt(s, out);
}

template <typename ArgList, typename... Ts>
bool GetArgs(ArgList args, size_t off, Ts&... out) {
  if (args.size() < off + sizeof...(Ts)) return false;
  size_t i = off;
  return (ParseOne(args[i++], out) && ...);
}

}  // namespace util

#pragma once

#include <cctype>
#include <charconv>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
namespace dfly {

// ParseAll 的返回结果：一批完整命令 + 半包起始偏移。
struct ParseResult {
  std::vector<std::vector<std::string_view>> cmds;  // 每条命令的参数列表
  size_t partial_offset =
      0;  // 第一个不完整命令（半包）的起始偏移；无半包时 == bytes
};

struct ParseRESP {
  bool ParseOne(const char* data, size_t bytes, size_t* pos,
                std::vector<std::string_view>* out, bool* need_more) {
    *need_more = false;
    if (*pos >= bytes) {
      *need_more = true;
      return false;
    }

    if (data[*pos] != '*') {
      return false;
    }
    (*pos)++;

    int64_t num_elements = 0;
    while (*pos < bytes &&
           std::isdigit(static_cast<unsigned char>(data[*pos]))) {
      num_elements = num_elements * 10 + (data[*pos] - '0');
      (*pos)++;
    }

    if (*pos + 2 > bytes) {
      *need_more = true;
      return false;
    }
    if (data[*pos] != '\r' || data[*pos + 1] != '\n') {
      return false;
    }
    *pos += 2;

    if (num_elements == 0) {
      return true;  // 合法空数组（*0\r\n），无参数，直接成功。
    }
    if (num_elements < 0) {
      return false;  // 负数长度/元素数是格式错误。
    }

    // 参数个数已知，一次性预留容量，避免 emplace_back 多次扩容。
    out->reserve(static_cast<size_t>(num_elements));

    for (int64_t i = 0; i < num_elements; i++) {
      if (*pos >= bytes) {
        *need_more = true;
        return false;
      }

      if (data[*pos] != '$') {
        return false;
      }
      (*pos)++;

      int64_t len = 0;
      while (*pos < bytes &&
             std::isdigit(static_cast<unsigned char>(data[*pos]))) {
        len = len * 10 + (data[*pos] - '0');
        (*pos)++;
      }

      if (*pos + 2 > bytes) {
        *need_more = true;
        return false;
      }
      if (data[*pos] != '\r' || data[*pos + 1] != '\n') {
        return false;
      }
      *pos += 2;

      if (len < 0) {
        return false;
      }

      if (*pos + static_cast<size_t>(len) + 2 > bytes) {
        *need_more = true;
        return false;
      }
      out->emplace_back(data + *pos, static_cast<size_t>(len));
      *pos += static_cast<size_t>(len);

      if (*pos + 2 > bytes) {
        *need_more = true;
        return false;
      }
      if (data[*pos] != '\r' || data[*pos + 1] != '\n') {
        return false;
      }
      *pos += 2;
    }
    return true;
  }

  ParseResult ParseAll(const char* data, size_t bytes) {
    ParseResult pr;
    size_t pos = 0;
    while (pos < bytes) {
      size_t cmd_start = pos;
      std::vector<std::string_view> args;
      bool need_more = false;
      if (ParseOne(data, bytes, &pos, &args, &need_more)) {
        if (!args.empty()) {
          pr.cmds.push_back(std::move(args));
        }
      } else if (need_more) {
        pr.partial_offset = cmd_start;
        return pr;
      } else {
        // 格式错误：跳过当前字节继续，避免死循环。
        pos = cmd_start + 1;
      }
    }
    pr.partial_offset = bytes;  // 全部消费完，无半包。
    return pr;
  }
};

inline std::string EncodeBulkString(std::string_view v) {
  std::string out;
  out.reserve(v.size() + 32);
  out.push_back('$');
  char buf[24];
  auto res = std::to_chars(buf, buf + sizeof(buf), v.size());
  out.append(buf, res.ptr - buf);
  out.append("\r\n");
  out.append(v);
  out.append("\r\n");
  return out;
}

inline std::string EncodeBulkString(int64_t v) {
  char num[24];
  auto nres = std::to_chars(num, num + sizeof(num), v);
  size_t n = static_cast<size_t>(nres.ptr - num);

  std::string out;
  out.reserve(n + 32);
  out.push_back('$');
  char len[24];
  auto lres = std::to_chars(len, len + sizeof(len), n);
  out.append(len, lres.ptr - len);
  out.append("\r\n");
  out.append(num, n);
  out.append("\r\n");
  return out;
}

}  // namespace dfly

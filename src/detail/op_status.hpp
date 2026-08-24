#pragma once

#include <cstdint>

namespace dfly {

// 操作状态码：命令执行结果的成功/失败语义。
// OK 表示成功，其余为具体失败原因（表达层据此生成响应）。
enum class OpStatus : uint16_t {
  OK,
  KEY_NOTFOUND,
  WRONG_TYPE,
  SKIPPED,
  NO_KEY,
  OUT_OF_RANGE,
  SYNTAX_ERROR,
  INVALID_VALUE,  // 值存在但格式不正确（如 INCR 作用于非整数字符串）
};

}  // namespace dfly

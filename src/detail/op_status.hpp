#pragma once

#include <cstdint>

namespace dfly {

enum class OpStatus : uint16_t {
  OK,
  KEY_NOTFOUND,
  WRONG_TYPE,
  SKIPPED,
  NO_KEY,
  OUT_OF_RANGE,
  SYNTAX_ERROR,
  INVALID_VALUE,  // 值存在但格式不正确
};

}  // namespace dfly

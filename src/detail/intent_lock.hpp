#pragma once

#include <glog/logging.h>

#include <string>
namespace dfly {

class IntentLock {
 public:
  enum Mode { SHARED = 0, EXCLUSIVE = 1 };

  bool Acquire(Mode m) {
    ++cnt_[m];

    if (cnt_[1 ^ int(m)]) return false;
    return m == SHARED || cnt_[EXCLUSIVE] == 1;
  }

  bool Check(Mode m) const {
    unsigned s = cnt_[EXCLUSIVE];
    if (s) return false;

    return (m == SHARED) ? true : cnt_[SHARED] == 0;
  }

  void Release(Mode m, unsigned val = 1) {
    DCHECK_GE(cnt_[m], val);
    cnt_[m] -= val;
  }
  bool IsFree() const noexcept {
    return cnt_[SHARED] == 0 && cnt_[EXCLUSIVE] == 0;
  }

  static const char* ModeName(Mode m) {
    return m == SHARED ? "SHARED" : "EXCLUSIVE";
  }

  std::string Print() const {
    return std::string("{SHARED: ") + std::to_string(cnt_[0]) +
           ", EXCLUSIVE: " + std::to_string(cnt_[1]) + "}";
  }

 private:
  unsigned cnt_[2] = {0, 0};
};

}  // namespace dfly

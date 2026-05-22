#pragma once

#include <cassert>
#include <ostream>

namespace dfly {

class IntentLock {
 public:
  enum Mode { SHARED = 0, EXCLUSIVE = 1 };

  bool Acquire(Mode m) {
    ++cnt_[m];

    if (cnt_[1 ^ int(m)])
      return false;
    return m == SHARED || cnt_[EXCLUSIVE] == 1;
  }

  bool Check(Mode m) const {
    unsigned s = cnt_[EXCLUSIVE];
    if (s)
      return false;

    return (m == SHARED) ? true : cnt_[SHARED] == 0;
  }

  bool IsContended() const {
    return (cnt_[EXCLUSIVE] > 1) || (cnt_[EXCLUSIVE] == 1 && cnt_[SHARED] > 0);
  }

  unsigned ContentionScore() const {
    return cnt_[EXCLUSIVE] * 256 + cnt_[SHARED];
  }

  void Release(Mode m, unsigned val = 1) {
    assert(cnt_[m] >= val);
    cnt_[m] -= val;
  }

  bool IsFree() const {
    return (cnt_[0] | cnt_[1]) == 0;
  }

  static const char* ModeName(Mode m) {
    return m == SHARED ? "SHARED" : "EXCLUSIVE";
  }

  void VerifyDebug() {}

  friend std::ostream& operator<<(std::ostream& o, const IntentLock& lock) {
    return o << "{SHARED: " << lock.cnt_[0] << ", EXCLUSIVE: " << lock.cnt_[1] << "}";
  }

 private:
  unsigned cnt_[2] = {0, 0};
};

}  // namespace dfly
#pragma once

#include <glog/logging.h>

#include <ostream>

namespace dfly {

class IntentLock {
 public:
  enum Mode { SHARED = 0, EXCLUSIVE = 1 };

  bool Acquire(Mode m) {
    ++cnt_[m];

    if (cnt_[1 ^ int(m)]) return false;
    return m == SHARED || cnt_[EXCLUSIVE] == 1;
  }

  void Release(Mode m, unsigned val = 1) {
    DCHECK_GE(cnt_[m], val);
    cnt_[m] -= val;
  }

  // 判断锁是否完全空闲（无任何 SHARED/EXCLUSIVE 持有者），供 db_table 在
  // Release 后判定是否可从 map 里 erase 掉这个 IntentLock。
  bool IsFree() const noexcept {
    return cnt_[SHARED] == 0 && cnt_[EXCLUSIVE] == 0;
  }

  static const char* ModeName(Mode m) {
    return m == SHARED ? "SHARED" : "EXCLUSIVE";
  }

  friend std::ostream& operator<<(std::ostream& o, const IntentLock& lock) {
    return o << "{SHARED: " << lock.cnt_[0] << ", EXCLUSIVE: " << lock.cnt_[1]
             << "}";
  }

 private:
  unsigned cnt_[2] = {0, 0};
};

}  // namespace dfly

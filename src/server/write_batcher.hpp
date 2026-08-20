#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "io/uring_socket.hpp"
#include "util/thread.hpp"

namespace dfly {

// WriteCursor：DoWrite 内部的"写游标"，记录一批 batch 发到哪了。
//
// 一个 batch 里多条 std::string，内核可能只 sendmsg 一部分字节，
// 必须记住"发到第几条 + 这一条内部偏移多少"，下次接着发。
// 把 (bi, off) 封成结构体 + 两个方法，比裸 size_t 更可读：
//   - cur_idx_  当前在 batch 的第几条
//   - off_      当前条目内部已发字节数
//   - done()    整个 batch 发完？
//   - fill_iovec(rem, batch) 从游标处往后填 iovec
//   - advance(bytes) 按"刚发了多少字节"推进游标
struct WriteCursor {
  size_t cur_idx_ = 0;  // batch 里当前条目下标
  size_t off_ = 0;      // batch[cur_idx_] 内部已发偏移

  bool done(const std::vector<std::string>& batch) const noexcept {
    return cur_idx_ >= batch.size();
  }

  // 从游标处开始往后填 iovec，第一条带偏移 off_。
  // batch 是非 const 因为 iovec.iov_base 是 void*（虽然 sendmsg 不会修改）。
  void fill_iovec(std::vector<iovec>& rem,
                  std::vector<std::string>& batch) const {
    for (size_t i = cur_idx_; i < batch.size(); ++i) {
      size_t start = (i == cur_idx_) ? off_ : 0;
      rem.push_back({batch[i].data() + start, batch[i].size() - start});
    }
  }

  // 按"刚发了 bytes 字节"推进游标：先把当前条目剩余部分扣掉，
  // 不够就跳到下一条，直到 bytes 扣完或 batch 走完。
  void advance(const std::vector<std::string>& batch, size_t bytes) noexcept {
    while (bytes > 0 && cur_idx_ < batch.size()) {
      size_t cur_left = batch[cur_idx_].size() - off_;
      if (bytes >= cur_left) {
        bytes -= cur_left;
        ++cur_idx_;
        off_ = 0;
      } else {
        off_ += bytes;
        bytes = 0;
      }
    }
  }

  void reset() noexcept {
    cur_idx_ = 0;
    off_ = 0;
  }
};

class WriteBatcher {
 public:
  WriteBatcher(base::UringSocket* socket, pthread_t loop_tid) noexcept
      : socket_(socket), loop_tid_(loop_tid) {}

  // 单条回复入队。如果当前没有写在进行，启动 DoWrite。
  void Enqueue(std::string&& s);

  // 批量回复入队（来自 pipeline_squasher 的整批回复）。
  // 同样会在空闲时触发 DoWrite。
  void EnqueueBatch(std::vector<std::string>&& batch);

  bool in_progress() const noexcept { return in_progress_; }

  // 用于 RedisSession 析构顺序断言 / 调试。
  size_t pending_count() const noexcept { return queue_.size() - read_idx_; }

 private:
  cppcoro::AsyncTask DoWrite();
  void StartIfNeeded();

  base::UringSocket* socket_;
  pthread_t loop_tid_;

  std::vector<std::string> queue_;
  size_t read_idx_ = 0;
  bool in_progress_ = false;  // 防止DoWrite被多开

  // 一次 sendmsg 合并的回复条数上限：调大以减少 sendmsg/syscall 次数。
  // 超过 UIO_FASTIOV(8) 时内核会 kmalloc iovec 数组，但减少 syscall 的
  // 收益通常远大于 iovec 分配代价（loopback 下 syscall 更贵）。
  static constexpr size_t kMaxWriteBatch = 64;
};

}  // namespace dfly

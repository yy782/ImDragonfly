#include "server/write_batcher.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <utility>

#include "util/thread.hpp"

namespace dfly {

void WriteBatcher::Enqueue(std::string&& s) {
  VLOG(3) << "[SEND] fd:" << socket_->fd() << " size:" << s.size()
          << " head:" << s.substr(0, s.find('\r'));
  queue_.push_back(std::move(s));
  StartIfNeeded();
}

void WriteBatcher::EnqueueBatch(std::vector<std::string>&& batch) {
  for (auto& s : batch) queue_.push_back(std::move(s));
  StartIfNeeded();
}

void WriteBatcher::StartIfNeeded() {
  if (in_progress_) return;
  in_progress_ = true;
  DoWrite();
}

cppcoro::AsyncTask WriteBatcher::DoWrite() {
  assert(util::Thread::current_tid() == loop_tid_);
  std::vector<std::string> batch;
  std::vector<struct iovec> rem;
  WriteCursor cursor;
  batch.reserve(kMaxWriteBatch);
  rem.reserve(kMaxWriteBatch);

  while (read_idx_ < queue_.size()) {
    size_t n = std::min(queue_.size() - read_idx_, kMaxWriteBatch);
    batch.clear();
    for (size_t i = 0; i < n; ++i) {
      batch.push_back(std::move(queue_[read_idx_++]));
    }

    cursor.reset();
    while (!cursor.done(batch)) {
      rem.clear();
      cursor.fill_iovec(rem, batch);
      struct msghdr msg = {};
      msg.msg_iov = rem.data();
      msg.msg_iovlen = rem.size();
      auto wr = co_await socket_->AsyncWriteV(&msg);
      if (wr <= 0) {
        LOG(WARNING) << "Write error on fd: " << socket_->fd()
                     << ", error: " << wr;
        // 清空整个队列：丢弃未发回复，避免连接卡死。
        queue_.clear();
        read_idx_ = 0;
        in_progress_ = false;
        co_return;
      }
      cursor.advance(batch, static_cast<size_t>(wr));
    }

    if (read_idx_ == queue_.size()) {
      queue_.clear();
      read_idx_ = 0;
    }
  }
  in_progress_ = false;
  co_return;
}

}  // namespace dfly

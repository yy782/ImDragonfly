#include "expire_index.hpp"

#include <glog/logging.h>

#include <utility>

namespace dfly {

void ExpireIndex::AddExpire(std::string_view key, uint64_t hash,
                            uint64_t exp_ms) {
  heap_.push_back(Entry{exp_ms, hash, std::string(key)});
  SiftUp(heap_.size() - 1);
}

ExpireIndex::Entry ExpireIndex::Pop() {
  DCHECK(!heap_.empty());
  Entry top = std::move(heap_.front());
  heap_.front() = std::move(heap_.back());
  heap_.pop_back();
  if (!heap_.empty()) SiftDown(0);
  return top;
}

void ExpireIndex::SiftUp(size_t pos) {
  Entry e = std::move(heap_[pos]);
  while (pos > 0) {
    const size_t parent = (pos - 1) / 2;
    if (!(e.exp_ms < heap_[parent].exp_ms)) break;
    heap_[pos] = std::move(heap_[parent]);
    pos = parent;
  }
  heap_[pos] = std::move(e);
}

void ExpireIndex::SiftDown(size_t pos) {
  Entry e = std::move(heap_[pos]);
  const size_t n = heap_.size();
  for (;;) {
    const size_t l = 2 * pos + 1;
    if (l >= n) break;
    size_t child = l;
    const size_t r = l + 1;
    if (r < n && heap_[r].exp_ms < heap_[l].exp_ms) child = r;
    if (!(heap_[child].exp_ms < e.exp_ms)) break;
    heap_[pos] = std::move(heap_[child]);
    pos = child;
  }
  heap_[pos] = std::move(e);
}

}  // namespace dfly

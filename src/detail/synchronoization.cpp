// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "synchronization.hpp"
#include "engine_shard_set.hpp"

namespace dfly {

ThreadLocalMutex::ThreadLocalMutex() {
    shard_ = EngineShard::tlocal();
}

ThreadLocalMutex::~ThreadLocalMutex() {
}

// void ThreadLocalMutex::lock() {
//     if (ServerState::tlocal()->serialization_max_chunk_size != 0) {
//         util::fb2::NoOpLock noop_lk_;
//         cond_var_.wait(noop_lk_, [this]() { return !flag_; });
//         flag_ = true;
//         locked_fiber_ = util::fb2::detail::FiberActive();
//     }
// }

// void ThreadLocalMutex::unlock() {
//     if (ServerState::tlocal()->serialization_max_chunk_size != 0) {
//         DCHECK_EQ(EngineShard::tlocal(), shard_);
//         flag_ = false;
//         cond_var_.notify_one();
//         locked_fiber_ = nullptr;
//     }
// }

}  // namespace dfly

#include "task_queue.hpp"

namespace dfly {

__thread unsigned TaskQueue::blocked_submitters_ = 0;

}  // namespace dfly

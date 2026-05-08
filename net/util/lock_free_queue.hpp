#pragma once

#include "lock_free_queues/mpmc_bounded_queue.h"
#include "lock_free_queues/mpsc_intrusive_queue.h"
#include "lock_free_queues/ProducerConsumerQueue.h"

namespace util{

template < typename T>
using mpmc_bounded_queue = base::mpmc_bounded_queue<T>;

template < typename T>
using MPSCIntrusiveQueue = base::MPSCIntrusiveQueue<T>;

template < typename T>
using ProducerConsumerQueue = folly::ProducerConsumerQueue<T>;



}
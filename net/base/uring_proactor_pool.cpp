// UringProactorPool — per-core proactor thread management

#include "uring_proactor_pool.hpp"

#include <glog/logging.h>
#include <pthread.h>
#include <cstdio>
#include <cstring>

namespace base {

UringProactorPool::UringProactorPool(int num_threads,
                                     UringConfig cfg,
                                     bool pin_cores)
    : pin_cores_(pin_cores), config_(cfg) {
    proactors_.resize(num_threads);

    LOG(INFO) << "ProactorPool: " << num_threads
              << " proactors created, pin_cores=" << pin_cores;
}

UringProactorPool::~UringProactorPool() {
    Shutdown();
}

void UringProactorPool::Start() {
    if (started_.exchange(true, std::memory_order_acquire)) {
        return;  // Already started
    }

    threads_.reserve(proactors_.size());

    for (int i = 0; i < static_cast<int>(proactors_.size()); ++i) {
        threads_.emplace_back(&UringProactorPool::ThreadLoop, this, i);

        if (pin_cores_) {
            // Pin thread to core i (one proactor per core)
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(i, &cpuset);
            int ret = pthread_setaffinity_np(threads_.back().native_handle(),
                                             sizeof(cpu_set_t), &cpuset);
            if (ret != 0) {
                LOG(WARNING) << "Failed to pin thread " << i
                             << " to core " << i << ": " << strerror(ret);
            }
        }
    }

    LOG(INFO) << "ProactorPool: all " << proactors_.size() << " threads started";
}

void UringProactorPool::Shutdown() {
    if (!started_.load(std::memory_order_acquire) ||
        shutdown_.exchange(true, std::memory_order_acquire)) {
        return;  // Not started or already shutting down
    }

    // Signal all proactors to stop
    for (auto& p : proactors_) {
        p->Shutdown();
    }

    // Join all threads
    for (auto& t : threads_) {
        if (t.joinable()) {
            t.join();
        }
    }

    LOG(INFO) << "ProactorPool: all threads shut down";
}

void UringProactorPool::ThreadLoop(int index) {
    // Set thread name for debugging
    char name[16];
    snprintf(name, sizeof(name), "proactor-%d", index);
    pthread_setname_np(pthread_self(), name);

    LOG(INFO) << "Proactor thread " << index << " started";
    proactors_[index] = std::make_shared<UringProactor>(config_, index);
    // Enter the proactor's event loop — blocks until Shutdown()
    proactors_[index]->Run();

    LOG(INFO) << "Proactor thread " << index << " exited";
}

}  // namespace base

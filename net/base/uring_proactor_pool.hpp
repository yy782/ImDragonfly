// UringProactorPool — manages a pool of per-CPU-core UringProactor instances.
// Each proactor runs on its own pinned thread, providing:
//   - Zero contention: one thread per proactor = SPSC ring access
//   - Cache locality: each shard's data lives on its own core
//   - Scalability: linear scaling with number of cores

#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "uring_proactor.hpp"

namespace base {

class UringProactorPool {
public:
    // Create a pool with `num_threads` proactors.
    // Each proactor thread is pinned to its own CPU core (0, 1, 2, ...).
    // If `pin_cores` is false, threads are not pinned.
    explicit UringProactorPool(int num_threads,
                               UringConfig cfg = {},
                               bool pin_cores = true);
    ~UringProactorPool();

    // Non-copyable, non-movable
    UringProactorPool(const UringProactorPool&) = delete;
    UringProactorPool& operator=(const UringProactorPool&) = delete;

    // Start all proactor threads.
    // After this call, each thread enters its Run() loop processing IO.
    void Start();
    
    // Alias for Start (used in existing code)
    void AsyncLoop() { Start(); }

    // Signal all proactors to shut down and join threads.
    // Blocks until all threads exit.
    void Shutdown();

    // Alias for Shutdown (used in existing code)
    void stop() { Shutdown(); }

    // Access the i-th proactor. Valid after construction, before Shutdown.
    UringProactorPtr GetProactor(int index) const {
        return proactors_[index];
    }

    // Alias for GetProactor (used in existing code)
    UringProactorPtr at(int index) const {
        return proactors_[index];
    }

    // Array-style access
    UringProactorPtr operator[](int index) const {
        return proactors_[index];
    }

    int PoolSize() const { return static_cast<int>(proactors_.size()); }
    size_t size() const { return proactors_.size(); }
    
    // Wait for all proactors to process a task
    template<typename F>
    void AwaitOnAll(F&& func) {
        for (auto& proactor : proactors_) {
            if (proactor) {
                // Dispatch the function to the proactor's thread
                bool success = proactor->DispatchBrief([proactor, &func]() {
                    func(proactor);
                });
                if (!success) {
                    LOG(ERROR) << "Failed to dispatch task to proactor";
                }
            }
        }
    }

private:
    void ThreadLoop(int index);

    std::vector<UringProactorPtr> proactors_;
    std::vector<std::thread> threads_;
    std::atomic<bool> started_{false};
    std::atomic<bool> shutdown_{false};
    bool pin_cores_;
    UringConfig config_;
};

}  // namespace base

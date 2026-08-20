#pragma once

#include <latch>
#include <memory>
#include <vector>

#include "uring_proactor.hpp"
#include "util/thread.hpp"

namespace base {

class UringProactorPool {
 public:
  UringProactorPool(uint32_t size, UringConfig cfg = {})
      : cfg_(cfg), proactors_(size) {
    for (std::size_t i = 0; i < proactors_.size(); ++i) {
      threads_.emplace_back();
    }
  }
  ~UringProactorPool() {
    for (UringProactor* p : proactors_) {
      delete p;
    }
  }

  UringProactorPool(const UringProactorPool&) = delete;
  UringProactorPool& operator=(const UringProactorPool&) = delete;

  void AsyncLoop() {
    std::string base_name = "proactor_thread_";
    for (std::size_t i = 0; i < proactors_.size(); ++i) {
      threads_[i] = std::make_unique<util::Thread>(
          (base_name + std::to_string(i)).c_str(), [this, i] {
            proactors_[i] = new UringProactor(cfg_, i);
            proactors_[i]->Run();
            // Run 返回后 delete 自家 proactor，避免 ~UringProactorPool 跨线程
            // delete
            delete proactors_[i];
            proactors_[i] = nullptr;
          });
    }
  }

  void stop() {
    DispatchBrief([](UringProactor* p) { p->Shutdown(); });

    for (std::size_t i = 0; i < proactors_.size(); ++i) {
      threads_[i]->join();
    }
    // threads_ join 后 proactors_ 已被各 worker 线程清空
  }

  size_t size() const { return proactors_.size(); }

  template <typename Func>
  void DispatchBrief(Func&& f) {
    for (std::size_t i = 0; i < size(); ++i) {
      auto p = proactors_[i];

      p->DispatchBrief([p, f]() mutable { f(p); });
    }
  }
  template <typename Func>
  void AwaitOnAll(Func&& func) {
    std::latch latch(size());
    auto cb = [func = std::forward<Func>(func),
               &latch](UringProactor* p) mutable {
      func(p);
      latch.count_down();
    };
    DispatchBrief(std::move(cb));
    latch.wait();
  }

  auto at(size_t index) const { return proactors_[index]; }

  auto operator[](size_t index) const { return at(index); }

 private:
  UringConfig cfg_;
  std::vector<UringProactor*> proactors_;
  std::vector<std::unique_ptr<util::Thread>> threads_;
};

}  // namespace base
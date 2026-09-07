#pragma once

#include <latch>
#include <memory>
#include <vector>

#include "sharding/shard.hpp"
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

  // 线程级启动同步：
  //   ready: 每个分片线程创建完 UringProactor 后 count_down，
  //          AsyncLoop() 返回前 wait 它 —— 保证返回时 proactors_[i] 全部有效；
  //   gate:  分片线程在进入 Run() 前 wait 它，
  //          由主线程完成 ShardPool::Init() 后 count_down 放行。
  // 两个同步器都在调用方（RedisServer::Start）栈上，生命周期覆盖本次启动。
  void AsyncLoop(std::latch* ready, std::latch* gate) {
    std::string base_name = "proactor_thread_";
    for (std::size_t i = 0; i < proactors_.size(); ++i) {
      threads_[i] = std::make_unique<util::Thread>(
          (base_name + std::to_string(i)).c_str(), [this, i, ready, gate] {
            proactors_[i] = new UringProactor(cfg_, i);
            ready->count_down();  // 通知主线程：本 proactor 已创建
            gate->wait();         // 等主线程完成 ShardPool::Init()
            proactors_[i]->Run();
            delete proactors_[i];
            proactors_[i] = nullptr;
          });
    }
    ready->wait();  // 等所有 proactor 创建完再返回
  }

  void stop() {
    DispatchBriefFromMain([](UringProactor* p) { p->Shutdown(); });

    for (std::size_t i = 0; i < proactors_.size(); ++i) {
      threads_[i]->join();
    }
  }

  size_t size() const { return proactors_.size(); }

  template <typename Func>
  void DispatchBriefFromMain(Func&& f) {
    for (std::size_t i = 0; i < size(); ++i) {
      auto p = proactors_[i];
      p->GetTaskQueue().TryAdd([p, f]() mutable { f(p); });
    }
  }

  template <typename Func>
  void AwaitOnAllFromMain(Func&& func) {
    std::latch latch(size());
    auto cb = [func = std::forward<Func>(func),
               &latch](UringProactor* p) mutable {
      func(p);
      latch.count_down();
    };
    DispatchBriefFromMain(std::move(cb));
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
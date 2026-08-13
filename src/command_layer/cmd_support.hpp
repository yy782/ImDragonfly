// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <glog/logging.h>

#include <concepts>
#include <coroutine>
#include <variant>

#include "command_layer/cmn_types.hpp"
#include "cppcoro/task.hpp"
#include "detail/conn_context.hpp"
#include "net/uring_proactor.hpp"
#include "sharding/engine_shard.hpp"
#include "sharding/op_status.hpp"
#include "transaction_layer/transaction.hpp"
#include "util/function.hpp"
#include "util/thread.hpp"

namespace dfly::cmd {
using ::cmn::CmdArgList;
template <typename RT>
using SingleHopSentinelT = util::FunctionRef<RT(Transaction*, EngineShard*)>;

auto SingleHopT(const auto& f)
    -> SingleHopSentinelT<decltype(f(nullptr, nullptr))> {
  return f;
}

class Coro;

struct CoroTask {
  using promise_type = Coro;
  CoroTask() = default;
  CoroTask(std::coroutine_handle<promise_type> coroutine) : coro_(coroutine) {}
  CoroTask(CoroTask&& o) noexcept : coro_(o.coro_) { o.coro_ = nullptr; }
  CoroTask(const CoroTask&) = delete;
  CoroTask& operator=(CoroTask&& o) noexcept {
    if (this != &o) {
      if (coro_) {
        coro_.destroy();
      }
      coro_ = o.coro_;
      o.coro_ = nullptr;
    }
    return *this;
  }
  CoroTask& operator=(const CoroTask&) = delete;
  ~CoroTask() {
    if (coro_) {
      coro_.destroy();
    }
  }

  bool await_ready() const noexcept { return false; }  // modify
  void await_suspend(std::coroutine_handle<> h) noexcept;
  void await_resume() const noexcept {}

  std::coroutine_handle<promise_type> coro_;
};

class Coro {
 public:
  Coro() = default;
  template <typename... Args>
  Coro(CommandContext* cmd_cntx, Args&&...) : cmd_cntx_{cmd_cntx} {}
  CoroTask get_return_object() noexcept {
    return CoroTask{std::coroutine_handle<Coro>::from_promise(*this)};
  }
  void return_void() {}
  void unhandled_exception() { std::terminate(); }
  template <typename RT>
  auto await_transform(SingleHopSentinelT<RT> callback) const {
    return SingleHopWaiterT{cmd_cntx_, callback};
  }

  auto initial_suspend() const noexcept { return std::suspend_always{}; }

  struct FinalAwaiter {
    bool await_ready() const noexcept { return false; }
    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<Coro> h) noexcept {
      auto cont = h.promise().cmd_cntx_->TakeContinuation();
      if (!cont) {
        LOG(FATAL) << " cont is null";
      }
      return cont;  // 交给压缩器来将协程投放到正确的线程上
    }
    void await_resume() const noexcept {}
  };
  auto final_suspend() const noexcept { return FinalAwaiter{}; }

  void SetContinuation(std::coroutine_handle<> h) noexcept {
    cmd_cntx_->SetContinuation(h);
  }

 private:
  template <typename RT>
  struct SingleHopWaiterT {
    SingleHopWaiterT(CommandContext* cmd_cntx, SingleHopSentinelT<RT> callback)
        : cmd_cntx_{cmd_cntx}, callback_{callback} {}

    bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<Coro> coro) noexcept {
      cmd_cntx_->tx()->SingleHopAsync(*this, coro);
      return;
    }

    void operator()(Transaction* tx, EngineShard* es) const {
      result_ = callback_(tx, es);
      return;
    }

    RT&& await_resume() noexcept { return std::move(result_); }

    CommandContext* cmd_cntx_;
    mutable SingleHopSentinelT<RT> callback_;
    mutable RT result_;
  };

  CommandContext* cmd_cntx_;
};

inline void CoroTask::await_suspend(std::coroutine_handle<> h) noexcept {
  coro_.promise().SetContinuation(h);
  coro_.resume();
}

}  // namespace dfly::cmd

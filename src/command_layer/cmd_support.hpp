// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once



#include <concepts>
#include <coroutine>
#include <variant>
#include "command_layer/cmn_types.hpp"
#include "util/function.hpp"
#include "sharding/op_status.hpp"
#include "conn_context.hpp"
#include "sharding/engine_shard.hpp"
#include "transaction_layer/transaction.hpp"
#include "cppcoro/task.hpp"

namespace dfly::cmd {
using ::cmn::CmdArgList;
template <typename RT> 
using SingleHopSentinelT = util::FunctionRef<RT(Transaction*, EngineShard*)>;


auto SingleHopT(const auto& f) -> SingleHopSentinelT<decltype(f(nullptr, nullptr))> {
    return f;
}

class Coro;

struct CoroTask {
    using promise_type = Coro;
    CoroTask(std::coroutine_handle<promise_type> coroutine) : coro_(coroutine) {}
    CoroTask(CoroTask&&) = delete;
    CoroTask(const CoroTask&) = delete;
    CoroTask& operator=(const CoroTask&) = delete;
    CoroTask& operator=(CoroTask&&) = delete;
    ~CoroTask() = default;


    std::coroutine_handle<promise_type> coro_;
};

class Coro {
public:
    Coro() = default;
    Coro(CmdArgList arg, CommandContext* cmd_cntx) : cmd_cntx_{cmd_cntx} {
      (void)arg;
    }
    CoroTask get_return_object() noexcept{
      return CoroTask{ std::coroutine_handle<Coro>::from_promise(*this) };
    }
    void return_void() {}
    void unhandled_exception() { 
          std::terminate(); 
    }
    template <typename RT>
    auto await_transform(SingleHopSentinelT<RT> callback) const {
        return SingleHopWaiterT{cmd_cntx_, callback};
    }

    auto initial_suspend() const noexcept {
        return std::suspend_never{};
    }
    auto final_suspend() const noexcept {
        return std::suspend_never{};
    }

private:
  template <typename RT> 
  struct SingleHopWaiterT  {
    SingleHopWaiterT(CommandContext* cmd_cntx,
                    SingleHopSentinelT<RT> callback)
        :  callback_{callback} {
          (void)cmd_cntx;
    }

    bool await_ready() const noexcept { 
      return false; 
    }

    void await_suspend(
      std::coroutine_handle<Coro> coro) noexcept
    {
      cmd_cntx_->tx()->Scheduling(coro, *this); // tx执行完恢复权柄
    }

    void operator()(Transaction* tx, EngineShard* es) const {
      result_ = callback_(tx, es);
      return;
    }

    RT&& await_resume() noexcept {
      return std::move(result_);
    }

    CommandContext* cmd_cntx_;
    mutable SingleHopSentinelT<RT> callback_;
    mutable RT result_;
  };



  CommandContext* cmd_cntx_;
};




}  // namespace dfly::cmd

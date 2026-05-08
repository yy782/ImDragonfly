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

using SingleHopSentinel = Transaction::RunnableType; // 一个回调

template <typename RT> 
using SingleHopSentinelT = util::FunctionRef<RT(Transaction*, EngineShard*)>;

SingleHopSentinel SingleHop(const auto& f) {
    return f;
}

auto SingleHopT(const auto& f) -> SingleHopSentinelT<decltype(f(nullptr, nullptr))> {
    return f;
}

class Coro : cppcoro::detail::task_promise_base<false>{
public:
  Coro(cmn::CmdArgList arg, CommandContext* cmd_cntx) : cmd_cntx_{cmd_cntx} {
  }

  cppcoro::task<void, cmd::Coro> get_return_object() noexcept{
    return cppcoro::task<void, cmd::Coro>{ std::coroutine_handle<Coro>::from_promise(*this) };
  }

  void return_value() noexcept {}
  void return_void() {}
  void result() noexcept {}

  void unhandled_exception() { 
        std::terminate(); 
  }
  
  auto await_transform(SingleHopSentinel callback) const {
      return SingleHopWaiterT{cmd_cntx_, callback};
  }

private:

  template <typename RT> 
  struct SingleHopWaiterT  {
    SingleHopWaiterT(CommandContext* cmd_cntx,
                    util::FunctionRef<RT(Transaction*, EngineShard*)> callback)
        :  callback_{callback} {
    }

    bool await_ready() const noexcept { 
      return false; 
    }

    void await_suspend(
      std::coroutine_handle<Coro> coro) noexcept
    {
      cmd_cntx_->tx()->Execute(coro, *this); // tx执行完恢复权柄
    }

    void operator()(Transaction* tx, EngineShard* es) const {
      result_ = callback_(tx, es);
      return;
    }

    RT&& await_resume() noexcept {
      return std::move(result_);
    }

    CommandContext* cmd_cntx_;
    util::FunctionRef<RT(Transaction*, EngineShard*)> callback_;
    mutable RT result_;
  };



  CommandContext* cmd_cntx_;
};

using CoroTask = cppcoro::task<void, cmd::Coro>;


}  // namespace dfly::cmd

// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once



#include <concepts>
#include <coroutine>
#include <variant>

#include "utils/function_ref.hpp"
#include "op_status.hpp"
#include "conn_context2.hpp"
#include "engine_shard.hpp"
#include "transaction.hpp"
#include "util/fibers/synchronization.h"

#include "cppcoro/task.hpp"

namespace dfly::cmd {

using SingleHopSentinel = Transaction::RunnableType; // 一个回调

template <typename RT> 
using SingleHopSentinelT = utils::FunctionRef<RT(Transaction*, EngineShard*)>;

SingleHopSentinel SingleHop(const auto& f) {
    return f;
}

auto SingleHopT(const auto& f) -> SingleHopSentinelT<decltype(f(nullptr, nullptr))> {
    return f;
}

struct SingleHopWaiter {
  bool await_ready() noexcept;
  void await_suspend(std::coroutine_handle<> handle) const noexcept;
  facade::OpStatus await_resume() const noexcept;

  CommandContext* cmd_cntx;
  Transaction::RunnableType callback;
  boost::intrusive_ptr<Transaction> tx_keepalive_ = nullptr;
};


template <typename RT> struct SingleHopWaiterT : public SingleHopWaiter {
  static_assert(std::is_base_of_v<facade::OpResultBase, RT>);

  SingleHopWaiterT(CommandContext* cmd_cntx,
                   absl::FunctionRef<RT(Transaction*, EngineShard*)> callback)
      : SingleHopWaiter{cmd_cntx, *this}, callback{callback} {
  }

  OpStatus operator()(Transaction* tx, EngineShard* es) const {
    result = callback(tx, es);
    return result.status();
  }

  RT&& await_resume() noexcept {
    return std::move(result);
  }

  absl::FunctionRef<RT(Transaction*, EngineShard*)> callback;
  mutable RT result;
};





class Coro : cppcoro::detail::task_promise_base<false>{
public:
  Coro(facade::CmdArgList arg, CommandContext* cmd_cntx) : cmd_cntx{cmd_cntx} {
  }

  task<void> get_return_object() noexcept{
    return task<void>{ std::coroutine_handle<Coro>::from_promise(*this) };
  }

  void return_value() noexcept {}

  void result() noexcept {}


  auto await_transform(SingleHopSentinel callback) const {
      return SingleHopWaiter{cmd_cntx_, callback};
  }

private:

  struct SingleHopWaiter {
      bool await_ready() const noexcept { 
        return false; 
      }
      void await_suspend(
        std::coroutine_handle<Coro> coro) noexcept
      {
        tx->Execute(coro, callback_, cmd_cntx_); // tx执行完恢复权柄
      }
      facade::OpStatus await_resume() noexcept {
        return cmd_cntx_->result();
      }
      CommandContext* cmd_cntx_;
      Transaction::RunnableType callback_;
      // boost::intrusive_ptr<Transaction> tx_keepalive_ = nullptr;
  };
  CommandContext* cmd_cntx_;
};




}  // namespace dfly::cmd

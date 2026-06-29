#pragma once
#include <cstdint>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <span> 
#include <utility>
#include <coroutine>
#include <variant>
#include <gtest/gtest.h>
#include <atomic>
#include <functional>
#include <optional>
#include <span>

#include "detail/tx_base.hpp"
#include "detail/common_types.hpp"
#include "util/function.hpp"
#include "command_layer/command_registry.hpp"
#include "command_layer/cmn_types.hpp"
#include "detail/conn_context.hpp"
#include "sharding/op_status.hpp"
#include "detail/intent_lock.hpp"
#include "detail/tx_queue.hpp"
#include "cppcoro/async_task.hpp"
#include "cppcoro/task.hpp"
namespace dfly{
using ::cmn::CmdArgList;

class CommandId;
class Namespace;
class RedisSession;
class EngineShard;

class Transaction{
public:
  enum class State {
    IDLE,
    MULTI,
    EXEC
  };

  Transaction(const Transaction&) = delete;
  void operator=(const Transaction&) = delete;

  ~Transaction();

  friend void intrusive_ptr_add_ref(Transaction* trans) noexcept {
    trans->use_count_.fetch_add(1, std::memory_order_relaxed);
  }

  friend void intrusive_ptr_release(Transaction* trans) noexcept {
    if (1 == trans->use_count_.fetch_sub(1, std::memory_order_release)) {
      std::atomic_thread_fence(std::memory_order_acquire);
      delete trans;
    }
  }
  using RunnableType = util::FunctionRef<void(Transaction*, EngineShard*)>;

  enum LocalMask : uint16_t {
    ACTIVE = 1,
    OPTIMISTIC_EXECUTION = 1 << 1, // 乐观执行
    OUT_OF_ORDER = 1 << 2,  // 顺序执行
    KEYLOCK_ACQUIRED = 1 << 3, // 锁已获取
    WAS_SUSPENDED = 1 << 4, // 已挂起
    AWAKED_Q = 1 << 5, // 已唤醒队列
  };


  explicit Transaction(const CommandId* cid);

  void InitByArgs(ConnectionContext* conn_cntx, CmdArgList args);

  KeyLockArgs GetLockArgs(ShardId sid) const;

  bool IsActive(ShardId sid) const;

  bool IsScheduled() const {
    return coordinator_state_ & COORD_SCHED;
  }


  const DbContext& GetDbContext() const {
    return db_cntx_;
  }
  DbContext& GetDbContext() {
    return db_cntx_;
  }

  const Namespace& GetNamespace() const {
    return *namespace_;
  }

  DbSlice& GetDbSlice(ShardId sid) const;

  CommandContext& GetCommandContext() {
    return cmd_cntx_;
  }
  const CommandContext& GetCommandContext() const {
    return cmd_cntx_;
  }

  struct Slice{
    ShardId unique_shard_id;
    Transaction* tx;
    std::vector<uint32_t> keyIds;
    CmdArgList lists;
    uint16_t local_mask = 0;
    TxQueue::Iterator pq_pos = TxQueue::kEnd;
    DbSlice& GetDbSlice() {
      return tx->GetDbSlice(unique_shard_id);
    }
    DbContext& GetDbContext() {
      return tx->GetDbContext();
    } 
    const DbSlice& GetDbSlice() const {
      return tx->GetDbSlice(unique_shard_id);
    }
    const DbContext& GetDbContext() const {
      return tx->GetDbContext();
    } 

    struct Iterator {
      using ArgsIndexPair = std::pair<std::string_view, uint32_t>;
      using iterator_category = std::input_iterator_tag;
      using value_type = ArgsIndexPair;
      using difference_type = ptrdiff_t;
      using pointer = value_type*;
      using reference = value_type&;

      ArgsIndexPair pa;
      const Slice* slice;
      uint32_t idx;
      Iterator(std::string_view key, uint32_t keyId, const Slice* sl, uint32_t id) : pa(key, keyId), slice(sl), idx(id) {}
      bool operator==(const Iterator& o) const { return pa == o.pa; }
      bool operator!=(const Iterator& o) const { return !(*this == o); }
      ArgsIndexPair& operator*() { return pa; }            
      ArgsIndexPair* operator->() { return &pa; }
      Iterator& operator++() {
        if (idx + 1 >= slice->keyIds.size()) {
          *this = slice->cend();
          return *this;
        }else {
          uint32_t nextId = slice->keyIds[++idx];
          std::string_view key = slice->lists[nextId];
          pa = ArgsIndexPair(key, nextId);
          return *this;                    
        }
      }      
    };

    Iterator cbegin() const {
      if (keyIds.empty()) {
        return cend();
      }else {
        uint32_t keyId = keyIds[0];
        std::string_view key = lists[keyId]; 
        return Iterator(key, keyId, this, 0);
      }
    }

    Iterator cend() const {
      uint32_t keyId = lists.size();
      std::string_view key;
      return Iterator(key, keyId, this, keyIds.size());
    }

    Iterator begin() const {
      return cbegin();
    }

    Iterator end() const {
      return cend();
    }   
  };

  Slice& GetSlice(ShardId id) {
    assert(id < Slices_.size());
    return Slices_[id];
  }

  DbIndex GetDbIndex() const {
    return db_cntx_.GetDbIndex();
  }
  uint32_t GetKeyNum() {
    return key_num_;
  }

  const CommandId* GetCId() const {
    return cid_;
  }

  uint64_t txid() const {
    return txid_;
  }

  IntentLock::Mode LockMode() const;

  std::string_view Name() const;




  State GetState() const { return state_; }
  uint8_t GetCoordinatorState() const { return coordinator_state_; }
  void SetState(State new_state) { state_ = new_state; }

  struct QueuedCommand {
    const CommandId* cid;
    std::vector<std::string> args;
    std::vector<std::string_view> ViewArgs;
    CmdArgList GetCmdArgList() const {
      CmdArgList argsList(ViewArgs);
      return argsList;
    }
  };

  void QueueCommand(const CommandId* cid, CmdArgList args);
  const std::vector<QueuedCommand>& GetQueuedCommands() const { return queued_commands_; }
  void ClearQueuedCommands() { queued_commands_.clear(); }

  template<typename Cb>
  void AddWatchKey(std::string_view key, Cb&& cb) { return conn_cntx_->AddWatchKey(key, std::move(cb)); }

  auto ClearWatchKeys() { conn_cntx_->ClearWatchKeys(); }

  const std::unordered_set<std::string_view>& GetWatchKeys() const { return conn_cntx_->GetWatchKeys(); }
  bool HasWatchKeys() const { return conn_cntx_->HasWatchKeys(); }
  
  bool IsDirty() const { return conn_cntx_->IsDirty(); }

  bool collectMultiRes(const std::string& s) {
    MultiRes_.push_back(s);
    return MultiReady();
  }
  void ClearMultiRes() {
    MultiRes_.clear();
  }
  std::vector<std::string> SwapOrClearMultiRes(std::vector<std::string> vec = {}) {
    MultiRes_.swap(vec);
    return vec;
  }
  bool MultiReady() {
    return MultiRes_.size() == queued_commands_.size();
  }
  void startMulti() {
    SetState(Transaction::State::MULTI);
  }
  void FinishOrDiscardMulti() {
    SetState(Transaction::State::IDLE);
    ClearQueuedCommands();
    ClearWatchKeys();
    ClearMultiRes();
  }
  ConnectionContext* GetConnectionContext() {
    return conn_cntx_;
  }

  bool RunInShard(EngineShard* shard);
  cppcoro::AsyncTask Scheduling(std::coroutine_handle<> handle, RunnableType&& cb);

  // 协调器状态
  enum CoordinatorState : uint8_t {
    COORD_SCHED = 1, // 协调器已调度
    COORD_CONCLUDING = 1 << 1, // 协调器正在结束
    COORD_CANCELLED = 1 << 2, // 协调器已取消
  };

private:

  cppcoro::task<void> ScheduleInternal();
  bool ScheduleInShard(EngineShard* shard, bool execute_optimistic);
  void FinishHop();
  cppcoro::AsyncTask Finish();
  void RunCallback(EngineShard* shard);




  void InitSlice();

  void EnableAllShards();


  bool LockMultiShardCb(const KeyLockArgs& lock_args, EngineShard* shard);
  void UnlockMultiShardCb(const KeyLockArgs& lock_args, EngineShard* shard);

  unsigned SidToId(ShardId sid) const {
    return sid < Slices_.size() ? sid : 0;
  }

  template<typename F>
  cppcoro::task<void> IterateActiveShards(F&& f) {
    util::BlockingCounter counter(unique_shard_cnt_);
    auto cb = [counter, f](auto& sd, auto i) mutable -> cppcoro::AsyncTask {
      
      co_await f(sd, i);
      counter->Dec();
      co_return;
    };
    if (unique_shard_cnt_ == 1) {
      shard_set->Add(unique_shard_id_, [this, cb]() mutable {
        cb(Slices_[SidToId(unique_shard_id_)], unique_shard_id_);
      });
    } else {
      for (ShardId i = 0; i < Slices_.size(); ++i) {
        auto shard_id = Slices_[i].unique_shard_id;
        if (!(Slices_[i].local_mask & ACTIVE)) {
          continue;
        }
        shard_set->Add(shard_id, [this, cb, shard_id]() mutable {
          cb(Slices_[SidToId(shard_id)], shard_id);
        });
      }
    }
    co_await counter->Wait();
    co_return;
  }

  struct MultiData {

  };

  std::atomic_uint32_t run_barrier_{0};
  std::vector<Slice> Slices_;
  CmdArgList full_args_;
  RunnableType cb_;
  std::coroutine_handle<> coro_handle_;
  const CommandId* cid_ = nullptr;
  std::unique_ptr<MultiData> multi_;
  uint64_t txid_{0};
  const Namespace* namespace_{nullptr};
  std::atomic_uint32_t use_count_{0};
  uint32_t unique_shard_cnt_{0};
  ShardId unique_shard_id_{kInvalidSid};
  uint8_t coordinator_state_ = 0;
  State state_ = State::IDLE;
  uint32_t key_num_ = 0;
  std::vector<QueuedCommand> queued_commands_;
  std::vector<std::string> MultiRes_;
  ConnectionContext* conn_cntx_;
  DbContext db_cntx_;
  CommandContext cmd_cntx_;

public:
  std::atomic_int debug_re = 0;
};

}

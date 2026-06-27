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

  enum MultiMode : uint8_t {
    NOT_DETERMINED = 0,
    GLOBAL = 1,
    LOCK_AHEAD = 2,
    NON_ATOMIC = 3,
  };

  enum MultiRole {
    DEFAULT = 0,
    SQUASHER = 1,
    SQUASHED_STUB = 2,
  };

  enum LocalMask : uint16_t {
    ACTIVE = 1,
    OPTIMISTIC_EXECUTION = 1 << 1,
    OUT_OF_ORDER = 1 << 2,
    KEYLOCK_ACQUIRED = 1 << 3,
    WAS_SUSPENDED = 1 << 4,
    AWAKED_Q = 1 << 5,
  };


  explicit Transaction(const CommandId* cid);

  void InitByArgs(ConnectionContext* conn_cntx, CmdArgList args);

  void Execute(RunnableType cb, bool conclude);

  OpStatus ScheduleSingleHop(RunnableType cb);

  void Conclude();

  bool RunInShard(EngineShard* shard, bool allow_q_removal);


  void Scheduling(std::coroutine_handle<> handle, RunnableType&& cb);


  KeyLockArgs GetLockArgs(ShardId sid) const;

  bool IsActive(ShardId sid) const;

  bool IsMulti() const {
    return bool(multi_);
  }

  bool IsScheduled() const {
    return coordinator_state_ & COORD_SCHED;
  }

  MultiMode GetMultiMode() const {
    return multi_ ? multi_->mode : MultiMode::NOT_DETERMINED;
  }

  bool IsAtomicMulti() const {
    return multi_ && (multi_->mode == LOCK_AHEAD || multi_->mode == GLOBAL);
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
    bool is_armed = false;

    uint32_t slice_start = 0;
    uint32_t slice_count = 0;

    uint32_t fp_start = 0;
    uint32_t fp_count = 0;

    TxQueue::Iterator pq_pos = TxQueue::kEnd;

    struct Stats {
      unsigned total_runs = 0;
    } stats;

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

  uint32_t GetUniqueShardCnt() const {
    return unique_shard_cnt_;
  }

  ShardId GetUniqueShard() const;

  const std::set<std::pair<ShardId, LockFp>>& GetMultiFps() const;

  State GetState() const { return state_; }
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
    EXPECT_TRUE(MultiRes_.size() <= queued_commands_.size());
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

  void setFatherTransaction(Transaction* fa) {
    fa_ = fa;
  }

  template<typename... Args>
  Transaction*& CreateSubTransaction(Args&&... args) {
    SubTransactions_.emplace_back(new Transaction(std::forward<Args>(args)...));
    SubTransactions_.back()->setFatherTransaction(this);
    return SubTransactions_.back();
  }
  void ClearSubTransaction() {
    for (auto& t : SubTransactions_) {
      delete t;
    }
    SubTransactions_.clear();
  }

  ConnectionContext* GetConnectionContext() {
    return conn_cntx_;
  }

private:

  struct MultiData {
    MultiRole role = MultiRole::DEFAULT;
    MultiMode mode = MultiMode::NOT_DETERMINED;
    std::optional<IntentLock::Mode> lock_mode;

    std::set<std::pair<ShardId, LockFp>> tag_fps;

    bool concluding = false;

    unsigned cmd_seq_num = 0;
  };

  enum CoordinatorState : uint8_t {
    COORD_SCHED = 1,
    COORD_CONCLUDING = 1 << 1,
    COORD_CANCELLED = 1 << 2,
  };

  void InitSlice();

  void EnableAllShards();

  void PrepareMultiFps(CmdArgList keys);

  void ScheduleInternal();

  bool ScheduleInShard(EngineShard* shard, bool execute_optimistic);

  void DispatchHop();

  void FinishHop();

  void RunCallback(EngineShard* shard);

  bool LockMultiShardCb(std::span<const LockFp> fps, EngineShard* shard);
  void UnlockMultiShardCb(std::span<const LockFp> fps, EngineShard* shard);


  bool IsActiveMulti() const {
    return multi_ && multi_->role != SQUASHED_STUB;
  }

  unsigned SidToId(ShardId sid) const {
    return sid < Slices_.size() ? sid : 0;
  }

  template <typename F> void IterateShards(F&& f) {
    if (unique_shard_cnt_ == 1) {
      f(Slices_[SidToId(unique_shard_id_)], unique_shard_id_);
    } else {
      for (ShardId i = 0; i < Slices_.size(); ++i) {
        f(Slices_[i], i);
      }
    }
  }

  template <typename F> void IterateActiveShards(F&& f) {
    IterateShards([&f](auto& sd, auto i) {
      if (sd.local_mask & ACTIVE)
        f(sd, i);
    });
  }
  std::atomic_uint32_t run_barrier_{0};
  std::vector<Slice> Slices_;
  std::vector<LockFp> kv_fp_;
  CmdArgList full_args_;
  RunnableType cb_;
  std::coroutine_handle<> coro_handle_;
  const CommandId* cid_ = nullptr;
  std::unique_ptr<MultiData> multi_;
  uint64_t txid_{0};
  const Namespace* namespace_{nullptr};
  std::atomic_uint32_t use_count_{0};
  std::vector<Transaction*> SubTransactions_;
  uint32_t unique_shard_cnt_{0};
  ShardId unique_shard_id_{kInvalidSid};
  OpStatus block_cancel_result_ = OpStatus::OK;
  uint8_t coordinator_state_ = 0;
  uint32_t key_num_ = 0;
  State state_ = State::IDLE;
  std::vector<QueuedCommand> queued_commands_;
  std::vector<std::string> MultiRes_;
  Transaction* fa_;

  ConnectionContext* conn_cntx_;
  DbContext db_cntx_;
  CommandContext cmd_cntx_;


  util::BlockingCounter block_counter_;
};

}

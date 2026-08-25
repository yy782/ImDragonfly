#pragma once

#include <glog/logging.h>

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "command_layer/command_id.hpp"
#include "detail/common_types.hpp"
#include "detail/intent_lock.hpp"
#include "detail/op_status.hpp"
#include "detail/tx_queue.hpp"
#include "sharding/synchronization.hpp"
#include "util/cppcoro/async_task.hpp"
#include "util/cppcoro/task.hpp"
#include "util/function.hpp"
#include "util/intrusive_ptr.hpp"

namespace dfly {

class Shard;

class Transaction final
    : public util::intrusive_ref_counter<Transaction,
                                         util::thread_safe_counter> {
 public:
  enum ShardFlag : uint16_t {
    kUncontended = 1 << 0,  // 乱序执行
    kOptimistic = 1 << 1,
  };

  enum TxState : uint8_t {
    kPipeline = 1 << 0,
    kScheduling = 1 << 1,
    kDistributed = 1 << 2,
    kFinished = 1 << 3,
  };

  TxState State() const { return static_cast<TxState>(state_); }
  bool IsPipeline() const { return (state_ & TxState::kPipeline) != 0; }
  std::string StateName() const;

  enum class ScheduleResult : uint8_t {
    kGranted,
    kQueued,
    kRejected,
  };

  using Callback = util::FunctionRef<void(Transaction*, Shard*)>;

  Transaction();
  explicit Transaction(const CommandId* cid);
  ~Transaction();

  void Init(::dfly::DbIndex db, ::dfly::CmdArgList args);
  // pipeline复用
  void ResetForReuse(const CommandId* cid);

#ifdef UNIT_TESTS
  int id = 0;
  void set_txid(TxId v) { txid_ = v; }
#endif

  TxId txid() const { return txid_; }
  bool IsGlobal() const;
  bool IsReadOnly() const { return (cid_->opt_mask() & CO::READONLY) != 0; }
  IntentLock::Mode LockMode() const {
    return IsReadOnly() ? IntentLock::SHARED : IntentLock::EXCLUSIVE;
  }
  size_t ShardCount() const { return active_shard_count_; }

  ShardId SoleShard() const {
    return active_shard_count_ == 1 ? involved_.single.sid : kInvalidSid;
  }
  std::string_view Name() const { return cid_ ? cid_->name() : "null-command"; }

  cppcoro::AsyncTask Run(Callback cb, std::coroutine_handle<> resume);

  ScheduleResult ScheduleOnShard(Shard& shard, bool allow_optimistic);
  void ExecuteOnShard(Shard& shard);
  bool RollbackOnShard(Shard& shard);
  bool AllowOn(ShardId sid);
  bool AllowOnIf(ShardId sid, uint16_t need_flags);
  bool IsAllowedOn(ShardId sid) const;
  DbContext GetDbContext() const;
  DbIndex DbIndex() const { return db_; }
  uint64_t TimeMs() const { return start_ms_; }
  ::dfly::CmdArgList Args() const { return args_; }
  size_t GetKeyNum() const { return key_num_; }  // 总键数（BuildKeyMap 时累计）
  KeyLockArgs LockArgsOn(ShardId sid) const;
  class Slice {
   public:
    Slice(std::span<const IndexSlice> slices, unsigned step,
          ::dfly::CmdArgList args)
        : slices_(slices), step_(step), args_(args) {}
    struct Iterator {
      const IndexSlice* cur_ = nullptr;
      const IndexSlice* end_ = nullptr;
      unsigned idx_ = 0;
      unsigned step_ = 1;
      ::dfly::CmdArgList args_;
      std::pair<std::string_view, unsigned> val_;

      const std::pair<std::string_view, unsigned>& operator*() const {
        return val_;
      }
      Iterator& operator++() {
        idx_ += step_;
        if (idx_ >= cur_->second) {
          ++cur_;
          if (cur_ < end_) {
            idx_ = cur_->first;
          } else {
            return *this;
          }
        }
        val_ = {args_[idx_], idx_};
        return *this;
      }

      bool operator!=(const Iterator& o) const { return idx_ != o.idx_; }
    };

    Iterator begin() const {
      if (slices_.empty()) return end();
      Iterator it{&slices_.front(),
                  &slices_.back() + 1,
                  slices_.front().first,
                  step_,
                  args_,
                  {}};
      it.val_ = {args_[it.idx_], it.idx_};
      return it;
    }

    Iterator end() const {
      if (slices_.empty()) return {nullptr, nullptr, 0, step_, args_, {}};
      return {&slices_.back() + 1,
              &slices_.back() + 1,
              slices_.back().second,
              step_,
              args_,
              {}};
    }

    std::span<const IndexSlice> slices_;
    unsigned step_ = 1;
    ::dfly::CmdArgList args_;
  };

  Slice GetSlice(ShardId sid) const;

 private:
  struct alignas(64) PerShardData {
    PerShardData() = default;
    PerShardData(PerShardData&& o) noexcept
        : slices(std::move(o.slices)), fps(std::move(o.fps)) {}

    uint16_t flags = 0;
    std::atomic_bool is_armed = false;
    std::vector<IndexSlice> slices;
    std::vector<LockFp> fps;
    TxQueue::Iterator queue_pos = TxQueue::kEnd;
  };

  void InitBase(::dfly::DbIndex db, ::dfly::CmdArgList args);
  void BuildKeyMap();
  void MarkAllShards();
  bool CanRunInlined() const;
  size_t IndexInvolved(ShardId sid) const;
  bool IsInvolved(ShardId sid) const;
  ShardId InvolvedAt(size_t idx) const;
  PerShardData& ShardDataAt(size_t idx);
  const PerShardData& ShardDataAt(size_t idx) const;
  std::vector<ShardId>& ActivateMany();
  cppcoro::task<> Schedule();
  void Distribute();
  void InvokeCallback(Shard& shard);
  bool AcquireLocks(Shard& shard, PerShardData& sd);
  void ReleaseLocks(Shard& shard, PerShardData& sd);
  void SetStartTime();

  const CommandId* cid_ = nullptr;
  ::dfly::DbIndex db_ = 0;
  ::dfly::CmdArgList args_;
  uint64_t start_ms_ = 0;

  union InvolvedData {
    struct {
      ShardId sid = kInvalidSid;
      PerShardData sd;
    } single;
    struct {
      std::vector<ShardId> sids;
      std::vector<PerShardData> sds;
    } many;

    InvolvedData() : single{} {}
    ~InvolvedData() {}
    InvolvedData(const InvolvedData&) = delete;
    InvolvedData& operator=(const InvolvedData&) = delete;
    InvolvedData(InvolvedData&&) = delete;
    InvolvedData& operator=(InvolvedData&&) = delete;
  } involved_;
  uint32_t active_shard_count_ = 0;
  size_t key_num_ = 0;

  TxId txid_ = 0;
  bool global_ = false;
  uint8_t state_ = 0;

  struct CoroutineCtx {
    CoroutineCtx() = default;
    CoroutineCtx(Callback callback, std::coroutine_handle<> handle,
                 ShardId owner, uint16_t count) {
      Start(std::move(callback), handle, owner, count);
    }
    void Start(Callback callback, std::coroutine_handle<> handle, ShardId owner,
               uint16_t count) {
      cb = std::move(callback);
      resume = handle;
      owner_ = owner;
      pending.store(count, std::memory_order_relaxed);
    }
    void Clear() {
      cb = nullptr;
      resume = nullptr;
      owner_ = kInvalidSid;
      pending.store(0, std::memory_order_relaxed);
      need_resume.store(false, std::memory_order_relaxed);
      has_resume.store(false, std::memory_order_relaxed);
    }

    Callback cb;
    std::coroutine_handle<> resume;
    std::atomic<uint16_t> pending{0};
    std::atomic<bool> need_resume{false};
    std::atomic<bool> has_resume{false};
    ShardId owner_ = kInvalidSid;
    void FinishCallback();

  } coro_ctx_;

  void
  ResumeIfNeed();  // 选择多个观察者观察命令是否完成，而不是回调完成就恢复协程，保证事务调度完才恢复协程

  BlockingCounter barrier_{0};
};

}  // namespace dfly

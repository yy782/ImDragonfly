// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <glog/logging.h>

#include <memory>
#include <optional>

#include "absl/container/inlined_vector.h"
#include "command_layer/command_registry.hpp"
#include "detail/tx_base.hpp"
#include "detail/tx_queue.hpp"
#include "sharding/engine_shard_set.hpp"
#include "sharding/op_status.hpp"
#include "sharding/synchronization.hpp"
#include "util/function.hpp"
using namespace util;
namespace dfly {

using namespace cmn;
using namespace ::cmn;

using facade::OpResult;
using facade::OpStatus;

class Transaction : public std::enable_shared_from_this<Transaction> {
  Transaction(const Transaction&) = delete;
  void operator=(const Transaction&) = delete;

 public:
  using time_point = ::std::chrono::steady_clock::time_point;
  using RunnableType = util::FunctionRef<void(Transaction* t, EngineShard*)>;

  static constexpr std::nullopt_t kShardArgs{std::nullopt};

  // 当前 shard 上的状态标志位。
  enum LocalMask : uint16_t {
    // 该事务涉及此 shard（key hash 落到此 shard，或通过
    // InitGlobal/EnableAllShards 全局激活）。
    // 在 InitByKeys 中设置；unique_shard_cnt_ > 0 的 shard 上此位必为 1。
    // IsActive() 以此位判断是否需要向该 shard 分发调度/执行/取消操作。
    ACTIVE = 1,

    // 回调已在调度阶段乐观执行（无需进入 TxQueue 排队）。
    // 当 lock_granted && execute_optimistic 为真时，ScheduleInShard
    // 内联执行回调，
    // 后续 Execute() 跳过此 shard 不再 poll。RunCallback 返回后或下一轮
    // ScheduleInShard 时清除。
    OPTIMISTIC_EXECUTION = 1 << 1,

    // 所有 key 级锁无竞争获取成功，可绕过 TxQueue 排序直接执行。
    // ScheduleInShard 中 lock_granted 为真时置位；concluding hop
    // 结束时（RunInShard）清除。
    // 仅在 KEYLOCK_ACQUIRED 已置位时有效。
    OUT_OF_ORDER = 1 << 2,

    // 已持有 fingerprint 级 key 锁。ScheduleInShard 中 Acquire() 成功后置位。
    // RunInShard 中据此判断是否需要释放锁；CancelShardCb/expire
    // 路径也据此决定是否 Release()。
    // 全局事务不置此位（走 shard_lock 而非 fp lock）。
    KEYLOCK_ACQUIRED = 1 << 3,

    // // 粘性标志：事务已通过 WatchInShard() 注册到 BlockingController 等待（如
    // BLPOP）。
    // // 一旦设置永不撤销，标记此事务曾作为阻塞事务存在。与 AWAKED_Q
    // 配合区分"首次注册等待"
    // // 与"被唤醒后重执行"两种状态。
    // WAS_SUSPENDED = 1 << 4,

    // // 事务被 NotifySuspended() 从阻塞等待中唤醒（目标 key 已就绪）。
    // // 每次唤醒最多设置一次；RunInShard 中据此驱动 BlockingController
    // 清理（RemovedWatched），
    // // 并保证事务挂起期间不释放锁。
    // AWAKED_Q = 1 << 5,
  };

  Transaction(const CommandId* cid = nullptr);  // todo 使用explicit
  ~Transaction();

  OpStatus InitByArgs(const Namespace* ns, DbIndex index, CmdArgList args);

  cppcoro::AsyncTask SingleHopAsync(RunnableType cb,
                                    std::coroutine_handle<> handle);

  bool RunInShard(EngineShard* shard, std::string context);

  KeyLockArgs GetLockArgs(ShardId sid) const;

  uint16_t DisarmInShard(ShardId sid);

  std::pair<uint16_t, bool> DisarmInShardWhen(ShardId sid, uint16_t req_flags);

  bool IsActive(ShardId sid) const;

  TxId txid() const { return txid_; }

  IntentLock::Mode LockMode() const;

  std::string_view Name() const;

  uint32_t GetUniqueShardCnt() const { return unique_shard_cnt_; }

  ShardId GetUniqueShard() const;

  bool IsScheduled() const { return coordinator_state_ & COORD_SCHED; }

  DbContext GetDbContext() const {
    return DbContext{namespace_, db_index_, time_now_ms_};
  }

  TxQueue::Iterator& GetTxQueuePos(ShardId sid) {
    return shard_data_[sid].pq_pos;
  }

  const Namespace& GetNamespace() const { return *namespace_; }

  DbSlice& GetDbSlice(ShardId sid) const;

  DbIndex GetDbIndex() const { return db_index_; }

  const CommandId* GetCId() const { return cid_; }

  class Slice {
   public:
    struct Iterator {
      const IndexSlice* cur_;
      const IndexSlice* end_;
      unsigned idx_;
      unsigned step_;
      CmdArgList args_;
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
          }
        }
        val_ = {args_[idx_], idx_};  // 会越界
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

    Transaction* trans_ = nullptr;
    ShardId sid_ = -1;

    DbSlice& GetDbSlice() const { return trans_->GetDbSlice(sid_); }
    DbContext GetDbContext() const { return trans_->GetDbContext(); }

    absl::Span<const IndexSlice> slices_;
    unsigned step_ = 1;
    CmdArgList args_;
  };

  const Slice& GetSlice(ShardId sid) const { return key_slices_[SidToId(sid)]; }

  unsigned GetKeyNum() const { return kv_fp_.size(); }

#if !defined(NDEBUG) || defined(UNIT_TESTS)
  int id;
#endif
#ifdef UNIT_TESTS
  void set_txid(int new_id) { txid_ = new_id; }
#endif
 private:
  struct alignas(64) PerShardData {
    PerShardData() {}
    PerShardData(PerShardData&& /*other*/) noexcept {}

    uint16_t local_mask = 0;

    std::atomic_bool is_armed = false;

    uint32_t slice_start = 0;
    uint32_t slice_count = 0;

    uint32_t fp_start = 0;
    uint32_t fp_count = 0;

    TxQueue::Iterator pq_pos = TxQueue::kEnd;

    char pad[64 - 2 - 1 - 1 - 5 * sizeof(uint32_t)];
  };

  static_assert(sizeof(PerShardData) == 64);

  enum CoordinatorState : uint8_t {
    COORD_SCHED = 1,
    COORD_CONCLUDING = 1 << 1,
    COORD_CANCELLED = 1 << 2,
  };

  struct PerShardCache {
    std::vector<IndexSlice> slices;
    unsigned key_step = 1;

    void Clear() { slices.clear(); }
  };

  void InitBase(const Namespace* ns, DbIndex dbid, CmdArgList args);

  void InitByKeys(const KeyIndex& keys);

  void BuildShardIndex(const KeyIndex& keys, std::vector<PerShardCache>* out);

  void InitShardData(absl::Span<const PerShardCache> shard_index,
                     size_t num_args);

  void StoreKeysInArgs(const KeyIndex& key_index);

  cppcoro::task<> ScheduleInternal();

  bool ScheduleInShard(EngineShard* shard, bool execute_optimistic,
                       std::string context);

  void DispatchHop();

  void FinishHop();

  void RunCallback(EngineShard* shard, std::string context);

  bool CancelShardCb(EngineShard* shard);

  void InitTxTime();

  bool CanRunInlined() const;

  unsigned SidToId(ShardId sid) const {
    return sid < shard_data_.size() ? sid : 0;
  }

  template <typename F>
  void IterateShards(F&& f) {
    if (unique_shard_cnt_ == 1) {
      f(shard_data_[SidToId(unique_shard_id_)], unique_shard_id_);
    } else {
      for (ShardId i = 0; i < shard_data_.size(); ++i) {
        f(shard_data_[i], i);
      }
    }
  }

  template <typename F>
  void IterateActiveShards(F&& f) {
    IterateShards([&f](auto& sd, auto i) {
      if (sd.local_mask & ACTIVE) f(sd, i);
    });
  }

  //::dfly::BlockingCounter run_barrier_{0}; // 会导致极高的P99.9 延迟
  BlockingCounter run_barrier_{0};

  absl::InlinedVector<PerShardData, 4> shard_data_;

  absl::InlinedVector<IndexSlice, 4> args_slices_;

  absl::InlinedVector<Slice, 4> key_slices_;

  absl::InlinedVector<LockFp, 4> kv_fp_;

  CmdArgList full_args_;

  std::optional<RunnableType> cb_ptr_;

  const CommandId* cid_ = nullptr;

  TxId txid_{0};

  const Namespace* namespace_{nullptr};
  DbIndex db_index_{0};
  uint64_t time_now_ms_{0};

  uint32_t unique_shard_cnt_{0};
  ShardId unique_shard_id_{kInvalidSid};

  uint8_t coordinator_state_ = 0;

  std::coroutine_handle<> handle_;
  std::atomic<uint16_t> blocking_count_ = 0;
  std::atomic<bool> need_resume = false;
  std::atomic<bool> resume_count_ = false;

  void InitBlockingController(std::coroutine_handle<> handle,
                              unsigned blocking_count) {
    handle_ = handle;
    DCHECK_EQ(blocking_count_, 0u);
    blocking_count_ = blocking_count;
  }
  void ResumeIfNeed() {
    if (!need_resume.load(std::memory_order_acquire)) {
      return;
    }
    bool expected = false;
    if (resume_count_.compare_exchange_strong(expected, true,
                                              std::memory_order_acq_rel)) {
      handle_.resume();
    }
  }

 private:
  struct TLTmpSpace {
    std::vector<PerShardCache>& GetShardIndex(unsigned size);

   private:
    std::vector<PerShardCache> shard_cache;
  };
  static thread_local TLTmpSpace tmp_space;
};

OpResult<KeyIndex> DetermineKeys(const CommandId* cid, CmdArgList args);

inline std::string CmdArgListToString(CmdArgList full_args) {
  std::string out;
  for (size_t i = 0; i < full_args.size(); ++i) {
    if (i > 0) out += ' ';
    out += '"';
    out += full_args[i];
    out += '"';
  }
  return out;
}

}  // namespace dfly

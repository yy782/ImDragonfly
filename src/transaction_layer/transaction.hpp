// ============================================================================
// transaction.hpp —— 多分片事务调度核心（独立设计）
//
// ImDragonfly 对 VVL 论文（Kun Ren et al., "Lightweight Locking for
// Main Memory Database Systems", VLDB Journal 2015）的独立落地面。
//
// 本文件为全新设计：不携带 DragonflyDB 原版代码的移植痕迹，接口不兼容
// 原版调用点。行为语义锚定论文：
//   - 计数器锁 (CX, CS)：由 IntentLock 提供，Acquire 递增计数器并判定授予
//     （授予 = 无竞争），Release 对称递减；
//   - 全局事务序：单调递增 txid，每分片 TxQueue 按序插入，所有分片看到
//     一致的串行化顺序；
//   - 许可闸门：调度成功后各分片"放行"（is_armed），放行后才允许执行；
//   - 队首引理：每分片只需检查队首即可推进调度；
//   - 乐观内联：锁无竞争的分片可跳过排队直接执行回调（多分片仅幂等命令）；
//   - SCA：队首被外部卡住时，用写集/读集位数组扫描出无冲突事务提前执行。
// ============================================================================
#pragma once

#include <glog/logging.h>

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "command_layer/cmn_types.hpp"
#include "command_layer/command_id.hpp"
#include "detail/common_types.hpp"
#include "detail/intent_lock.hpp"
#include "detail/tx_base.hpp"
#include "detail/tx_queue.hpp"
#include "sharding/op_status.hpp"
#include "sharding/synchronization.hpp"
#include "util/cppcoro/async_task.hpp"
#include "util/cppcoro/task.hpp"
#include "util/function.hpp"
#include "util/intrusive_ptr.hpp"

namespace dfly {

class EngineShard;

using TxId = uint64_t;

class Transaction final
    : public util::intrusive_ref_counter<Transaction, util::thread_safe_counter> {
 public:
  // ---- 事务在单个分片上的调度状态位（flags，仅所属分片线程访问） ----
  // 放行位独立为 is_armed：协调器线程写、分片线程以 acquire 读并以 exchange
  // 清除（放行闸门）；其余状态位由所属分片线程独占访问。
  enum ShardFlag : uint16_t {
    kShardInvolved = 1 << 0,  // 本分片参与本事务（有 key 或全局事务）
    // 计数器锁下拿锁成功即无竞争（granted ⇔ keys_free）：本分片无更早序冲突者，
    // 获锁即可无视队首阻塞，乱序执行。
    kUncontended   = 1 << 1,  // 本分片锁无竞争（Acquire 成功即已持锁）：可无视队首乱序执行
    kRanInline     = 1 << 2,  // 回调已在本分片乐观内联执行完毕
  };

  // 单分片调度申请的结果（三态：完成 / 入队 / 重试，非单纯锁结果）
  enum class ScheduleResult : uint8_t {
    kGranted,  // 本分片回合已完成（乐观内联：已拿锁并执行回调）
    kQueued,   // 已入队，等待队首引理 / SCA 放行
    kRejected, // 本次申请失败（序冲突 / 队列高水位），需回滚重试
  };

  // 分片线程回调：在持有锁的上下文执行命令逻辑
  using Callback = util::FunctionRef<void(Transaction&, EngineShard&)>;

  Transaction();
  explicit Transaction(const CommandId* cid);
  ~Transaction();

  // 按命令参数初始化：经 cid->Keys(args) 提取 key 并映射到分片
  OpStatus Init(DbIndex db, cmn::CmdArgList args);

  // ---- 元信息 ----
  TxId txid() const { return txid_; }
  bool IsGlobal() const;  // 覆盖全部分片即为全局事务
  bool IsReadOnly() const { return (cid_->opt_mask() & CO::READONLY) != 0; }
  IntentLock::Mode LockMode() const {
    return IsReadOnly() ? IntentLock::SHARED : IntentLock::EXCLUSIVE;
  }
  size_t ShardCount() const { return active_shard_count_; }

  ShardId SoleShard() const {
    return active_shard_count_ == 1 ? involved_.single.sid : kInvalidSid;
  }
  std::string_view Name() const { return cid_ ? cid_->name() : "null-command"; }

  // ---- 执行入口（协调器协程） ----
  // 单跳执行：调度 -> 分发 -> 等待完成 -> 恢复调用方协程
  cppcoro::AsyncTask Run(Callback cb, std::coroutine_handle<> resume);

  // ---- 分片线程协议（EngineShard 驱动） ----
  // 单分片调度申请：Acquire 锁计数、授予判定，乐观内联或入队等待
  ScheduleResult ScheduleOnShard(EngineShard& shard, bool allow_optimistic);
  void ExecuteOnShard(EngineShard& shard);
  bool RollbackOnShard(EngineShard& shard);
  // 无条件放行（队首引理）：清除 is_armed，返回是否曾放行
  bool AllowOn(ShardId sid);
  // 条件放行（乱序执行）：仅当 is_armed 且含 need_flags 时清除并返回 true
  bool AllowOnIf(ShardId sid, uint16_t need_flags);
  bool IsAllowedOn(ShardId sid) const;

  // ---- 执行上下文（回调内使用） ----
  DbContext GetDbContext() const;
  DbIndex DbIndex() const { return db_; }
  uint64_t TimeMs() const { return start_ms_; }
  cmn::CmdArgList Args() const { return args_; }
  size_t GetKeyNum() const {
    size_t n = 0;
    for (size_t i = 0; i < active_shard_count_; ++i)
      n += ShardDataAt(i).fps.size();
    return n;
  }
  KeyLockArgs LockArgsOn(ShardId sid) const;

  // ---- 分片键视图（回调内遍历，按参数下标延迟取键） ----
  // 回调用法：for (const auto& [key, key_id] : tx->GetSlice(es->shard_id()))，
  // key_id 即该键在命令参数中的下标（值的取位由命令自身按约定解析）。
  class Slice {
   public:
    Slice(std::span<const IndexSlice> slices, unsigned step, cmn::CmdArgList args)
        : slices_(slices), step_(step), args_(args) {}

    struct Iterator {
      const IndexSlice* cur_ = nullptr;
      const IndexSlice* end_ = nullptr;
      unsigned idx_ = 0;
      unsigned step_ = 1;
      cmn::CmdArgList args_;
      std::pair<std::string_view, unsigned> val_;

      const std::pair<std::string_view, unsigned>& operator*() const {
        return val_;
      }

      Iterator& operator++() {
        idx_ += step_;
        if (idx_ >= cur_->second) {  // 越过当前段终点（开区间）
          ++cur_;
          if (cur_ < end_) {
            idx_ = cur_->first;
          } else {
            return *this;  // 越过最后一段：idx_ 与 end() 一致，作哨兵
          }
        }
        val_ = {args_[idx_], idx_};
        return *this;
      }

      bool operator!=(const Iterator& o) const { return idx_ != o.idx_; }
    };

    Iterator begin() const {
      if (slices_.empty()) return end();
      Iterator it{&slices_.front(), &slices_.back() + 1, slices_.front().first,
                  step_, args_, {}};
      it.val_ = {args_[it.idx_], it.idx_};
      return it;
    }

    Iterator end() const {
      if (slices_.empty()) return {nullptr, nullptr, 0, step_, args_, {}};
      return {&slices_.back() + 1, &slices_.back() + 1, slices_.back().second,
              step_, args_, {}};
    }

    std::span<const IndexSlice> slices_;  // 本分片的下标段（[pos, pos+step)）
    unsigned step_ = 1;                    // 命令级键步长（对所有键恒定）
    cmn::CmdArgList args_;
  };

  Slice GetSlice(ShardId sid) const;

 private:
  // 事务在单个分片上的调度状态（64B 对齐，避免与相邻分片状态伪共享）
  struct alignas(64) PerShardData {
    PerShardData() = default;
    // 原子成员不可拷贝/移动；move 仅转移向量，原子成员走默认构造（扩容后
    // 元素被重建并立即赋值，不依赖被移动元素的残留值）。
    PerShardData(PerShardData&& o) noexcept
        : slices(std::move(o.slices)), fps(std::move(o.fps)) {}

    uint16_t flags = 0;                      // 状态位：仅所属分片线程读写
    std::atomic_bool is_armed = false;        // 放行闸门：协调器置位 / 分片读清
    std::vector<IndexSlice> slices;          // 本分片的键参数下标段（相邻键合并）
    std::vector<LockFp> fps;                 // 与本分片键一一对应的锁指纹
    TxQueue::Iterator queue_pos = TxQueue::kEnd; // 队列位置；kEnd = 未入队
  };

  void InitBase(DbIndex db, cmn::CmdArgList args);
  void BuildKeyMap();
  void MarkAllShards();
  bool CanRunInlined() const;
  size_t IndexInvolved(ShardId sid) const;  // 分片 id → 状态数组下标
  bool IsInvolved(ShardId sid) const;       // 分片是否参与本事务
  ShardId InvolvedAt(size_t idx) const;     // 状态数组下标 → 分片 id
  PerShardData& ShardDataAt(size_t idx);    // 状态数组下标 → 分片状态
  const PerShardData& ShardDataAt(size_t idx) const;
  std::vector<ShardId>& ActivateMany();     // 激活多分片分支（返回 sids）
  cppcoro::task<> Schedule();          // 多分片调度（可重试）
  void Distribute();                   // 放行涉及分片并投递队列驱动
  void InvokeCallback(EngineShard& shard);
  void ReleaseLocks(EngineShard& shard, PerShardData& sd);
  void SetStartTime();               // 时间点随初始化/重调度刷新（复用事务每次 Init 重新计时）

  const CommandId* cid_ = nullptr;
  DbIndex db_ = 0;
  cmn::CmdArgList args_;
  uint64_t start_ms_ = 0;

  // 参与分片数据：单分片（标量）与多分片（向量）互斥，共占一块存储。
  // 同一时刻仅一个分支活跃，以 active_shard_count_ 判定：==1 读 single，
  // >1 读 many。many 分支的 vector 由 Init / ~Transaction 显式回收。
  union InvolvedData {
    struct {
      ShardId sid = kInvalidSid;  // 单分片：唯一参与分片
      PerShardData sd;            // 单分片：该分片状态
    } single;
    struct {
      std::vector<ShardId> sids;      // 多分片：参与分片 id（升序）
      std::vector<PerShardData> sds;  // 多分片：与 sids 同序的状态
    } many;

    InvolvedData() : single{} {}
    ~InvolvedData() {}  // many 的 vector 由外层显式回收
    InvolvedData(const InvolvedData&) = delete;
    InvolvedData& operator=(const InvolvedData&) = delete;
    InvolvedData(InvolvedData&&) = delete;
    InvolvedData& operator=(InvolvedData&&) = delete;
  } involved_;
  uint32_t active_shard_count_ = 0;

  TxId txid_ = 0;
  bool global_ = false;
  

  // 命令协程上下文：Run 启动时记录发起方与协调器分片，收尾时据此恢复协程。
  // "可恢复"的判定（pending 归零）与恢复动作分离：回调执行完只标记
  // （FinishCallback），由两个观察者（Run / ExecuteOnShard）在函数末尾
  // 检查并执行恢复（ResumeIfNeed），保证恢复发生在调度结束之后。
  struct CoroutineCtx {
    CoroutineCtx() = default;  // 供成员默认构造（初始值见字段初始化器）
    // Run 启动时初始化：记录回调、恢复句柄与协调器分片，重置执行进度
    // （need_resume / has_resume 走默认 false）
    CoroutineCtx(Callback callback, std::coroutine_handle<> handle,
                 ShardId owner, uint16_t count)
        : cb(std::move(callback)), resume(handle), owner_(owner) {
      pending.store(count, std::memory_order_relaxed);
    }

    std::optional<Callback> cb;     // 每分片执行的命令回调（单分片执行后置空）
    std::coroutine_handle<> resume; // 发起方协程句柄
    std::atomic<uint16_t> pending{0};  // 剩余未执行分片数；归零 = 全部完成
    std::atomic<bool> need_resume{false};
    std::atomic<bool> has_resume{false};
    ShardId owner_ = kInvalidSid;   // 协调器所在分片（须在该分片线程恢复）
    // Run 与 ExecuteOnShard 是两个观察者：他们结束的时候 Transaction
    // 可能已经调度完了（pending==0），此时恢复命令协程是安全的，事务
    // 可以被析构。所以他们在函数的最后面检查可不可以恢复协程。
    void ResumeIfNeed();
    // 在完成回调后 pending--：归零即"命令执行完成"（置 need_resume）。
    // 此刻事务可能还没调度完成（协调器 Run 未退出），故只标记不恢复。
    void FinishCallback();
  } coro_ctx_;

  BlockingCounter barrier_{0};                 // 调度 / 回滚回合的集合点
};

}  // namespace dfly

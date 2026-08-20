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
//   - 许可闸门：调度成功后各分片"放行"（kAllowed），放行后才允许执行；
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

class DbSlice;
class EngineShard;
class Namespace;

using TxId = uint64_t;

// ---- 事务在单个分片上的调度标志（原子位集） ----
// kAllowed 由协调器线程写、分片线程读/清除；其余位仅所属分片线程访问。
enum ShardFlag : uint16_t {
  kShardInvolved = 1 << 0,  // 本分片参与本事务
  kKeyHeld       = 1 << 1,  // 本分片参与锁申请且计数未释放（论文的"持锁/占位"）
  kAllowed       = 1 << 2,  // 已放行（armed）：允许执行
  kRanInline     = 1 << 3,  // 回调已在本分片乐观内联执行完毕
  kUncontended   = 1 << 4,  // 本分片锁无竞争：可无视队首阻塞提前执行
};

// 事务在单个分片上的视图
struct ShardState {
  TxQueue::Iterator queue_pos = TxQueue::kEnd;  // 队列位置；kEnd=未入队
  std::atomic<uint16_t> flags{0};
  uint32_t key_begin = 0;  // keys_ / fps_ 的段起点
  uint32_t key_count = 0;  // 段长
};

// 事务整体阶段（协调器视角，调试/断言用途）
enum class TxPhase : uint8_t {
  kReady,       // 初始化完成，可投入调度
  kScheduling,  // 申请锁 / 入队中
  kWaiting,     // 调度成功，等待分片放行
  kRunning,     // 分片执行中
  kFinished,    // 全部完成，可复用
};

// 单分片调度申请的结果
enum class LockResult : uint8_t {
  kGranted,  // 锁已获取且本分片已完成（乐观内联路径）
  kQueued,   // 已入队，等待队首引理 / SCA 放行
  kRejected, // 本次申请失败（序冲突 / 队列高水位），需回滚重试
};

class Transaction final
    : public util::intrusive_ref_counter<Transaction, util::thread_safe_counter> {
 public:
  // 分片线程回调：在持有锁的上下文执行命令逻辑
  using Callback = util::FunctionRef<void(Transaction&, EngineShard&)>;

  Transaction();
  explicit Transaction(const CommandId* cid);
  ~Transaction();

  // 按命令参数初始化：提取 key 并映射到分片
  OpStatus Init(const Namespace* ns, DbIndex db, cmn::CmdArgList args);
  // 用预计算的 KeyIndex 初始化（管线优化，跳过 key 提取）
  OpStatus Init(const Namespace* ns, DbIndex db, cmn::CmdArgList args,
                const KeyIndex& key_index, ShardId precomputed_sid);

  // ---- 元信息 ----
  TxId txid() const { return txid_; }
  bool IsGlobal() const { return global_; }
  bool IsReadOnly() const { return (cid_->opt_mask() & CO::READONLY) != 0; }
  IntentLock::Mode LockMode() const {
    return IsReadOnly() ? IntentLock::SHARED : IntentLock::EXCLUSIVE;
  }
  size_t ShardCount() const { return active_shard_count_; }
  ShardId SoleShard() const { return sole_shard_; }
  bool IsDone() const { return phase_ == TxPhase::kFinished; }
  std::string_view Name() const { return cid_ ? cid_->name() : "null-command"; }

  // ---- 执行入口（协调器协程） ----
  // 单跳执行：调度 -> 分发 -> 等待完成 -> 恢复调用方协程
  cppcoro::AsyncTask Run(Callback cb, std::coroutine_handle<> resume);

  // ---- 分片线程协议（EngineShard 驱动） ----
  LockResult ApplyForLockOn(EngineShard& shard, bool allow_optimistic);
  bool ExecuteOnShard(EngineShard& shard);
  bool RollbackOnShard(EngineShard& shard);
  // 无条件放行（队首引理）：清除 kAllowed，返回是否曾放行
  bool AllowOn(ShardId sid);
  // 条件放行（乱序 / SCA）：仅当 kAllowed 且含 need_flags 时清除并返回 true
  bool AllowOnIf(ShardId sid, uint16_t need_flags, uint16_t* got_flags);
  bool IsAllowedOn(ShardId sid) const;

  // ---- 执行上下文（回调内使用） ----
  DbSlice& SliceOn(ShardId sid) const;
  const Namespace* Ns() const { return ns_; }
  DbIndex DbIndex() const { return db_; }
  uint64_t TimeMs() const { return start_ms_; }
  cmn::CmdArgList Args() const { return args_; }
  std::span<const std::string_view> KeysOn(ShardId sid) const;
  size_t KeyCount() const { return keys_.size(); }
  KeyLockArgs LockArgsOn(ShardId sid) const;

 private:
  void InitBase(const Namespace* ns, DbIndex db, cmn::CmdArgList args);
  void BuildKeyMap(const KeyIndex& key_index, ShardId precomputed_sid);
  void MarkAllShards();
  bool CanRunInlined() const;
  size_t SidToId(ShardId sid) const;
  cppcoro::task<> Schedule();          // 多分片调度（可重试）
  void Distribute();                   // 放行涉及分片并投递队列驱动
  bool InvokeCallback(EngineShard& shard);
  void FinishShardExecution();         // 递减完成计数，归零后尝试恢复协调器
  void ReleaseLocks(EngineShard& shard, ShardState& sd);
  void ResumeIfReady();
  void SetPhase(TxPhase p) { phase_ = p; }

  const CommandId* cid_ = nullptr;
  const Namespace* ns_ = nullptr;
  DbIndex db_ = 0;
  cmn::CmdArgList args_;
  uint64_t start_ms_ = 0;

  std::vector<std::string_view> keys_;  // 按分片分段的 key 表
  std::vector<LockFp> fps_;             // 与 keys_ 一一对应的锁指纹
  std::vector<ShardState> shards_;      // 按分片 id 索引（单分片时压缩为 1 项）
  uint32_t active_shard_count_ = 0;
  ShardId sole_shard_ = kInvalidSid;

  TxId txid_ = 0;
  bool global_ = false;
  bool concluding_ = false;       // 单跳：分片执行完即可恢复协调器
  bool idempotent_ = false;       // CO::IDEMPOTENT：多分片允许乐观内联
  TxPhase phase_ = TxPhase::kReady;

  std::optional<Callback> cb_;
  std::coroutine_handle<> resume_handle_;
  ShardId owner_shard_ = kInvalidSid;
  std::atomic<uint16_t> pending_{0};           // 剩余未执行分片数
  std::atomic<bool> resume_requested_{false};  // 全部执行完毕，可恢复
  std::atomic<bool> complete_{false};          // 协调器 Run 已退出（co_return 前置位）
  BlockingCounter barrier_{0};                 // 调度 / 回滚回合的集合点
};

}  // namespace dfly

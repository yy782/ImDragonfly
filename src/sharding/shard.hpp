#pragma once

#include <mimalloc.h>

#include <array>
#include <cstdint>
#include <utility>
#include <vector>

#include "detail/intent_lock.hpp"
#include "detail/task_queue.hpp"
#include "detail/tx_queue.hpp"
#include "io/uring_proactor.hpp"
#include "raft/raft_log_entry.hpp"
#include "sharding/shard_storage.hpp"
#include "util/intrusive_ptr.hpp"
#include "util/mi_memory_resource.hpp"

namespace dfly {

class Transaction;

class Shard {
 public:
  friend class ShardPool;

  static void InitThreadLocal(base::UringProactor* pb);
  static void DestroyThreadLocal();
  static Shard* tlocal() { return shard_; }
  bool IsMyThread() const { return this == shard_; }

  ShardId shard_id() const { return shard_id_; }
  base::UringProactor* proactor() const { return proactor_; }
  std::pmr::memory_resource* memory_resource() { return &mi_resource_; }
  dfly::TaskQueue* GetQueue() { return &proactor_->GetTaskQueue(); }
  void DriveQueue(Transaction* tx);

  TxQueue& Queue() { return txq_; }
  const TxQueue& Queue() const { return txq_; }
  IntentLock& ShardLock() { return shard_lock_; }
  TxId CommittedTxId() const { return committed_txid_; }

  // 把一批"已就绪的 txid 区间"并入本分片的 ready 集合，然后重新驱动队列。
  // main 广播调用（**必须在分片线程上执行**，由 main 通过 shard_pool->Post
  // 投递过来）。is_read=true 走读确认集合，false 走 raft 提交集合。
  //
  // 采用"区间集合"而非 per-tx 标志：main 只产出"哪些 txid 可以走了"这一
  // 事实（纯数据），事务的执行权完全留在分片。集合只增不删（区间按 txid
  // 单调递增 append），判定时遍历查找——txid 有洞也无所谓，区间天然表达。
  void AddReadyRanges(bool is_read, std::vector<std::pair<TxId, TxId>> ranges);

#ifdef UNIT_TESTS
  void set_committed_txid(TxId v) { committed_txid_ = v; }
#endif

  ShardStorage& GetShardStorage() { return storage_; }
  const ShardStorage& GetShardStorage() const { return storage_; }

  // 队列高水位阈值（VVL 论文 §2.1）：多分片事务未拿全锁且队列达到此值时
  // 放弃入队，把 CPU 让给队首推进 / SCA 消化队列。
  static constexpr size_t kQueueHighWater = 8;

  // 把一条待提交的事务日志攒进本分片的缓冲。只在 shard 线程调用。
  // 只读事务不入日志（raft.md §9），由本函数内部判断后直接返回。
  // 返回 false = 本节点非 leader，客户端写进不了 raft 日志（永远等不到
  // 提交），调用方应把 OpStatus::RAFT_SCHED_FAIL 交给回调而非入队等待。
  bool PushLogIfNeed(Transaction* tx);

  // 只读事务入队时发起一次线性一致确认：把 txid 投给 main（leader 租约
  // 直接广播就绪 / follower 走 ReadIndex RPC）。只在 shard 线程调用，
  // 每事务只调用一次（入队点），之后 IsRaftReady 只做纯查询不放行副作用。
  void PushReadIndexIfNeed(Transaction* tx);

  // 攒到这个条数就提前刷给 main，不等定时器。
  static constexpr size_t kLogHighWater = 8;

  // 定时刷日志给 main 的周期。注意已知代价（raft.md §10）：squasher 内
  // 同一分片的命令是串行 co_await 的，单连接下攒不满 kLogHighWater，
  // 每条写命令最坏要等一个周期 —— 所以这个值直接决定单连接写延迟上限。
  // 降到 2ms 的代价是每分片每 2ms 一个定时器 CQE（空载时的固定开销），
  // 换来写延迟从 1s 降到 ~2ms。真要彻底消掉这个延迟就改成 push 即投递。
  static constexpr uint64_t kLogFlushIntervalMs = 2;

  // 启动 raft 日志刷新定时器。必须在 thread_local shard_ 已赋值、
  // 且 shard_pool / RedisServer 已就绪之后调用 —— 不能放在构造函数里
  // （构造期间 shard_ 还是 nullptr，见 raft.md §12.7）。
  void StartRaftLogTimer();

  // 把当前攒的日志立刻交给 main（在 shard 线程上取走，避免数据竞争）。
  void FlushLogToMain();

 private:
  Shard(base::UringProactor* pb, mi_heap_t* heap);

  // 轻量 SCA（选择性冲突分析，论文 §2.6）：仅队列堆积时激活，用写集/读集
  // 位数组扫描出已就绪且无冲突的事务提前执行。
  void MaybeDriveUnblocked();

  // raft 提交门闩：事务的日志必须先被 raft 提交，才允许 ExecuteOnShard。
  // 与 is_armed 是两道独立的门 —— is_armed 回答"调度器放行了吗"，
  // 这个回答"raft 提交了吗"。见 raft.md §5。
  // 非 const：只读事务在尚未确认时会把确认请求投递给 main（有副作用）。
  bool IsRaftReady(Transaction* tx);

  // 事务的写（raft 已提交）/ 读（ReadIndex 已确认）是否就绪。
  bool WriteTxReady(TxId txid) const {
    return InRanges(write_ready_ranges_, txid);
  }
  bool ReadTxReady(TxId txid) const {
    return InRanges(read_ready_ranges_, txid);
  }

  // ready 集合里是否含 txid。区间只增不删，直接遍历（区间数远小于事务数）。
  static bool InRanges(const std::vector<std::pair<TxId, TxId>>& ranges,
                       TxId txid);

  cppcoro::AsyncTask RaftLogTimerLoop();

  base::UringProactor* proactor_;
  ShardId shard_id_;
  MiMemoryResource mi_resource_;
  static thread_local Shard* shard_;

  ShardStorage storage_;
  // dragonflydb官方选择namespaces管理分片存储，为的是多租户，我们不需要多租户，shard管理即可
  TxQueue txq_;
  IntentLock shard_lock_;
  TxId committed_txid_ = 0;

  // SCA 位数组：2^16 位 = 8KB，L1 缓存友好
  static constexpr size_t kScgBits = 1 << 16;
  static constexpr size_t kScgWords = kScgBits / 64;
  static constexpr size_t kScgMaxScan = 32;  // 单次 SCA 最多扫描的事务数
  std::array<uint64_t, kScgWords> scg_dx_{};  // 已扫描事务的写集
  std::array<uint64_t, kScgWords> scg_ds_{};  // 已扫描事务的读集

  // 本分片攒着还没交给 main 的日志条目。**shard 线程独占** ——
  // 取走时必须在 shard 线程上 move 出来再投给 main，
  // 不能让 main 线程直接碰它（raft.md §12.5）。
  std::vector<RaftLogEntry> log_;
  bool raft_timer_started_ = false;

  // 已就绪的 txid 区间集合（闭区间 [lo, hi]），只增不删。写：raft 已提交；
  // 读：follower ReadIndex 已确认。**只被本分片线程读写**，main 通过
  // AddReadyRanges 投递过来（Post 到本分片线程后执行，故无需加锁）。
  std::vector<std::pair<TxId, TxId>> write_ready_ranges_;
  std::vector<std::pair<TxId, TxId>> read_ready_ranges_;
};

}  // namespace dfly

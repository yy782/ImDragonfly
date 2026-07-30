# 多分片事务：VLL 轻量级事务模型

> **参考论文**：[VLL: A Lock Manager Redesign for Main Memory Database Systems](https://www.cs.umd.edu/~abadi/papers/vldbj-vll.pdf) — VLDB Journal, 2015
> **作者**：Kun Ren, Alexander Thomson, Daniel J. Abadi (University of Maryland)

---

## 1. 架构概述：Thread-per-Shard

ImDragonfly 采用 **Thread-per-Shard** 架构。每个 CPU 核心运行一个 EventLoop 线程，管理一个分片（EngineShard）。Key 通过哈希映射到固定分片：

```
┌───────────────────────────────────────────────────────────────┐
│                        EngineShardSet                          │
│                                                               │
│  Thread 0         Thread 1         Thread 2         Thread 3  │
│  ┌─────────┐      ┌─────────┐      ┌─────────┐      ┌────────┐│
│  │ Shard 0 │      │ Shard 1 │      │ Shard 2 │      │Shard 3 ││
│  │ DbSlice │      │ DbSlice │      │ DbSlice │      │DbSlice ││
│  │ TxQueue │      │ TxQueue │      │ TxQueue │      │TxQueue ││
│  │EventLoop│      │EventLoop│      │EventLoop│      │EventLp ││
│  └─────────┘      └─────────┘      └─────────┘      └────────┘│
└───────────────────────────────────────────────────────────────┘
         ↑                ↑                ↑                ↑
         │      ──────────────────────────────────────
         │      │        Key → Shard 映射              │
         │      │   ShardId = Hash(key) % shard_count   │
         │      ──────────────────────────────────────
```

客户端连接通过 RedisSession 处理。每个命令到达后，被调度到涉及的分片上执行。单 Key 命令只在一个分片执行；多 Key 命令需要跨分片协调。

---

## 2. VLL 核心思想

VLL（Very Lightweight Locking）的洞察是：在内存数据库中，**锁管理不需要锁表、锁请求节点、等待队列、死锁检测图**这些东西。所有锁状态可以退化为每个 Key 上的**两个原子计数器**。

### 2.1 IntentLock：计数器实现

```cpp
// detail/intent_lock.hpp
class IntentLock {
 public:
  enum Mode { SHARED = 0, EXCLUSIVE = 1 };

  bool Acquire(Mode m) {
    ++cnt_[m];
    if (cnt_[1 ^ int(m)]) return false;           // 有相反类型锁存在
    return m == SHARED || cnt_[EXCLUSIVE] == 1;   // 排他锁要求只有自己
  }

  bool Check(Mode m) const {
    unsigned s = cnt_[EXCLUSIVE];
    if (s) return false;                          // 有排他锁 → 一律失败
    return (m == SHARED) ? true : cnt_[SHARED] == 0;
  }

  bool IsContended() const {
    return (cnt_[EXCLUSIVE] > 1) ||               // 多个排他竞争者
           (cnt_[EXCLUSIVE] == 1 && cnt_[SHARED] > 0); // 读写冲突
  }

 private:
  unsigned cnt_[2] = {0, 0};   // [共享锁计数, 排他锁计数]
};
```

**两个计数器**：

| 字段 | 含义 |
|------|------|
| `cnt_[SHARED]` | 正在 / 想要持有共享锁的事务数量 |
| `cnt_[EXCLUSIVE]` | 正在 / 想要持有排他锁的事务数量 |

**锁获取规则**：

```
Acquire(SHARED):    cnt_[EXCLUSIVE] == 0           → 成功
Acquire(EXCLUSIVE): cnt_[EXCLUSIVE] == 1 && cnt_[SHARED] == 0  → 成功
```

**锁冲突判定**（`IsContended`）：

```
有多个排他请求者            → 争用
有排他 + 有共享同时存在     → 争用
```

每个 Key 在 DbSlice 中关联一个 `IntentLock`。事务获取锁时不做阻塞等锁——尝试获取，拿到则执行，拿不到则通过队列调度。

---

## 3. 事务生命周期

### 3.1 命令入口

以 `DEL key1 key2 key3` 为例，命令层收到请求后：

```
用户发送: DEL key1 key2 key3
  │
  ▼
RedisSession::OnMessage()
  → 解析 RESP 协议，得到 commands
  → 找到 CommandId("DEL")
  → 创建 Transaction(tx)
  → tx->InitByArgs(namespace, db_index, args)
  → 进入协程: CmdDel(cmd_cntx, args)
```

### 3.2 InitByArgs：Key 到分片的映射

```cpp
// transaction.cpp
OpStatus Transaction::InitByArgs(const Namespace* ns, DbIndex index,
                                 CmdArgList args) {
  OpResult<KeyIndex> key_index = DetermineKeys(cid_, args);
  // key_index = {start=1, end=4, step=1}
  //   即 args[1], args[2], args[3] 都是 Key

  InitByKeys(*key_index);
}
```

`InitByKeys` 根据 Key 数量决定路径：

**路径 A：单 Key（`key_index.NumArgs() == 1`）**

```cpp
  unique_shard_cnt_ = 1;
  unique_shard_id_ = Shard(key, shard_set->size());
  shard_data_.resize(1);
  // 直接填好一个 PerShardData + 一个 Slice
```

只有一个分片参与，后续调度极其简单。

**路径 B：多 Key（`key_index.NumArgs() > 1`）**

```cpp
  shard_data_.resize(shard_set->size());  // 给每个分片预留 PerShardData

  BuildShardIndex(key_index, &shard_index);
  // 遍历所有 Key，按 ShardId 分组：
  //   Shard 0: {"key1", "key3"}
  //   Shard 2: {"key2"}
  //
  // 每个分片生成 slice: 连续的参数区间 [{1,2}, {3,4}]

  InitShardData(shard_index, key_index.NumArgs());
  // 对活跃分片设置 local_mask |= ACTIVE
  // 构建 args_slices_（参数区间）和 kv_fp_（锁指纹）
  // 统计 unique_shard_cnt_
```

如果多 Key 恰好落在同一分片，最终 `unique_shard_cnt_ == 1`，优化回单分片路径。

### 3.3 PerShardData：每个分片的独立状态

```cpp
// transaction.hpp
struct PerShardData {
  uint16_t local_mask;       // ACTIVE | KEYLOCK_ACQUIRED | OUT_OF_ORDER | ...
  int16_t slice_start;       // 在 args_slices_ 中的起始偏移
  int16_t slice_count;       // 参数区间数量
  uint32_t fp_start;         // 在 kv_fp_ 中的起始偏移
  uint32_t fp_count;         // 锁指纹数量
  TxQueue::Iterator pq_pos;  // 在 TxQueue 中的位置
  atomic_bool is_armed;      // 是否已就绪等待执行
};
```

**local_mask 含义**：

| 掩码 | 含义 |
|------|------|
| `ACTIVE` | 此分片参与当前事务 |
| `KEYLOCK_ACQUIRED` | Key 锁已获取 |
| `OUT_OF_ORDER` | 锁已拿到，可以立即执行（不排队） |
| `OPTIMISTIC_EXECUTION` | 乐观执行：锁拿到且无冲突，直接运行 |

---

## 4. SingleHopAsync：单跳事务调度

`SingleHopAsync` 是整个事务调度的入口。命令层的协程通过 `co_await SingleHopT(cb)` 挂起，由它驱动事务在分片上执行。

### 4.1 协程桥接

```cpp
// cmd_support.hpp
template <typename RT>
struct SingleHopWaiterT {
  bool await_ready() const noexcept { return false; }  // 总是挂起

  void await_suspend(std::coroutine_handle<Coro> coro) noexcept {
    cmd_cntx_->tx()->SingleHopAsync(*this, coro);  // 调用事务调度
  }

  RT&& await_resume() noexcept { return std::move(result_); }

  void operator()(Transaction* tx, EngineShard* es) const {
    result_ = callback_(tx, es);  // 在各分片回调中执行用户逻辑
  }
};
```

`SingleHopWaiterT` 既是 awaiter（协程挂起点），又是 callback（分片执行时的回调）。`await_suspend` 触发事务调度，`operator()` 在分片上执行具体命令逻辑。

### 4.2 执行流程

```cpp
// transaction.cpp
cppcoro::AsyncTask Transaction::SingleHopAsync(RunnableType cb,
                                               std::coroutine_handle<> handle) {
  coordinator_state_ |= COORD_CONCLUDING;
  cb_ptr_ = cb;

  if (unique_shard_cnt_ == 1) {
    // ── 单分片路径 ──
    shard_data_.front().is_armed.store(true);
    run_barrier_->Add(1);
    // 当前线程是否就是目标分片？
    if (CanRunInlined()) {
      ScheduleInShard(EngineShard::tlocal(), true);
    } else {
      shard_set->Add(unique_shard_id_, [this] { /* ... */ });
    }
    co_await run_barrier_->Wait();

  } else {
    // ── 多分片路径 ──
    co_await ScheduleInternal();   // 在所有分片上调度
    DispatchHop();                 // 触发执行
  }

  ResumeIfNeed("SingleHopAsync");  // 恢复协程
  co_return;
}
```

**两阶段模式**：

```
阶段一 ScheduleInternal:
  意图 → 锁 → 入队 → 记录位置

阶段二 DispatchHop:
  设置 is_armed → 各分片 PollExecution → 出队 → 执行 → FinishHop
```

---

## 5. ScheduleInternal：多分片调度

```cpp
cppcoro::task<> Transaction::ScheduleInternal() {
  bool optimistic_exec =
      (coordinator_state_ & COORD_CONCLUDING) &&
      (unique_shard_cnt_ == 1 || (cid_->opt_mask() & CO::IDEMPOTENT));

  while (true) {
    run_barrier_->Start(unique_shard_cnt_);
    // ...
    ScheduleContext schedule_ctx{this, optimistic_exec};

    auto cb = [&schedule_ctx] {
      if (!schedule_ctx.trans->ScheduleInShard(
              EngineShard::tlocal(),
              schedule_ctx.optimistic_execution)) {
        schedule_ctx.fail_cnt.fetch_add(1);  // 失败计数
      }
      schedule_ctx.trans->FinishHop();        // barrier 计数减一
    };

    // 为每个活跃分片投递 ScheduleInShard
    IterateActiveShards(
        [cb](..., ShardId i) { shard_set->Add(i, cb); });

    co_await run_barrier_->Wait();  // 等待所有分片调度完成

    if (schedule_ctx.fail_cnt.load() == 0) {
      break;  // 全部成功
    }

    // 有失败：取消已入队的，触发 PollExecution
    // 然后重试整个 while(true) 循环
  }
  coordinator_state_ |= COORD_SCHED;
}
```

**乐观执行的条件**：

- `COORD_CONCLUDING`：事务正在收尾阶段（即将执行）
- **且**（单分片 或 命令标记为 `IDEMPOTENT`）

如果满足，尝试在 ScheduleInShard 中直接执行回调，跳过排队等待。

---

## 6. ScheduleInShard：单分片锁获取

这是每个分片上的核心调度函数：

```cpp
bool Transaction::ScheduleInShard(EngineShard* shard, bool execute_optimistic,
                                  std::string context) {
  auto& sd = shard_data_[SidToId(shard->shard_id())];
  sd.local_mask &= ~(OUT_OF_ORDER | OPTIMISTIC_EXECUTION);

  // [1] 尝试获取 Key 锁
  lock_args = GetLockArgs(shard->shard_id());
  bool lock_granted = GetDbSlice(shard->shard_id()).Acquire(LockMode(), lock_args);
  sd.local_mask |= KEYLOCK_ACQUIRED;

  if (lock_granted) {
    sd.local_mask |= OUT_OF_ORDER;  // 标记：拿到了锁，可以直接执行

    // [2] 乐观路径：锁拿到了 → 直接执行回调
    if (lock_granted && execute_optimistic) {
      sd.local_mask |= OPTIMISTIC_EXECUTION;
      RunCallback(shard, context);       // ★ 执行用户命令逻辑
      if (coordinator_state_ & COORD_CONCLUDING) {
        release_locks();                 // 释放锁
        return true;
      }
    }
  }

  // [3] 需要排队的情况：
  //     - 锁没拿到 → 必须排队等
  //     - 锁拿到了但非乐观路径 → 排队获取 txid 和顺序保证

  // 检查：当前 txid 是否还小于队尾事务的 txid？
  // 如果是且锁也没拿到 → 直接失败返回（被更新的请求插队了）
  if (!txq->Empty() && txid_ < txq->Back()->txid() && !lock_granted) {
    release_locks();
    return false;  // 通知 ScheduleInternal 要重试
  }

  // 入队
  sd.pq_pos = txq->Push(this);
  return true;
}
```

**关键设计**：

- `OUT_OF_ORDER` 标记：拿到锁的事务可以被 `PollExecution` 越过队列直接执行，因为没有锁冲突
- 入队时检查 `txid_ < Back()->txid()`：防止旧事务排在新事务前面造成优先级反转
- 返回 `false` 触发 `ScheduleInternal` 的 while(true) 重试

---

## 7. TxQueue：无死锁调度

TxQueue 是每个分片独立维护的双向链表，保证同一分片上事务的执行顺序：

```cpp
// detail/tx_queue.hpp
class TxQueue {
  Iterator Push(Transaction* t);     // 追加到队尾
  Transaction* Front();              // 查看队头
  void Pop(Iterator& it);            // 从链表删除
  size_t Size() const;
  bool Empty() const;

 private:
  struct Node {
    Transaction* trans;
    Iterator next;  // 后向指针
    Iterator prev;  // 前向指针
  };
  vector<Node> vec_;          // 节点池
  uint32_t head_;             // 队头指针
  uint32_t tail_;             // 队尾指针
  uint32_t free_head_;        // 空闲链表的头，复用已删除节点
};
```

`free_head_` 是一个 **空闲节点链表**，`Pop` 时释放的节点并不归还内存，而是加入 `free_head_` 链表供后续 `Push` 复用。

### 7.1 无死锁保证

VLL 的核心定理不需要显式死锁检测：

```
队列状态:
  [T1] → [T2] → [T3] → [T4] → ...
   头

断言：队列头事务 T1 一定可以完成。

证明：
  假设 T1 需要访问 Key K
  - T1 入队时，已经在 K 上原子递增了对应的计数器（EXCLUSIVE 或 SHARED）
  - 后续入队的 T2、T3、T4 访问 K 时：
      - 如果是排他锁：cnt_[EXCLUSIVE] > 1  → 获取失败
      - 如果是共享锁：cnt_[EXCLUSIVE] > 0  → 获取失败
  - 排在 T1 后面的事务无法抢走 T1 的锁
  - PollExecution 按队列顺序触发，T1 最先获得执行机会
  - T1 执行完毕 → 释放锁 → 通知 PollExecution
  - T2 成为新队头，递归成立
```

**推论**：只要所有事务都经过 `ScheduleInShard → push → PollExecution → pop` 这条路径，系统就无死锁。

---

## 8. PollExecution：队列驱动执行

每个分片的 EventLoop 在空闲时、事务完成后、或收到通知时调用 `PollExecution` 驱动队列：

```cpp
// engine_shard.cpp
void EngineShard::PollExecution(Transaction* trans) {
  // [1] 处理特殊事务（DISARM / OUT_OF_ORDER）
  auto [trans_mask, disarmed] = trans ? trans->DisarmInShardWhen(sid, flags)
                                      : {0, false};

  // [2] 遍历队列
  while (!txq_.Empty()) {
    head = txq_.Front();

    // 被 disarm 的特定事务，或队头事务可以执行
    bool should_run = (head == trans && disarmed) || head->DisarmInShard(sid);
    if (!should_run) break;

    committed_txid_ = head->txid();
    head->RunInShard(this);  // ★ 执行
  }

  // [3] OUT_OF_ORDER 事务：队列空时，也可以执行
  if (trans && disarmed) {
    run(trans);
  }
}
```

**DisarmInShard 机制**：

- `ScheduleInternal` 阶段，事务在各分片上入队
- `DispatchHop` 阶段，调用 `sd.is_armed.store(true)` 武装
- `DisarmInShard` 检查 `is_armed == true`，若为真则 disarm（重置为 false）并返回 mask，表示"就绪可以执行"
- 如果 `should_run == false`，说明队头事务还没就绪，停止遍历

---

## 9. RunInShard：事务在分片上的执行

```cpp
bool Transaction::RunInShard(EngineShard* shard, std::string context) {
  auto& sd = shard_data_[shard->shard_id()];

  IntentLock::Mode mode = LockMode();
  RunCallback(shard, context);  // ★ 执行用户逻辑

  // 从队列中移除
  if (sd.pq_pos != TxQueue::kEnd) {
    shard->txq()->Pop(sd.pq_pos);
    sd.pq_pos = TxQueue::kEnd;
  }

  // 释放 Key 锁
  if (coordinator_state_ & COORD_CONCLUDING) {
    if (sd.local_mask & KEYLOCK_ACQUIRED) {
      GetDbSlice(shard->shard_id()).Release(mode, lock_args);
      sd.local_mask &= ~KEYLOCK_ACQUIRED;
    }
    sd.local_mask &= ~OUT_OF_ORDER;
  }

  FinishHop();     // barrier 减一
  ResumeIfNeed();  // 如果所有分片都完成了 → 恢复协程
  return is_concluding;
}
```

`FinishHop` 调用 `run_barrier_->Dec()`，递减 `RunBlockingBarrier` 的计数器。当所有活跃分片都执行完 `RunInShard`，barrier 归零，协程恢复。

---

## 10. DispatchHop：触发所有分片执行

```cpp
void Transaction::DispatchHop() {
  // 统计哪些分片需要 PollExecution 唤醒
  bitset<1024> poll_flags(0);

  IterateActiveShards([&](auto& sd, auto i) {
    if ((sd.local_mask & OPTIMISTIC_EXECUTION) == 0) {
      // 乐观路径已在 ScheduleInShard 中直接执行了，不需要再唤醒
      poll_flags.set(i, true);
    }
    sd.local_mask &= ~OPTIMISTIC_EXECUTION;
  });

  // 武装 is_armed
  IterateActiveShards([&](auto& sd, auto i) {
    if (poll_flags.test(i))
      sd.is_armed.store(true, memory_order_relaxed);
  });

  // 向目标分片投递 PollExecution
  IterateShards([&](..., auto i) {
    if (poll_flags.test(i))
      shard_set->Add(i, [this] {
        EngineShard::tlocal()->PollExecution(this);
      });
  });
}
```

---

## 11. 锁的获取与释放

锁不存储在事务对象上，而是存储在 DbSlice 管理的每个 Key 上：

```cpp
// 获取锁
bool DbSlice::Acquire(IntentLock::Mode mode, const KeyLockArgs& args) {
  for (auto fp : args.fps) {
    auto& lock = GetLock(fp);  // 找到 Key 对应的 IntentLock
    lock.Acquire(mode);
    if (lock.IsContended()) {
      // 有争用 → 前面获取的锁不释放（保留意图标记）
      return false;
    }
  }
  return true;  // 全部获取成功且无争用
}

// 释放锁
void DbSlice::Release(IntentLock::Mode mode, const KeyLockArgs& args) {
  for (auto fp : args.fps) {
    auto& lock = GetLock(fp);
    lock.Release(mode);
  }
}
```

**注意**：`Acquire` 中 `IsContended` 检查失败时，之前获取的锁**不释放**。这保留了事务对 Key 的"意图标记"，后续通过 TxQueue 调度时，其他事务可以检测到并乖乖排队。

---

## 12. 完整执行链路示例

以 `DEL key1 key2 key3`（3 个 Key 分布在 2 个分片）为例：

```
1. 客户端 → "DEL key1 key2 key3"
2. RedisSession → 解析 → GenericFamily::Delex → CmdDel
3. CmdDel:
     tx.InitByArgs(ns, db, args)
       → BuildShardIndex: Shard 0 = {key1, key3}, Shard 2 = {key2}
       → unique_shard_cnt_ = 2

     auto cb = [&](Transaction* t, EngineShard* es) {
       DbSlice& dbslice = t->GetDbSlice(es->shard_id());
       auto res = OpDel(t, dbslice);  // 删除该分片上的 Key
       result.fetch_add(res.value_or(0));
     };

     co_await SingleHopT(cb);
       │
       ▼
4. SingleHopAsync(cb, handle):
     coordinator_state_ |= COORD_CONCLUDING
     cb_ptr_ = cb
     co_await ScheduleInternal():
       │
       ├─ run_barrier_->Start(2)
       ├─ 向 Shard 0 投递: ScheduleInShard(shard0, optimistic)
       │     → Acquire(EXCLUSIVE, {fp(key1), fp(key3)})
       │     → 无争用 → OUT_OF_ORDER | OPTIMISTIC_EXECUTION
       │     → 乐观执行: 直接 RunCallback → OpDel 删除 key1, key3
       │     → FinishHop: barrier (2→1)
       │
       ├─ 向 Shard 2 投递: ScheduleInShard(shard2, optimistic)
       │     → Acquire(EXCLUSIVE, {fp(key2)})
       │     → 无争用 → OUT_OF_ORDER | OPTIMISTIC_EXECUTION
       │     → 乐观执行: 直接 RunCallback → OpDel 删除 key2
       │     → FinishHop: barrier (1→0)
       │
       └─ co_await barrier.Wait() → 恢复
     
     DispatchHop():
       → 所有分片都是 OPTIMISTIC_EXECUTION，无需唤醒
       → 直接跳过

5. ResumeIfNeed → co_await 返回 → coro 继续
6. CmdDel 继续: rb->BuildInteger(del_cnt)
7. 响应 → "3"
```

**如果第三 Key 与另一个事务冲突**（key2 已被其他事务持有锁）：

```
ScheduleInShard(shard2):
  → Acquire(EXCLUSIVE, {fp(key2)})
  → cnt_[EXCLUSIVE] > 1  → IsContended == true  → 获取失败
  → lock_granted = false
  → sd.local_mask |= KEYLOCK_ACQUIRED（留意图标记）
  → 无 OUT_OF_ORDER、无 OPTIMISTIC_EXECUTION
  → txq->Push(this): 正常入队

DispatchHop:
  → Shard 0 乐观执行过，Shard 2 需要唤醒
  → poll_flags[2] = true
  → sd.is_armed.store(true)
  → shard_set->Add(2, PollExecution(this))

PollExecution(shard2):
  → txq_.Front() → 发现 is_armed
  → DisarmInShard → 返回 mask
  → should_run = true
  → RunInShard → RunCallback → OpDel
  → Pop → Release → FinishHop
```

---

## 13. 扩展：选择性争用分析（SCA）

> SCA（Selective Contention Analysis）是 VLL 论文在第 6 节提出的优化方案，ImDragonfly 当前版本尚未实现。

### 13.1 问题：热点 Key 下的性能退化

基础 VLL 的 TxQueue 工作得很好：事务入队 → 按序出队 → 无死锁。但它有一个前提——事务之间是真正冲突的。现实负载中大量存在一种情况：**一群事务争抢同一个热点 Key**。

假设 `counter` 被 100 个事务并发执行 `INCR counter`：

```
100 个事务全部尝试 Acquire(EXCLUSIVE, fp(counter))
  → cnt_[EXCLUSIVE] > 1  → 全部失败
  → 全部入队
  → 排队逐个执行
```

100 个事务在队列中**串行化**，吞吐量跌到单线程水平。然而每个事务可能还访问其他完全不冲突的 Key，那些 Key 本可以并行。问题根源不是锁本身，而是**调度策略对冷热 Key 一视同仁**。

### 13.2 SCA 的核心思路

SCA 的洞察很简单：**绝大多数字 Key 是冷的，只有极少数是热的**。既然如此，为什么不区分对待？

- **冷 Key**：继续用乐观 VLL，没冲突直接执行，有冲突再说
- **热 Key**：提前施加序列化约束，避免大量并发事务撞上同一个锁后被迫排队

关键区别在于**时机**——基础 VLL 是"撞上了再排队"（被动），SCA 对热 Key 是"提前排队"（主动），避免大量事务同时失败后集中入队造成的突刺延迟。

### 13.3 争用检测

每个 Key 维护一个争用计数器，每次 `IsContended()` 上报冲突就递增。同时有一个时间窗口衰减机制，计数器随时间衰减，热度消退后自动降回冷状态：

```
冲突次数低  → Key 是 COLD  → 乐观路径
冲突次数中  → Key 是 WARM  → 预警，继续观察
冲突次数高  → Key 是 HOT   → 激活序列化调度
```

衰减的意义是：不能因为一个 Key 曾经热过就永久特殊对待它，热度是动态的。

### 13.4 热 Key 的调度语义

当 Key 被判为 HOT 后，所有访问它的新事务不再走乐观路径，而是强制进入序列化调度的轨道。只访问冷 Key 的事务完全不受影响，继续走乐观路径。

同一事务可能跨冷热：访问一个热 Key + 若干冷 Key。这种情况下论文提出先锁定热 Key 侧的锁，再锁定冷 Key 侧的锁——热 Key 锁的获取顺序确定了事务在热点上的执行序，冷 Key 锁延后拿不影响无死锁性质。

### 13.5 核心价值

SCA 不是替代 VLL，而是在 VLL 之上叠加的一层**自适应调度策略**。它的价值在于：

1. **无事时零开销**：冷负载下行为和基础 VLL 完全一致
2. **热点时定向管控**：只在真正热的数据上激活序列化，避免全量退化
3. **热度可衰减**：热点消退后自动恢复乐观路径，无需人工介入


## 参考文献

1. Ren, K., Thomson, A., & Abadi, D. J. (2015). **VLL: A Lock Manager Redesign for Main Memory Database Systems**. *The VLDB Journal*, 24(5), 681-705. [PDF](https://www.cs.umd.edu/~abadi/papers/vldbj-vll.pdf)

2. Lu, B., Hao, X., Wang, T., & Lo, E. (2020). **Dash: Scalable Hashing on Persistent Memory**. *Proceedings of the VLDB Endowment*, 13(8), 1147-1161. [arXiv:2003.07302](https://arxiv.org/abs/2003.07302)

3. DragonflyDB. [GitHub Repository](https://github.com/dragonflydb/dragonfly). BSD License.

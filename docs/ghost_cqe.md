# Pipeline 压测崩溃：幽灵 CQE 根因分析与修复记录

> **问题**：集成测试正常，但 pipeline 压力测试下 ImDragonfly 必然崩溃。
> **结论**：io_uring 的 CQE 延迟到达 + slot 循环复用导致"幽灵 CQE"错误唤醒协程。
> **修复**：generation（代际号）方案，`user_data = (generation << slot_idx_bits_) | idx`，代际不匹配即丢弃。

---

## 1. 背景与现象

- **现象**：`memtier_benchmark -P 50`（8 线程 × 100 连接）压测下服务崩溃；`-P 10` 或低负载下正常。
- **复现规律**：4 分片必然崩溃，24 分片不崩（分片越多、每个 slot 复用越快？——实际是分片少时单分片负载高、slot 复用竞争更激烈）。
- **ASAN 干扰**：ASAN 报 `heap-use-after-free`，但对协程帧重用存在误报嫌疑，且会隐藏真实时序问题，最终改用 **Debug 模式 + 断言** 定位。

## 2. 根因分析

### 2.1 io_uring 的 CQE 会延迟到达

一个异步操作提交后，其完成事件（CQE）**可能延迟很久才被收割**，常见原因：

- 操作被取消（如连接断开导致 accept/recv 取消）；
- 内核/网络栈处理延迟；
- 提交后 SQE 尚未真正下发（SQ 满时先缓存在用户态，`SubmitIfNeeded` 批量提交）。

### 2.2 slot 循环复用（ring buffer 语义）

`UringProactor` 用固定大小的 slot 数组（`queue_depth = 4096`）登记在途操作，通过取模循环复用：

```cpp
// src/net/uring_proactor.cpp
uint32_t UringProactor::AllocSlot() {
  uint32_t idx = (next_slot_++) & slot_mask_;   // 循环复用
  auto& slot = pending_slots_[idx];
  slot.coro = nullptr;
  slot.result = -ECANCELED;
  slot.flags = -1;
  ++slot.generation;                            // 每次复用递增代际
  return (slot.generation << slot_idx_bits_) | idx;
}
```

### 2.3 二者叠加 = 幽灵 CQE

```
操作 A 占用 slot 5，提交 SQE_A（尚未完成）
        │
        ▼  时刻 t1：slot 5 被循环复用，登记操作 B，提交 SQE_B
        │
        ▼  时刻 t2：A 的 CQE 此刻才到达
        │
        ▼  ProcessCqe 按 user_data 找到 slot 5
        │   → 误 resume 操作 B 的协程（或访问到 coro 已清空的 slot）→ 崩溃
```

修复前 `user_data = slot_idx`（直接当索引），**无法区分这个 CQE 属于 A 还是 B**。

### 2.4 诊断证据

修复前加入诊断日志后，出现大量：

```
[STRAY] user_data=2882 slot_idx=2882 res=890 slot.flags=4294967295
[STRAY] user_data=3408 slot_idx=3408 res=890 slot.flags=17
```

- `user_data=2882/3408` 均 `< 4096`，是**合法 slot 索引**（不是越界）；
- 但对应 slot 的 `coro == nullptr`、`flags == -1` → 是"已被复用/清空、旧 CQE 迟到"的典型特征。

## 3. 修复方案：Generation（代际号）

### 3.1 思路

给每个 slot 维护一个单调递增的 `generation`，每次 `AllocSlot` 复用时 `++generation`。`user_data` 编码为：

```
user_data = (generation << slot_idx_bits_) | slot_idx
```

CQE 到达时解码出 `slot_idx` 和 `gen`，**只有 `gen == slot.generation` 才是当前有效操作**，否则丢弃。

### 3.2 数据结构

```cpp
// src/net/uring_proactor.hpp
struct IoCompletionSlot {
  std::coroutine_handle<> coro;
  int32_t result = 0;
  uint32_t flags = 0;
  uint32_t generation = 0;   // 每次 AllocSlot 复用时递增
};
```

构造函数中计算 `slot_idx_bits_`（如 4096 → 12 位），保证代际与索引不重叠：

```cpp
// src/net/uring_proactor.cpp 构造函数
slot_idx_bits_ = 0;
while ((1u << slot_idx_bits_) < slots) ++slot_idx_bits_;
```

### 3.3 三个提交点统一编码

`AsyncAccept` / `AsyncRecvFixed` / `AsyncSend` 统一模式：

```cpp
uint32_t ud = AllocSlot();
uint32_t slot_idx = ud & slot_mask_;
// sqe->user_data = ud;
return AcceptAwaitable(this, slot_idx, ud >> slot_idx_bits_);  // awaitable 携带代际
```

### 3.4 CQE 入口校验

```cpp
// src/net/uring_proactor.cpp ProcessCqe
if (cqe->user_data == LIBURING_UDATA_TIMEOUT) {  // liburing 内部超时 SQE，无对应 slot
  return;
}
uint32_t ud = static_cast<uint32_t>(cqe->user_data);
uint32_t slot_idx = ud & slot_mask_;
uint32_t gen = ud >> slot_idx_bits_;
auto& slot = GetSlot(slot_idx);

if (slot.generation != gen) {   // 幽灵 CQE：slot 已被复用，旧操作的 CQE 迟到
  LOG(ERROR) << "[GHOST] ...";
  return;
}
if (!slot.coro) {               // coro 为空（理论上代际校验后不应出现）
  LOG(ERROR) << "[STRAY] ...";
  return;
}
ResumeSlot(slot_idx, cqe->res, slot.flags);
```

## 4. 验证结果

Debug 模式（保留 `assert`，禁用 ASAN）下：

| 压测 | 结果 |
|---|---|
| 4 分片 `-P 10`（8t×100c） | 229K ops，进程存活，0 崩溃，0 GHOST/STRAY |
| 4 分片 `-P 50`（8t×100c，30s） | 进程存活，0 崩溃 / 0 FATAL / 0 ERROR |
| `[GHOST]`（代际不匹配被正确拦截） | 286 次，如 `ud=55205 slot=1957 gen=13 cur_gen=17` |
| `[STRAY]`（coro 为空的误用） | **0** |

**结论**：幽灵 CQE 依然存在（io_uring 固有行为，无法消除），但已被代际校验**每次识别并丢弃**，不再产生错误唤醒。`[GHOST]` 日志是预期的拦截行为，不是错误。

## 5. 遗留事项

- 当前 `[GHOST]`/`[STRAY]` 使用 `LOG(ERROR)` 级别，压测时可能刷屏。拦截逻辑必须保留，但可将日志降为 `VLOG(2)`。
- 需评估代际溢出的理论风险：`generation` 为 `uint32_t`，`slot_idx_bits_` 占用 12 位，代际上限 `2^20`，在 `queue_depth=4096` 下需 slot 复用约 100 万次才会耗尽，且溢出后回绕仍能校验（`gen` 与 `slot.generation` 同步回绕），实际可忽略。

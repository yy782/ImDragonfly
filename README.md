# ImDragonfly

ImDragonfly 是一款模仿Dragonfly的高性能键值存储数据库，通过创新的架构设计和前沿技术，解决了传统 Redis 在大规模场景下的性能瓶颈和扩展性难题。

***

## 🔥 核心价值：解决的关键问题

### 1️⃣ 突破传统 Redis 的性能天花板

传统 Redis 受限于单线程模型，在多核服务器上无法充分利用硬件资源。ImDragonfly 采用**分片架构 + 协程调度**，实现真正的多核并行处理。

### 2️⃣ 解决高并发场景下的 IO 阻塞

ImDragonfly 基于 **io_uring + 协程** 实现高性能网络 IO，支持数万级并发连接。

### 3️⃣ 优化内存管理，降低运营成本

采用 **mimalloc** 高性能内存分配器，结合定制的内存资源池，提高内存利用率。

### 4️⃣ 实现高效的数据分片与查找

ImDragonfly 采用 **DashTable** 作为核心数据存储引擎，这是一款基于开放寻址的高性能哈希表实现。

**核心技术原理：**

- **开放寻址 + 线性探测**：摒弃传统链式哈希的指针开销，数据直接存储在连续内存槽位中。当发生哈希冲突时，通过线性探测（`NextBid`）查找相邻桶，实现 O(1) 平均复杂度的查找和插入操作。每个 Bucket 包含 12 个槽位，使用位图（`SlotBitmap`）高效管理槽位状态。

- **Fingerprint 指纹优化**：提取哈希值的低 8 位作为指纹（`kFingerBits = 8`），存储在 `finger_arr_` 数组中。查找时先通过 SIMD 指令（`_mm_cmpeq_epi8`）进行指纹比对，快速过滤不匹配的候选键，显著减少不必要的完整键比较。

- **Stash 溢出处理**：每个 Segment 包含 64 个主桶和 4 个 Stash 桶。当主桶及其邻居桶均已满时，数据会被写入 Stash 桶，并通过 `SetStashPtr` 建立反向引用，确保查找时能够追踪到溢出数据。

- **可扩展哈希分段**：采用类似 Extendible Hashing 的目录结构，支持动态扩展。通过 `Split` 操作将单个 Segment 分裂为两个，`IncreaseDepth` 扩展全局目录，实现按需扩容而无需重建整个哈希表。

### 5️⃣ 替换传统 2PL 锁为 VVL 意向锁

传统 2PL（Two-Phase Locking，两阶段锁）需要为每个锁维护等待队列和死锁检测图——随着 Key 数量增长，锁表本身成为严重的内存和性能瓶颈，而死锁检测更是在多 Key 事务中引入不可预测的延迟。

ImDragonfly 的 **VVL（Very Lightweight Locking）意向锁** 的核心洞察是：**所有锁状态可以退化为每个 Key 上的两个原子计数器**，不需要锁表、锁请求节点、等待队列、死锁检测图。

```
struct IntentLock {
  unsigned cnt_[2] = {0, 0};   // [SHARED 计数, EXCLUSIVE 计数]
};
```

**"意向"而非"锁"——关键差异：**

| 维度 | 2PL（真正加锁） | VVL（记录意图） |
| ---- | --- | --------- |
| 数据结构 | 锁表 + 等待队列 + 死锁检测图 | 仅 8 字节双计数器 |
| 加锁动作 | 真正阻塞线程，挂起等待 | `cnt_[m]++`，仅记录"我对这个 Key 有意向" |
| 冲突处理 | 阻塞 → 死锁检测 → 回滚 | `Acquire()` 非阻塞，失败直接返回 false |
| 锁获取失败 | 线程被挂起，死锁风险 | 之前已递增的计数器**不释放**，保留意图标记 |
| 调度方式 | 线程挂起/唤醒 | 排队进入 TxQueue，协程级调度 |
| 死锁 | 需要检测和回滚 | **不可能死锁**——事务从不阻塞等锁 |

**为什么不会死锁：**

所有事务都走同一条路径：`Acquire（非阻塞标记意图）→ 失败则 Push TxQueue → PollExecution 按队列顺序唤醒`。队头事务一定能完成——因为任何后来者只是在计数器上看到了冲突，拿不到锁返回 false 排队，永远无法阻塞队头。这个归纳保证消除了死锁的可能性。

**性能优势：**

- 非冲突场景（乐观路径）：事务拿到 `OUT_OF_ORDER` 标记，跳过队列直接执行，锁的开销仅是一次原子递增
- 冲突场景：TxQueue 的 FIFO 顺序 + 协程轻量切换，避免了线程上下文切换和死锁检测的计算开销
- 每个 Key 仅 8 字节，对比 2PL 的数百字节锁表，内存效率提升数十倍

***

## 🚀 技术亮点

| 技术领域      | 实现方案              | 核心优势        |
| --------- | ----------------- | ----------- |
| **网络 IO** | io_uring + C++20 协程   | 事件驱动，高并发处理 |
| **并发模型**  | C++ 20 Coroutines | 轻量级线程调度 |
| **内存管理**  | mimalloc + PMR    | 低碎片、高性能分配   |
| **协议兼容**  | Redis RESP     | 无缝对接现有生态    |

***

## 🏗️ 架构设计

### 分层架构

```
┌─────────────────────────────────────────────────────────────┐
│                      命令层 (Command Layer)                │
│         命令注册 / 参数解析 / 执行引擎 / 事务管理           │
├─────────────────────────────────────────────────────────────┤
│                      存储层 (Storage Layer)                  │
│         分片管理 / 内存表  / 数据同步                         │
├─────────────────────────────────────────────────────────────┤
│                      网络层 (Network Layer)                │
│         io_uring 事件驱动 / 协程调度                          │
└─────────────────────────────────────────────────────────────┘
```

### 核心组件

- **EngineShardSet**: 分片集合管理器，负责分片的创建，分片事务的运行
- **CommandRegistry**: 命令注册中心
- **RedisSession**: 客户端会话管理，处理连接生命周期
- **Transaction**: 事务引擎，处理分片事务


***

## 📊 性能对比

### DashTable 存储引擎基准

| 操作类型 | DashTable | std::unordered_map | 胜出 |
|---------|-----------|-------------------|------|
| **Insert** (插入) | 274.67 ms | 427.24 ms | DashTable |
| **Find** (查找) | 179.14 ms | 237.37 ms | DashTable |
| **Erase** (删除) | 307.60 ms | 357.29 ms | DashTable |
| **Memory Usage** (内存占用) | 22.8 MB | 41.6 MB | DashTable |

> - Insert 比 unordered_map 快 **36%**
> - 内存占用比 unordered_map 降低 **47%**

### 端到端吞吐基准 (io_uring, 4 分片)

| 指标 | 数值 |
|------|------|
| 总 Ops/sec | **392,043** |
| SET 吞吐 | 196,024 ops/s |
| GET 吞吐 | 196,018 ops/s |
| 平均延迟 | 1.02 ms |
| p50 延迟 | 0.90 ms |
| p99 延迟 | 1.79 ms |
| p99.9 延迟 | 5.73 ms |
| 峰值 CPU 利用率 | 400.0% (4 线程) |
| 测试配置 | 4 线程 × 100 连接, 30 秒 |
***

## 🚀 快速开始

### 方式一：Docker 一键启动（推荐）

```bash
# 克隆项目
git clone https://github.com/yy782/ImDragonfly.git
cd ImDragonfly

# 构建并启动
docker compose up -d

# 使用 redis-cli 连接
redis-cli -p 6379
```

> **提示**：Docker 构建采用多阶段优化，最终镜像仅包含运行时依赖，体积精简。如需自定义分片数量，修改 `docker-compose.yml` 中的 `command` 参数。

### 方式二：源码编译

#### 环境要求

- **操作系统**: Linux (内核 >= 5.18，支持 `IORING_SETUP_SINGLE_ISSUER`)
- **liburing**: >= 2.2
- **编译器**: Clang 17+ (或 GCC 11+)
- **构建工具**: CMake 3.20+

#### 安装依赖

```bash
# Ubuntu/Debian (推荐 Ubuntu 22.04+)
sudo apt-get install -y clang cmake make \
    liburing-dev libmimalloc-dev \
    libgoogle-glog-dev
```

> **注意**：项目默认启用 `IORING_SETUP_SINGLE_ISSUER`（需 Linux 5.18+）。如果内核版本较低，需在 `UringConfig` 中设置 `use_single_issuer = false` 和 `use_defer_taskrun = false`。

#### 构建安装

```bash
# 克隆项目
git clone https://github.com/yy782/ImDragonfly.git
cd ImDragonfly

# 构建
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

# 运行（参数为分片数量，默认 4）
./imdragonfly 4
```

### 测试连接

```bash
# 使用标准 Redis 客户端连接
redis-cli -p 6379

# 执行命令
127.0.0.1:6379> SET hello "ImDragonfly"
OK
```

***

## 📁 项目结构

```
ImDragonfly/
├── net/                    # 网络核心层
│   ├── base/              # 异步 IO 封装、Socket 抽象
│   ├── cppcoro/           # C++ 20 协程库
│   └── util/              # 工具函数、并发原语
├── src/                   # 业务逻辑层
│   ├── command_layer/     # 命令处理、参数解析
│   ├── network/           # Redis 协议实现
│   ├── redis/             # RESP 编解码
│   ├── sharding/          # 分片管理、DashTable
│   ├── transaction_layer/ # 事务引擎
│   └── detail/            # 内部实现细节
├── test/                  # 测试套件
├── CMakeLists.txt         # 构建配置
└── LICENSE                # BSL 1.1 许可证
```

***

## 🤝 贡献指南

欢迎贡献代码！请遵循以下流程：

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/your-feature`)
3. 提交代码 (`git commit -m 'Add some feature'`)
4. 推送到分支 (`git push origin feature/your-feature`)
5. 创建 Pull Request

***

## 📜 许可证

本项目采用 **Business Source License 1.1 (BSL 1.1)**，详见 [LICENSE](LICENSE)。

- Change Date：2030-11-01，届时将转为 Apache License 2.0
- 非生产环境（开发/测试/学习）使用不受限
- 部分文件衍生自 [DragonflyDB](https://github.com/dragonflydb/dragonfly)，版权归 DragonflyDB authors 所有

***

## 📞 联系方式

如有问题或建议，欢迎通过以下方式联系：

- GitHub Issues: [提交问题](https://github.com/yy782/ImDragonfly/issues)
- 邮件: <yy782@example.com>

***

> **ImDragonfly** — 让数据飞起来！ 🐉


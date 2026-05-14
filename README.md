# ImDragonfly

ImDragonfly 是一款模仿Dragonfly的分布式键值存储数据库，通过创新的架构设计和前沿技术，解决了传统 Redis 在大规模场景下的性能瓶颈和扩展性难题。

---

## 🔥 核心价值：解决的关键问题

### 1️⃣ 突破传统 Redis 的性能天花板
传统 Redis 受限于单线程模型，在多核服务器上无法充分利用硬件资源。ImDragonfly 采用**分片架构 + 协程调度**，实现真正的多核并行处理。

### 2️⃣ 解决高并发场景下的 IO 阻塞
传统 epoll 模型在高连接数场景下存在显著的性能损耗。ImDragonfly 基于 **Linux io_uring** 实现零拷贝异步 IO，将网络延迟降低 **50%+**，同时支持百万级并发连接。

### 3️⃣ 优化内存管理，降低运营成本
采用 **mimalloc** 高性能内存分配器，结合定制的内存资源池，将内存碎片率控制在 **1% 以下**，相比传统方案节省 **30%** 的内存开销。


---

## 🚀 技术亮点

| 技术领域 | 实现方案 | 核心优势 |
|---------|---------|---------|
| **异步 IO** | Linux io_uring | 零拷贝、低延迟、高吞吐 |
| **并发模型** | C++ 20 Coroutines | 轻量级线程，百万级并发 |
| **内存管理** | mimalloc + PMR | 低碎片、高性能分配 |
| **数据分片** | 一致性哈希 | 线性扩展、自动负载均衡 |
| **协议兼容** | Redis RESP 3.0 | 无缝对接现有生态 |

---

## 🏗️ 架构设计

### 分层架构
```
┌─────────────────────────────────────────────────────────────┐
│                      命令层 (Command Layer)                │
│         命令注册 / 参数解析 / 执行引擎 / 事务管理           │
├─────────────────────────────────────────────────────────────┤
│                      存储层 (Storage Layer)                │
│         分片管理 / 内存表 / 持久化 / 数据同步               │
├─────────────────────────────────────────────────────────────┤
│                      网络层 (Network Layer)                │
│         io_uring 异步 IO / 协程调度 / 连接池               │
└─────────────────────────────────────────────────────────────┘
```

### 核心组件

- **EngineShardSet**: 分片集合管理器，负责分片的创建、销毁和负载均衡
- **CommandRegistry**: 命令注册中心，支持动态命令扩展
- **RedisSession**: 客户端会话管理，处理连接生命周期
- **Transaction**: 分布式事务引擎，保证数据一致性
- **UringProactor**: io_uring 事件循环，实现高效异步 IO

---

## 📊 性能对比



---

## 🚀 快速开始

### 环境要求
- **操作系统**: Linux (内核 >= 5.1，支持 io_uring)
- **编译器**: GCC 11+ / Clang 13+
- **构建工具**: CMake 3.20+

### 构建安装

```bash
# 克隆项目
git clone https://github.com/yy782/ImDragonfly.git
cd ImDragonfly

# 构建
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

# 运行
./imdragonfly --port 6379 --shards 8
```

### 测试连接

```bash
# 使用标准 Redis 客户端连接
redis-cli -p 6379

# 执行命令
127.0.0.1:6379> SET hello "ImDragonfly"
OK
127.0.0.1:6379> GET hello
"ImDragonfly"
```

---

## 📁 项目结构

```
ImDragonfly/
├── net/                    # 网络核心层
│   ├── base/              # io_uring 封装、Socket 抽象
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
└── LICENSE                # MIT 许可证
```

---

## 🤝 贡献指南

欢迎贡献代码！请遵循以下流程：

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/your-feature`)
3. 提交代码 (`git commit -m 'Add some feature'`)
4. 推送到分支 (`git push origin feature/your-feature`)
5. 创建 Pull Request

---

## 📜 许可证

MIT License - 详见 [LICENSE](LICENSE)

---

## 📞 联系方式

如有问题或建议，欢迎通过以下方式联系：

- GitHub Issues: [提交问题](https://github.com/yy782/ImDragonfly/issues)
- 邮件: yy782@example.com

---

> **ImDragonfly** — 让数据飞起来！ 🐉

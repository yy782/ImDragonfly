# 致谢 (Acknowledgments)

本项目是一个为深入学习和研究内存数据库（In-Memory Database）内部原理而创建的实践项目。开发过程中参考了以下开源项目与学术论文，在此致谢。

## 设计参考

- **DragonflyDB**：本项目的核心架构、多线程模型（Thread-per-Core）以及 DashTable 哈希表等设计，参考了 [DragonflyDB](https://www.dragonflydb.io/) 的开源实现。
- **helio**：以下文件衍生自 [helio](https://github.com/romange/helio)，文件头部保留了原始版权声明（Copyright 2023, Roman Gershman. All rights reserved.）：
  - `src/util/synchronization.hpp`
  - `src/util/wait_queue.hpp`
  - `src/sharding/synchronization.hpp` / `src/sharding/synchronization.cpp`
  - `src/sharding/wait_queue.hpp` / `src/sharding/wait_queue.cpp`
- **Dash 论文**：DashTable 数据结构的设计参考了 *Dash: Scalable Hashing on Persistent Memory*。

## 许可证

- 本仓库原创代码采用 [MIT License](../LICENSE)。
- 衍生自 helio 的文件按 [Apache License 2.0](https://github.com/romange/helio/blob/main/LICENSE) 条款授权。
- `src/util/` 下 vendored 的第三方库（function2 按 Boost Software License 1.0、cppcoro 按 MIT License）适用各自原始许可证，详见各文件头部声明。

## 参考论文

- **Lu, B., Hao, X., Wang, T., & Lo, E. (2020).** *Dash: Scalable Hashing on Persistent Memory.* Proceedings of the VLDB Endowment, 13(8), 1147-1161. [arXiv:2003.07302](https://arxiv.org/abs/2003.07302)
- **Ren, K., Thomson, A., & Abadi, D. J. (2015).** *VLL: A Lock Manager Redesign for Main Memory Database Systems.* The VLDB Journal, 24(5), 681-705. [PDF](https://www.cs.umd.edu/~abadi/papers/vldbj-vll.pdf)

详细设计说明见 [dash_table.md](dash_table.md) 与 [multi_shard_transaction.md](multi_shard_transaction.md)。

# 致谢与设计声明 (Acknowledgments & Design Statement)

## 项目背景
本项目是一个为了深入学习和研究内存数据库（In-Memory Database）内部原理而创建的实践项目。

## 核心设计参考
本项目的核心架构、多线程模型（Thread-per-Core）以及数据结构设计，**极大程度地参考了 [DragonflyDB](https://www.dragonflydb.io/) 的设计思想**。在开发过程中，为透彻理解其高性能背后的设计哲学，我深入研读了 DragonflyDB 的源码，并在此基础上完成了本项目的独立实现。

## 对 DragonflyDB 的致谢
DragonflyDB 是一个极其出色的现代内存数据存储系统，其在**无共享架构**、**新型哈希表**（DashTable）以及**无 Fork 快照技术**等方面的创新，为业界提供了全新的思路。本项目的学习和探索，完全仰赖于 DragonflyDB 团队的开源精神和卓越的技术分享。

## 论文参考
本项目涉及的核心理念与数据结构设计，参考了以下学术论文：

### DashTable / 哈希表设计
- **Lu, B., Hao, X., Wang, T., & Lo, E. (2020).** *Dash: Scalable Hashing on Persistent Memory.* Proceedings of the VLDB Endowment, 13(8), 1147-1161. [arXiv:2003.07302](https://arxiv.org/abs/2003.07302)
- 详细说明见：[dash_table.md](dash_table.md)

### VLL / 多分片事务模型
- **Ren, K., Thomson, A., & Abadi, D. J. (2015).** *VLL: A Lock Manager Redesign for Main Memory Database Systems.* The VLDB Journal, 24(5), 681-705. [PDF](https://www.cs.umd.edu/~abadi/papers/vldbj-vll.pdf)
- 详细说明见：[multi_shard_transaction.md](multi_shard_transaction.md)

## 法律与版权声明
- **学习目的**：本项目严格遵循 **"学习与研究目的" (Educational and Research Purposes Only)** 原则。
- **许可证**：本项目原创代码采用 [MIT License](../LICENSE)，版权归属本项目作者。项目中部分源文件保留了 DragonflyDB 的原版权声明（Copyright DragonflyDB authors），这些文件中对 DragonflyDB 源码的改编部分遵循 DragonflyDB 的 [BSL 1.1](https://github.com/dragonflydb/dragonfly/blob/main/LICENSE.md) 许可证条款。
- **差异说明**：本项目在实现细节、周边生态或特定功能上可能与 DragonflyDB 存在差异，且**绝不代表 DragonflyDB 的官方实现**。

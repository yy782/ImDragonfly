# 致谢与设计声明 (Acknowledgments & Design Statement)

## 项目背景
本项目是一个为了深入学习和研究内存数据库（In-Memory Database）内部原理而创建的实践项目。

## 核心设计参考
本项目的核心架构、多线程模型（Thread-per-Core）以及数据结构设计，**极大程度地参考并致敬了 [DragonflyDB](https://www.dragonflydb.io/) 的优秀实现**。在开发过程中，为透彻理解其高性能背后的设计哲学，我深入研读了 DragonflyDB 的源码，并在本项目中复现了其核心逻辑。

## 对 DragonflyDB 的致谢
DragonflyDB 是一个极其出色的现代内存数据存储系统，其在**无共享架构**、**新型哈希表**（如 dash-table）以及**无 Fork 快照技术**等方面的创新，为业界提供了全新的思路。本项目的存在，完全仰赖于 DragonflyDB 团队的开源精神和卓越的技术分享。

## 法律与版权声明
- **学习目的**：本项目严格遵循 **“学习与研究目的” (Educational and Research Purposes Only)** 原则。
- **版权归属**：本项目中受 DragonflyDB 启发的代码逻辑，其原始设计思想和架构版权完全归属于 DragonflyDB 及其贡献者。本项目的任何商业用途均需谨慎评估法律风险。
- **差异说明**：本项目在实现细节、周边生态或特定功能上可能与 DragonflyDB 存在差异，且**绝不代表 DragonflyDB 的官方实现**。
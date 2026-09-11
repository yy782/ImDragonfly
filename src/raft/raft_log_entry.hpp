#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "detail/common_types.hpp"
#include "transaction_layer/transaction.hpp"
#include "util/intrusive_ptr.hpp"

namespace dfly {

struct RaftLogEntry {
  uint64_t index = 0;
  uint64_t term = 0;

  TxId txid = 0;

  // leader 定好的执行时刻（Transaction::TimeMs()）。必须进日志：
  // EXPIRE / SET PX / SETEX 若让 follower 用自己的时钟重放，会算出不同的
  // 过期时刻 → 副本状态分叉。见 raft.md §7。
  uint64_t start_ms = 0;

  std::string payload;  // RESP 编码的命令
};

// 把命令参数编码成一条 RESP 数组（*N\r\n$len\r\n arg \r\n ...）。
// 在 **shard 线程**调用：main 单线程要扛全部 raft，别给它加序列化开销。
std::string EncodeRespCommand(::dfly::CmdArgList args);

// 从 std::string 列表编码一条 RESP 数组（raft 内部生成成员变更条目时用，
// 那些参数不是来自客户端 io_uring 缓冲区的 Arg）。
std::string EncodeRespCommandFromStrings(const std::vector<std::string>& args);

// ---------------------------------------------------------------------------
// 磁盘记录格式，
// 单文件 append-only：
//
//   +-----+-------+------+----------+-------+------------------+
//   | len | index | term | start_ms | crc32 | payload (RESP)   |
//   | u32 | u64   | u64  | u64      | u32   | len 字节          |
//   +-----+-------+------+----------+-------+------------------+
//
// len 是 payload 字节数（不含头部）；crc32 覆盖头部各字段 + payload，
// 用于检测尾部撕裂（写盘中途断电）。txid 不落盘：它只是单次运行内的
// 事务定序键，重放时重新发号（见结构体内注释）。
//
// 头部 32 字节。格式演进后旧日志文件不兼容 —— 升级时删掉重建。
// 将来 RDB 快照可承载基线状态、并据此截断此前的 raft 日志，所以这里
// 暂不直接实现 raft 快照与日志截断。
// ---------------------------------------------------------------------------

constexpr size_t kRaftRecordHeaderSize = 32;
static_assert(sizeof(uint32_t) * 2 + sizeof(uint64_t) * 3 ==
                  kRaftRecordHeaderSize,
              "record header must stay 32 bytes on disk");

// 硬件 CRC32C（SSE4.2）。项目已用 -march=x86-64-v3，指令必然可用。
uint32_t RaftCrc32(uint64_t index, uint64_t term, uint64_t start_ms,
                   std::string_view payload);

// 任意字节流的 CRC32C（state 文件用 —— 它的字段布局与日志记录不同）。
uint32_t RaftCrc32Raw(std::string_view data);

// 把 entry 序列化成一条磁盘记录，追加到 out 尾部（不清空 out —— 便于
// 把整批条目拼成一次 AsyncWriteFile）。
void AppendRecord(const RaftLogEntry& e, std::string* out);

}  // namespace dfly
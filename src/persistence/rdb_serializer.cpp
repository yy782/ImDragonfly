#include "persistence/rdb_serializer.hpp"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>

#include "detail/common.hpp"
#include "detail/tx_base.hpp"
#include "redis/redis_aux.hpp"
#include "sharding/DashTable/compact_obj.hpp"
#include "sharding/db_slice.hpp"
#include "sharding/db_table.hpp"
#include "sharding/namespaces.hpp"
#include "util/Time.hpp"

namespace dfly {
namespace {

// ═══════════════════════════════════════════════════════════════════════
// RDB 文件格式（IMDRDB02）
// ═══════════════════════════════════════════════════════════════════════
//
// 整体布局：
//
//   ┌────────────┬──────────────────┬─────────────┬─────────────┬─────────┐
//   │  Magic 8B  │  Record × N      │  EofMarker  │            │
//   │ "IMDRDB02" │ (变长，见下)     │   0xFF 1B   │            │
//   └────────────┴──────────────────┴─────────────┴─────────────┴─────────┘
//        ↑                ↑                ↑
//   文件起始标识       数据记录          文件结束标识
//
// 单条 Record 布局（所有多字节整数均为小端序）：
//
//   ┌──────────────┬───────────┬──────┬──────────┬─────────┬────────┬───────────┐
//   │ RecordMarker │ ns_name  │ dbid │   key    │ expire  │  kind  │   value │
//   │    0x01 1B   │  len+str │ u32  │ len+str  │ u64 8B  │ 1B     │  变长 │
//   └──────────────┴───────────┴──────┴──────────┴─────────┴────────┴───────────┘
//
//   字段说明：
//   - RecordMarker (1B)：固定 0x01，标识一条记录开始；非 0x01 视为损坏
//   - ns_name (4B len + str)：记录所属命名空间，加载时按此路由到 Namespace
//   - dbid    (4B u32)     ：DB index，加载时据此路由到对应 DbSlice
//   - key     (4B len + str)：键名
//   - expire  (8B u64)     ：过期时间戳（秒），0 = 无 TTL 哨兵值
//                            加载端 AddOrUpdate(0) 会调 RemoveExpire 清标记
//   - kind    (1B)         ：值类型标识
//                            0=int64  1=string  2=list
//                            3=hash   4=set     5=zset
//   - value   (变长)       ：按 kind 不同格式见下方 value 子格式
//
// value 子格式（kind 决定）：
//
//   kind=0 (int)：  [i64 8B]
//   kind=1 (str)：  [u32 len][bytes]
//   kind=2 (list)： [u32 count][ [u32 len][bytes] ] × count
//   kind=3 (hash)： [u32 count][ [u32 klen][k][u32 vlen][v] ] × count
//   kind=4 (set)：  [u32 count][ [u32 len][bytes] ] × count
//   kind=5 (zset)： [u32 count][ [u32 mlen][member][u64 score_bits] ] × count
//                   score_bits = double 的位模式，避免精度损失
//
// 字符串统一用 [u32 len][bytes] 前缀长度格式。
// 所有 record 顺序写入，无偏移表、无索引；加载时顺序扫描至 EofMarker 即可。
// ═══════════════════════════════════════════════════════════════════════

constexpr std::string_view kMagic = "IMDRDB02";
constexpr uint8_t kRecordMarker = 0x01;
constexpr uint8_t kEofMarker = 0xFF;

// ---- 小端序列化工具 ----
void AppendU32(std::string* b, uint32_t v) {
  b->append(reinterpret_cast<const char*>(&v), sizeof(v));
}
void AppendU64(std::string* b, uint64_t v) {
  b->append(reinterpret_cast<const char*>(&v), sizeof(v));
}
void AppendI64(std::string* b, int64_t v) {
  b->append(reinterpret_cast<const char*>(&v), sizeof(v));
}
void AppendStr(std::string* b, std::string_view s) {
  AppendU32(b, static_cast<uint32_t>(s.size()));
  b->append(s.data(), s.size());
}
// double 以位模式保存，避免精度损失。
void AppendDoubleBits(std::string* b, double v) {
  uint64_t bits;
  std::memcpy(&bits, &v, sizeof(bits));
  AppendU64(b, bits);
}

bool ReadU32(const std::string& d, size_t* p, uint32_t* out) {
  if (*p + sizeof(uint32_t) > d.size()) return false;
  std::memcpy(out, d.data() + *p, sizeof(uint32_t));
  *p += sizeof(uint32_t);
  return true;
}
bool ReadU64(const std::string& d, size_t* p, uint64_t* out) {
  if (*p + sizeof(uint64_t) > d.size()) return false;
  std::memcpy(out, d.data() + *p, sizeof(uint64_t));
  *p += sizeof(uint64_t);
  return true;
}
bool ReadStr(const std::string& d, size_t* p, std::string_view* out) {
  uint32_t len;
  if (!ReadU32(d, p, &len)) return false;
  if (*p + len > d.size()) return false;
  *out = std::string_view(d.data() + *p, len);
  *p += len;
  return true;
}
bool ReadDoubleBits(const std::string& d, size_t* p, double* out) {
  uint64_t bits;
  if (!ReadU64(d, p, &bits)) return false;
  std::memcpy(out, &bits, sizeof(bits));
  return true;
}

// 序列化单个值到 buf。返回 false 表示未知类型（调用方记录错误）。
bool SerializeValue(const CompactValue& val, std::string* buf) {
  std::string scratch;
  if (val.IsInt()) {
    buf->push_back(0);
    AppendI64(buf, val.AsInt());
    return true;
  }
  if (val.IsStr() || val.IsTtlStr()) {
    buf->push_back(1);
    AppendStr(buf, val.GetSlice(&scratch));
    return true;
  }
  if (val.IsRobj()) {
    switch (val.ObjType()) {
      case OBJ_LIST: {
        buf->push_back(2);
        const auto& data = val.GetList()->Data();
        AppendU32(buf, static_cast<uint32_t>(data.size()));
        for (const auto& e : data) AppendStr(buf, e);
        return true;
      }
      case OBJ_HASH: {
        buf->push_back(3);
        const auto& data = val.GetHash()->Data();
        AppendU32(buf, static_cast<uint32_t>(data.size()));
        for (const auto& [k, v] : data) {
          AppendStr(buf, k);
          AppendStr(buf, v);
        }
        return true;
      }
      case OBJ_SET: {
        buf->push_back(4);
        const auto& data = val.GetSet()->Data();
        AppendU32(buf, static_cast<uint32_t>(data.size()));
        for (const auto& e : data) AppendStr(buf, e);
        return true;
      }
      case OBJ_ZSET: {
        buf->push_back(5);
        const auto data = val.GetZSet()->Range(0, -1);
        AppendU32(buf, static_cast<uint32_t>(data.size()));
        for (const auto& [member, score] : data) {
          AppendStr(buf, member);
          AppendDoubleBits(buf, score);
        }
        return true;
      }
      default:
        LOG(ERROR) << "RdbSerializer: unknown robj type " << val.ObjType();
        return false;
    }
  }
  LOG(ERROR) << "RdbSerializer: unknown value tag";
  return false;
}

// 反序列化单个值。返回 false 表示损坏。
bool DeserializeValue(const std::string& d, size_t* p, uint8_t kind,
                      CompactValue* out) {
  switch (kind) {
    case 0: {  // int
      uint64_t v;
      if (!ReadU64(d, p, &v)) return false;
      out->SetInt(static_cast<int64_t>(v));
      return true;
    }
    case 1: {  // string
      std::string_view sv;
      if (!ReadStr(d, p, &sv)) return false;
      out->SetString(sv);
      return true;
    }
    case 2: {  // list
      uint32_t n;
      if (!ReadU32(d, p, &n)) return false;
      CompactValue list = CompactValue::MakeList();
      for (uint32_t i = 0; i < n; ++i) {
        std::string_view sv;
        if (!ReadStr(d, p, &sv)) return false;
        list.GetList()->PushBack(std::string(sv));
      }
      *out = std::move(list);
      return true;
    }
    case 3: {  // hash
      uint32_t n;
      if (!ReadU32(d, p, &n)) return false;
      CompactValue hash = CompactValue::MakeHash();
      for (uint32_t i = 0; i < n; ++i) {
        std::string_view k, v;
        if (!ReadStr(d, p, &k)) return false;
        if (!ReadStr(d, p, &v)) return false;
        hash.GetHash()->Set(std::string(k), std::string(v));
      }
      *out = std::move(hash);
      return true;
    }
    case 4: {  // set
      uint32_t n;
      if (!ReadU32(d, p, &n)) return false;
      CompactValue set = CompactValue::MakeSet();
      for (uint32_t i = 0; i < n; ++i) {
        std::string_view sv;
        if (!ReadStr(d, p, &sv)) return false;
        set.GetSet()->Add(std::string(sv));
      }
      *out = std::move(set);
      return true;
    }
    case 5: {  // zset
      uint32_t n;
      if (!ReadU32(d, p, &n)) return false;
      CompactValue zset = CompactValue::MakeZSet();
      for (uint32_t i = 0; i < n; ++i) {
        std::string_view member;
        double score;
        if (!ReadStr(d, p, &member)) return false;
        if (!ReadDoubleBits(d, p, &score)) return false;
        zset.GetZSet()->Add(std::string(member), score);
      }
      *out = std::move(zset);
      return true;
    }
    default:
      LOG(ERROR) << "RdbSerializer: invalid value kind " << int(kind);
      return false;
  }
}

}  // namespace

bool RdbSerializer::SaveShard(ShardId sid, std::string_view dir) {
  const uint64_t now_ms = util::GetCurrentTimeMs();

  std::string tmp = std::string(dir) + "/" + FileName(sid) + ".tmp";
  FILE* f = std::fopen(tmp.c_str(), "wb");
  if (!f) {
    LOG(ERROR) << "RdbSerializer: cannot open " << tmp << " for writing";
    return false;
  }

  // 流式写入：固定 64KB 缓冲，满了就 flush，避免全量收集导致内存峰值翻倍。
  // 之前实现把整个 shard 的序列化数据收集到一个 std::string buf，
  // 大表（几百万 entry）会让 buf 涨到几百 MB。改用流式后内存恒定 64KB。
  constexpr size_t kFlushThreshold = 64 * 1024;
  std::string buf;
  buf.reserve(kFlushThreshold);
  buf.append(kMagic);

  // flush 辅助：把 buf 内容写到底层文件并清空（保留 capacity 避免 realloc）。
  auto FlushBuf = [&]() {
    if (buf.empty()) return;
    std::fwrite(buf.data(), 1, buf.size(), f);
    buf.clear();
  };

  // 遍历所有 namespace：每个命名空间下该 shard 的全部 db 都落盘，
  // 每条记录前置所属 ns 名，加载时据此路由回对应 DbSlice。
  // 过期项在遍历时直接删除
  namespaces->ForEach([&](std::string_view ns_name, Namespace& ns) {
    auto& db_slice = ns.GetDbSlice(sid);
    for (DbIndex dbid = 0; dbid < db_slice.DbCount(); ++dbid) {
      DbContext cntx(&ns, dbid, now_ms);
      db_slice.TraverseTableMutable(dbid, [&](PrimeIterator it) {
        const PrimeKey& key = it->first;
        const PrimeValue& val = it->second;
        if (key.HasExpire() &&
            int64_t(now_ms / 1000) >= int64_t(key.GetExpireTime())) {
          // 单遍历清理：直接删除当前项，DashTable::Traverse 为
          // cursor-based，删除当前 bucket 项后 cursor 仍能安全前进。
          db_slice.ExpireIfNeeded(cntx, it);
          return;
        }
        buf.push_back(kRecordMarker);
        AppendStr(&buf, ns_name);
        AppendU32(&buf, dbid);
        std::string scratch;
        AppendStr(&buf, key.GetSlice(&scratch));
        AppendU64(&buf, key.GetExpireTime());
        if (!SerializeValue(val, &buf)) {
          LOG(ERROR) << "RdbSerializer: failed to serialize a value, skipping";
        }
        // 满 64KB 就 flush，避免 buf 无限增长
        if (buf.size() >= kFlushThreshold) FlushBuf();
      });
    }
  });
  buf.push_back(kEofMarker);
  FlushBuf();  // 写剩余 + EOF marker

  // 显式 fsync 保证数据落盘，再 fclose。
  // 之前仅 fclose 不一定触发 fsync，崩溃后 rename 完成的文件可能丢失内容。
  std::fflush(f);
  int fd = fileno(f);
  if (fd >= 0) {
    fsync(fd);
  }
  if (std::fclose(f) != 0) {
    LOG(ERROR) << "RdbSerializer: fclose failed for " << tmp;
    std::remove(tmp.c_str());
    return false;
  }

  // rename 前后 fsync 父目录，保证 rename 元数据落盘。
  // 否则崩溃后文件可能存在但目录项未持久化，重启后文件丢失。
  int dir_fd = ::open(dir.data(), O_RDONLY | O_DIRECTORY);
  if (dir_fd >= 0) {
    fsync(dir_fd);
    close(dir_fd);
  }
  if (std::rename(tmp.c_str(),
                  (std::string(dir) + "/" + FileName(sid)).c_str()) != 0) {
    LOG(ERROR) << "RdbSerializer: rename " << tmp << " failed";
    std::remove(tmp.c_str());
    return false;
  }
  dir_fd = ::open(dir.data(), O_RDONLY | O_DIRECTORY);
  if (dir_fd >= 0) {
    fsync(dir_fd);
    close(dir_fd);
  }

  VLOG(3) << "RdbSerializer::SaveShard: shard " << sid << " done";
  return true;
}

bool RdbSerializer::LoadShard(ShardId sid, std::string_view dir) {
  std::string path = std::string(dir) + "/" + FileName(sid);
  // 用 Linux 原生 open/fstat/read/close 替代 std::ifstream：
  // 步骤更直白（fd -> 取大小 -> 循环读满 -> 关闭），不依赖 stream 隐式状态。
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    // 文件不存在视为正常（首次启动尚无 RDB），静默返回 true。
    return true;
  }

  struct stat st;
  if (::fstat(fd, &st) < 0) {
    LOG(ERROR) << "RdbSerializer: fstat failed for " << path;
    ::close(fd);
    return false;
  }
  const size_t file_size = static_cast<size_t>(st.st_size);

  std::string data;
  data.resize(file_size);
  // read 可能只返回部分数据（被信号打断 / 大文件），循环直到读满或 EOF。
  size_t total = 0;
  while (total < file_size) {
    ssize_t n = ::read(fd, data.data() + total, file_size - total);
    if (n < 0) {
      if (errno == EINTR) continue;  // 被信号打断，重试
      LOG(ERROR) << "RdbSerializer: read failed for " << path;
      ::close(fd);
      return false;
    }
    if (n == 0) break;  // EOF 提前到达（文件被截断）
    total += static_cast<size_t>(n);
  }
  ::close(fd);
  data.resize(total);  // 实际读到的字节数

  LOG(INFO) << "[load-shard] shard " << sid << " file=" << path
            << " size=" << data.size();

  if (data.size() < kMagic.size() ||
      std::string_view(data.data(), kMagic.size()) != kMagic) {
    LOG(ERROR) << "RdbSerializer: bad magic in " << path
               << ", refusing to load";
    return false;
  }

  uint64_t now_ms = util::GetCurrentTimeMs();

  size_t pos = kMagic.size();
  uint64_t records = 0;
  while (pos < data.size()) {
    uint8_t marker = data[pos++];
    if (marker == kEofMarker) break;
    if (marker != kRecordMarker) {
      LOG(ERROR) << "RdbSerializer: bad record marker in " << path;
      return false;
    }

    // 记录所属命名空间：未知 ns 自动创建（GetOrInsert），保证加载后
    // 所有 namespace 的数据都在位。
    std::string_view ns_name;
    if (!ReadStr(data, &pos, &ns_name)) return false;
    Namespace& ns = namespaces->GetOrInsert(ns_name);
    auto& db_slice = ns.GetDbSlice(sid);

    uint32_t dbid;
    if (!ReadU32(data, &pos, &dbid)) return false;
    std::string_view key;
    if (!ReadStr(data, &pos, &key)) return false;
    uint64_t expire_ms;
    if (!ReadU64(data, &pos, &expire_ms)) return false;
    if (pos >= data.size()) return false;
    uint8_t kind = data[pos++];

    CompactValue val;
    if (!DeserializeValue(data, &pos, kind, &val)) return false;

    if (!db_slice.IsDbValid(static_cast<DbIndex>(dbid))) {
      db_slice.EnsureDb(static_cast<DbIndex>(dbid));
    }
    DbContext cntx(&ns, static_cast<DbIndex>(dbid), now_ms);
    db_slice.AddOrUpdate(cntx, key, std::move(val), expire_ms);
    VLOG(3) << "[load-shard] shard " << sid << " ns='" << ns_name
            << "' dbid=" << dbid << " key='" << key << "'";
  }
  return true;
}

}  // namespace dfly

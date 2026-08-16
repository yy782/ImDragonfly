#include "persistence/rdb_serializer.hpp"

#include <glog/logging.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
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
  LOG(INFO) << "RdbSerializer::SaveShard: shard " << sid
            << " start tid=" << syscall(SYS_gettid);
  const uint64_t now_ms = util::GetCurrentTimeMs();

  std::string tmp = std::string(dir) + "/" + FileName(sid) + ".tmp";
  FILE* f = std::fopen(tmp.c_str(), "wb");
  if (!f) {
    LOG(ERROR) << "RdbSerializer: cannot open " << tmp << " for writing";
    return false;
  }

  std::string buf;
  buf.reserve(1u << 20);
  buf.append(kMagic);

  // 遍历所有 namespace：每个命名空间下该 shard 的全部 db 都落盘，
  // 每条记录前置所属 ns 名，加载时据此路由回对应 DbSlice。
  uint64_t records = 0, expired = 0;
  namespaces->ForEach([&](std::string_view ns_name, Namespace& ns) {
    auto& db_slice = ns.GetDbSlice(sid);
    LOG(INFO) << "[save-shard] shard " << sid << " ns='" << ns_name
              << "' DbCount=" << db_slice.DbCount()
              << " dbslice=" << (void*)&db_slice
              << " tid=" << syscall(SYS_gettid);
    for (DbIndex dbid = 0; dbid < db_slice.DbCount(); ++dbid) {
      LOG(INFO) << "[save-shard] shard " << sid << " ns='" << ns_name
                << "' dbid=" << dbid
                << " db_valid=" << db_slice.IsDbValid(dbid);
      uint64_t ns_records = 0;
      db_slice.TraverseTable(dbid, [&](const PrimeKey& key,
                                       const PrimeValue& val) {
        ++ns_records;
        if (ns_records <= 5) {
          std::string scratch;
          LOG(INFO) << "[save-shard] shard " << sid << " ns='" << ns_name
                    << "' dbid=" << dbid << " key='" << key.GetSlice(&scratch)
                    << "'";
        }
        if (key.HasExpire() &&
            int64_t(now_ms / 1000) >= int64_t(key.GetExpireTime())) {
          ++expired;
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
        ++records;
      });
      LOG(INFO) << "[save-shard] shard " << sid << " ns='" << ns_name
                << "' dbid=" << dbid << " traversed=" << ns_records;
    }
    if (expired > 0) {
      db_slice.ExpireAllIfNeeded();
    }
  });
  buf.push_back(kEofMarker);

  bool ok = std::fwrite(buf.data(), 1, buf.size(), f) == buf.size();
  if (std::fclose(f) != 0) ok = false;
  if (!ok) {
    LOG(ERROR) << "RdbSerializer: write failed for " << tmp;
    std::remove(tmp.c_str());
    return false;
  }

  if (std::rename(tmp.c_str(),
                  (std::string(dir) + "/" + FileName(sid)).c_str()) != 0) {
    LOG(ERROR) << "RdbSerializer: rename " << tmp << " failed";
    std::remove(tmp.c_str());
    return false;
  }
  LOG(INFO) << "RdbSerializer::SaveShard: shard " << sid
            << " done, records=" << records;
  return true;
}

bool RdbSerializer::LoadShard(ShardId sid, std::string_view dir) {
  std::string path = std::string(dir) + "/" + FileName(sid);
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    return true;
  }
  std::string data((std::istreambuf_iterator<char>(in)),
                   std::istreambuf_iterator<char>());
  in.close();
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
    ++records;
    LOG(INFO) << "[load-shard] shard " << sid << " ns='" << ns_name
              << "' dbid=" << dbid << " key='" << key << "'";
  }

  LOG(INFO) << "RdbSerializer::LoadShard: loaded " << records
            << " records from " << path;
  return true;
}

}  // namespace dfly

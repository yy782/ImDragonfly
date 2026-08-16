// Copyright 2026, ImDragonfly authors.  All rights reserved.
// RDB 快照单元测试：保存/加载往返、过期键扫描淘汰、损坏文件拒绝加载。
#include "persistence/rdb_serializer.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "command_layer/command_families.hpp"
#include "cppcoro/async_task.hpp"
#include "detail/common.hpp"
#include "network/redis_server.hpp"
#include "redis/facade/reply_builder.hpp"
#include "sharding/db_slice.hpp"
#include "sharding/namespaces.hpp"
#include "test_util/RESP2Parser.hpp"
#include "transaction_layer/transaction.hpp"
#include "util/Time.hpp"

using namespace dfly;
using namespace dfly::cmn;
using namespace ::cmn;

namespace {
cppcoro::AsyncTask RunCommand(dfly::CommandId* cid, CommandContext* cntx,
                              CmdArgList args) {
  co_await cid->Invoke(cntx, args);
  cntx->rb()->Flush();
  co_return;
}
}  // namespace

const int kShardNum = 5;

class RdbSerializerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_.AsyncLoop();
    sleep(1);
    shard_set = new EngineShardSet(&pool_);
    shard_set->Init(pool_.size());  // Init 内部创建全局 namespaces
    CIs = new CommandRegistry();
    RegisterStringFamily(CIs);
    RegisterGeneric(CIs);
    RegisterListFamily(CIs);
    RegisterHashFamily(CIs);
    RegisterSetFamily(CIs);
    RegisterZSetFamily(CIs);

    dir_ = std::filesystem::temp_directory_path() /
           ("rdb_test_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir_);
    std::filesystem::create_directories(dir_);
  }

  void TearDown() override {
    shard_set->Shutdown();
    pool_.stop();
    sleep(1);
    delete namespaces;
    namespaces = nullptr;
    delete shard_set;
    shard_set = nullptr;
    delete CIs;
    CIs = nullptr;
    std::filesystem::remove_all(dir_);
  }

  std::string Exec(const std::string& cmd, std::vector<std::string> args) {
    auto* cid = CIs->Find(cmd);
    CHECK(cid) << "unknown command: " << cmd;
    args.insert(args.begin(), cmd);

    std::vector<std::string_view> views;
    views.reserve(args.size());
    for (const auto& s : args) views.emplace_back(s);
    CmdArgList arg_list{views};

    std::string reply;
    std::atomic_bool done{false};
    ReplyBuilder rb;
    rb.SetSendCallback([&](std::vector<std::string>&& v) {
      if (!v.empty()) reply = std::move(v.back());
      done.store(true);
    });

    auto tx = util::intrusive_ptr<Transaction>{new Transaction(cid)};
    tx->id = next_tx_id_.fetch_add(1);
    CommandContext cntx(tx, cid, &rb);

    ShardId sid = 0;
    if (cid->first_key_pos() >= 1 &&
        static_cast<int>(args.size()) > cid->first_key_pos()) {
      sid = Shard(args[cid->first_key_pos()], shard_set->size());
    }
    shard_set->Add(sid, [&]() {
      tx->InitByArgs(&namespaces->GetDefaultNamespace(), 0, arg_list);
      RunCommand(cid, &cntx, arg_list);
    });

    for (int i = 0; i < 500 && !done.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    CHECK(done.load()) << "command timed out: " << cmd;
    return reply;
  }

  static std::string RespElement(const std::string& resp) {
    if (resp.empty()) return {};
    if (resp[0] == '+' || resp[0] == ':') {  // simple string / integer
      const size_t end = resp.find("\r\n");
      return resp.substr(1,
                         end == std::string::npos ? resp.size() - 1 : end - 1);
    }
    RESP2Parser p(resp);  // bulk string（含 nil）与 array
    const std::string_view e = p.NextElement();
    return std::string(e.data(), e.size());
  }

  static std::vector<std::string> RespArray(const std::string& resp) {
    RESP2Parser p(resp);
    const int n = p.ReadArrayLen();
    std::vector<std::string> out;
    out.reserve(n > 0 ? static_cast<size_t>(n) : 0);
    for (int i = 0; i < n; ++i) {
      const std::string_view e = p.NextElement();
      out.emplace_back(e.data(), e.size());
    }
    return out;
  }

  // 读取指定 shard 的 dump 文件字节（不存在则返回空串）。
  std::string DumpContent(ShardId sid) {
    std::ifstream f(dir_ / RdbSerializer::FileName(sid), std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(f),
                       std::istreambuf_iterator<char>());
  }

  void SaveAll() {
    for (ShardId s = 0; s < kShardNum; ++s) {
      ASSERT_TRUE(RdbSerializer::SaveShard(s, dir_.string()));
    }
  }

  void LoadAll() {
    for (ShardId s = 0; s < kShardNum; ++s) {
      ASSERT_TRUE(RdbSerializer::LoadShard(s, dir_.string()));
    }
  }

  base::UringProactorPool pool_{kShardNum};
  std::filesystem::path dir_;
  std::atomic<uint64_t> next_tx_id_{0};
};

// ============================================================================
// Test 1: 过期键扫描淘汰
//   SAVE 遍历时顺带删除过期键：过期键不落盘 + 从内存删除，存活键正常落盘。
//   过期后不访问 key，确保走的是 SAVE 扫描路径而非 GET 惰性删除。
// ============================================================================
TEST_F(RdbSerializerTest, ExpiredKeysPurgedDuringSave) {
  EXPECT_EQ("OK", RespElement(Exec("SET", {"ttl_key", "will_expire"})));
  EXPECT_EQ("1", RespElement(Exec("EXPIRE", {"ttl_key", "1"})));
  EXPECT_EQ("OK", RespElement(Exec("SET", {"keep_key", "alive"})));

  // 等待 ttl_key 过期
  std::this_thread::sleep_for(std::chrono::seconds(2));

  SaveAll();

  std::string all;
  for (ShardId s = 0; s < kShardNum; ++s) all += DumpContent(s);
  EXPECT_EQ(all.find("will_expire"), std::string::npos) << "过期键不应落盘";
  EXPECT_NE(all.find("keep_key"), std::string::npos)
      << "存活键应从 dump 中找到";

  // 内存已删：GET 返回 nil（RESP2Parser 对 $-1 返回空）
  EXPECT_TRUE(RespElement(Exec("GET", {"ttl_key"})).empty())
      << "过期键应从内存删除";
  EXPECT_EQ("alive", RespElement(Exec("GET", {"keep_key"})));
}

// ============================================================================
// Test 2: 多 namespace 保存/加载
//   默认 namespace 之外额外创建 ns2 并写入，SaveShard 应遍历所有
//   namespace 全部落盘；LoadShard 按记录中的 ns 名路由回对应 DbSlice。
// ============================================================================
TEST_F(RdbSerializerTest, MultipleNamespacesRoundTrip) {
  // 创建自定义 namespace "ns2" 并在其 shard 0 直接写入
  Namespace& ns2 = namespaces->GetOrInsert("ns2");
  auto& ds2 = ns2.GetDbSlice(0);
  ds2.EnsureDb(0);
  DbContext cntx(&ns2, 0, util::GetCurrentTimeMs());
  ds2.AddOrUpdate(cntx, "ns2_key", CompactValue("ns2_val"), 0);

  // 默认 namespace 也写一个
  EXPECT_EQ("OK", RespElement(Exec("SET", {"def_key", "def_val"})));

  SaveAll();

  // dump 包含两个 namespace 的数据
  std::string all;
  for (ShardId s = 0; s < kShardNum; ++s) all += DumpContent(s);
  EXPECT_NE(all.find("ns2_key"), std::string::npos) << "ns2 数据未落盘";
  EXPECT_NE(all.find("def_key"), std::string::npos) << "默认 ns 数据未落盘";

  LoadAll();

  // ns2 数据加载回原命名空间
  auto it = ds2.FindReadOnly(cntx, "ns2_key");
  ASSERT_FALSE(it.is_done()) << "ns2_key 未从 dump 加载回 ns2";
  std::string scratch;
  EXPECT_EQ("ns2_val", it->second.GetSlice(&scratch));

  // 默认 ns 数据正常（走 Exec 命令路径验证）
  EXPECT_EQ("def_val", RespElement(Exec("GET", {"def_key"})));
}

// ============================================================================
// Test 3: 不存在的 dump 文件 → 视为空 shard，加载成功。
// ============================================================================
TEST_F(RdbSerializerTest, LoadMissingFileOK) {
  auto missing = std::filesystem::temp_directory_path() / "rdb_missing_dir_xyz";
  std::filesystem::remove_all(missing);
  EXPECT_TRUE(RdbSerializer::LoadShard(0, missing.string()));
  std::filesystem::remove_all(missing);
}

// ============================================================================
// Test 5: 损坏的 dump 文件 → 拒绝加载。
// ============================================================================
TEST_F(RdbSerializerTest, LoadCorruptFileRejected) {
  {
    std::ofstream f(dir_ / RdbSerializer::FileName(0), std::ios::binary);
    f << "GARBAGEDATA_NOT_A_VALID_SNAPSHOT";
  }
  EXPECT_FALSE(RdbSerializer::LoadShard(0, dir_.string()));
}

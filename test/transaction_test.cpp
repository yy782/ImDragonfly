#include "transaction_layer/transaction.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "network/redis_server.hpp"
#include "redis/facade/reply_builder.hpp"
#include "sharding/namespaces.hpp"
#include "test_util/RESP2Parser.hpp"

using namespace dfly;
using namespace yy::net;
using namespace dfly::cmn;
using namespace ::cmn;

const int shardNum = 5;

class TransactionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_.run();
    shard_set = new EngineShardSet(&pool_);
    shard_set->Init(pool_.size());
    CIs = new CommandRegistry();
    RegisterStringFamily(CIs);
    namespaces = new Namespaces();
    namespaces->init();
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
  }

  EventLoopThreadPool pool_{shardNum};
};

// ============================================================================
// Test 1: SET / GET (single shard) / MGET (idempotent) / MSET (multi shard)
// ============================================================================

TEST_F(TransactionTest, SetGetMsetMget) {
  auto* Namespace = &namespaces->GetDefaultNamespace();
  auto db_index = 0;

  // ── Step 1: SET a single key via shard determined by key ──
  std::string set_key = "tx_test_key_1";

  // Build SET command args
  std::vector<std::string> set_strings = {"SET", set_key, "hello_tx"};
  std::vector<std::string_view> set_views;
  for (auto& s : set_strings) set_views.push_back(s);
  CmdArgList set_args{set_views};

  auto* set_cid = CIs->Find("SET");
  ASSERT_NE(set_cid, nullptr);

  std::atomic<int> set_done{0};
  ReplyBuilder set_rb;
  set_rb.SetSendCallback([&set_done](std::string&&) { set_done.store(1); });

  auto set_tx = std::make_shared<Transaction>(set_cid);
  set_tx->id = 1;
  CommandContext set_cntx(set_tx, set_cid, &set_rb);

  auto key_sid = Shard(set_key, shard_set->size());
  shard_set->Add(key_sid, [&]() {
    set_tx->InitByArgs(Namespace, db_index, set_args);
    set_cid->Invoke(&set_cntx, set_args);
  });

  for (int retry = 0; retry < 500 && set_done.load() == 0; ++retry) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_EQ(set_done.load(), 1) << "SET did not finish";

  // ── Step 2: GET the same key ──
  std::vector<std::string> get_strings = {"GET", set_key};
  std::vector<std::string_view> get_views;
  for (auto& s : get_strings) get_views.push_back(s);
  CmdArgList get_args{get_views};

  auto* get_cid = CIs->Find("GET");
  ASSERT_NE(get_cid, nullptr);

  std::string get_result;
  std::atomic<int> get_done{0};
  ReplyBuilder get_rb;
  get_rb.SetSendCallback([&get_result, &get_done](std::string&& reply) {
    get_result = std::move(reply);
    get_done.store(1);
  });

  auto get_tx = std::make_shared<Transaction>(get_cid);
  get_tx->id = 2;
  CommandContext get_cntx(get_tx, get_cid, &get_rb);

  shard_set->Add(key_sid, [&]() {
    get_tx->InitByArgs(Namespace, db_index, get_args);
    get_cid->Invoke(&get_cntx, get_args);
  });

  for (int retry = 0; retry < 500 && get_done.load() == 0; ++retry) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_EQ(get_done.load(), 1) << "GET did not finish";

  // Verify GET returns the SET value
  RESP2Parser get_parser(get_result);
  EXPECT_EQ(get_parser.NextElement(), "hello_tx");

  // ── Step 3: MSET multiple keys across shards ──
  const int kKeyCount = 5;
  std::vector<std::string> mset_strs = {"MSET"};
  std::vector<std::string> mset_keys;
  for (int k = 0; k < kKeyCount; ++k) {
    std::string key = "tx_mset_key_" + std::to_string(k);
    mset_keys.push_back(key);
    mset_strs.push_back(key);
    mset_strs.push_back("val_" + std::to_string(k) + "_mset");
  }

  std::vector<std::string_view> mset_views;
  for (auto& s : mset_strs) mset_views.push_back(s);
  CmdArgList mset_args{mset_views};

  auto* mset_cid = CIs->Find("MSET");
  ASSERT_NE(mset_cid, nullptr);

  int mset_tx_count = 0;
  for (const auto& key : mset_keys) {
    auto sid = Shard(key, shard_set->size());
    mset_tx_count = std::max(mset_tx_count, static_cast<int>(sid) + 1);
  }

  std::atomic<int> mset_done{0};
  ReplyBuilder mset_rb;
  mset_rb.SetSendCallback([&mset_done](std::string&&) { mset_done.store(1); });

  auto mset_tx = std::make_shared<Transaction>(mset_cid);
  mset_tx->id = 3;
  CommandContext mset_cntx(mset_tx, mset_cid, &mset_rb);

  // MSET with multi-shard dispatch
  shard_set->Add(0, [&]() {
    mset_tx->InitByArgs(Namespace, db_index, mset_args);
    mset_cid->Invoke(&mset_cntx, mset_args);
  });

  for (int retry = 0; retry < 500 && mset_done.load() == 0; ++retry) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_EQ(mset_done.load(), 1) << "MSET did not finish";

  // ── Step 4: MGET to verify MSET values ──
  std::vector<std::string> mget_strs = {"MGET"};
  for (const auto& key : mset_keys) mget_strs.push_back(key);

  std::vector<std::string_view> mget_views;
  for (auto& s : mget_strs) mget_views.push_back(s);
  CmdArgList mget_args{mget_views};

  auto* mget_cid = CIs->Find("MGET");
  ASSERT_NE(mget_cid, nullptr);

  std::string mget_result;
  std::atomic<int> mget_done{0};
  ReplyBuilder mget_rb;
  mget_rb.SetSendCallback([&mget_result, &mget_done](std::string&& reply) {
    mget_result = std::move(reply);
    mget_done.store(1);
  });

  auto mget_tx = std::make_shared<Transaction>(mget_cid);
  mget_tx->id = 4;
  CommandContext mget_cntx(mget_tx, mget_cid, &mget_rb);

  shard_set->Add(0, [&]() {
    mget_tx->InitByArgs(Namespace, db_index, mget_args);
    mget_cid->Invoke(&mget_cntx, mget_args);
  });

  for (int retry = 0; retry < 500 && mget_done.load() == 0; ++retry) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_EQ(mget_done.load(), 1) << "MGET did not finish";

  // Verify each MGET element matches MSET value
  RESP2Parser mget_parser(mget_result);
  int arr_len_mget = mget_parser.ReadArrayLen();
  ASSERT_EQ(arr_len_mget, kKeyCount);
  for (int k = 0; k < kKeyCount; ++k) {
    std::string_view elem = mget_parser.NextElement();
    EXPECT_EQ(elem, "val_" + std::to_string(k) + "_mset");
  }
}

// ============================================================================
// Test 2: MSET with different keys (differs from MultiConcurrent which uses
//         same keys by iterating over tid), then MGET to verify
// ============================================================================

TEST_F(TransactionTest, MsetDifferentKeysThenMget) {
  auto* Namespace = &namespaces->GetDefaultNamespace();
  auto db_index = 0;

  const int kKeyCount = 10;  // different keys, spread across shards
  const int kRoundCount = 3;

  for (int round = 0; round < kRoundCount; ++round) {
    // ── Build MSET with completely different keys per round ──
    std::vector<std::string> mset_strs = {"MSET"};
    std::vector<std::string> round_keys;
    for (int k = 0; k < kKeyCount; ++k) {
      std::string key = "r" + std::to_string(round) + "_k" + std::to_string(k);
      round_keys.push_back(key);
      mset_strs.push_back(key);
      mset_strs.push_back("round" + std::to_string(round) + "_val" +
                          std::to_string(k));
    }

    std::vector<std::string_view> mset_views;
    for (auto& s : mset_strs) mset_views.push_back(s);
    CmdArgList mset_args{mset_views};

    auto* mset_cid = CIs->Find("MSET");
    ASSERT_NE(mset_cid, nullptr);

    // Verify keys map to different shards
    std::set<ShardId> used_shards;
    for (const auto& key : round_keys) {
      used_shards.insert(Shard(key, shard_set->size()));
    }
    EXPECT_GT(used_shards.size(), 1u)
        << "Keys should span multiple shards for meaningful test";

    std::atomic<int> mset_done{0};
    ReplyBuilder mset_rb;
    mset_rb.SetSendCallback(
        [&mset_done](std::string&&) { mset_done.store(1); });

    auto mset_tx = std::make_shared<Transaction>(mset_cid);
    mset_tx->id = 100 + round;
    CommandContext mset_cntx(mset_tx, mset_cid, &mset_rb);

    shard_set->Add(0, [&]() {
      mset_tx->InitByArgs(Namespace, db_index, mset_args);
      mset_cid->Invoke(&mset_cntx, mset_args);
    });

    for (int retry = 0; retry < 500 && mset_done.load() == 0; ++retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(mset_done.load(), 1)
        << "MSET round " << round << " did not finish";

    // ── MGET to verify ──
    std::vector<std::string> mget_strs = {"MGET"};
    for (const auto& key : round_keys) mget_strs.push_back(key);

    std::vector<std::string_view> mget_views;
    for (auto& s : mget_strs) mget_views.push_back(s);
    CmdArgList mget_args{mget_views};

    auto* mget_cid = CIs->Find("MGET");
    ASSERT_NE(mget_cid, nullptr);

    std::string mget_result;
    std::atomic<int> mget_done{0};
    ReplyBuilder mget_rb;
    mget_rb.SetSendCallback([&mget_result, &mget_done](std::string&& reply) {
      mget_result = std::move(reply);
      mget_done.store(1);
    });

    auto mget_tx = std::make_shared<Transaction>(mget_cid);
    mget_tx->id = 200 + round;
    CommandContext mget_cntx(mget_tx, mget_cid, &mget_rb);

    shard_set->Add(0, [&]() {
      mget_tx->InitByArgs(Namespace, db_index, mget_args);
      mget_cid->Invoke(&mget_cntx, mget_args);
    });

    for (int retry = 0; retry < 500 && mget_done.load() == 0; ++retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(mget_done.load(), 1)
        << "MGET round " << round << " did not finish";

    // ── Verify every value matches ──
    RESP2Parser mget_parser(mget_result);
    int arr_len = mget_parser.ReadArrayLen();
    ASSERT_EQ(arr_len, kKeyCount);
    for (int k = 0; k < kKeyCount; ++k) {
      std::string_view elem = mget_parser.NextElement();
      std::string expected =
          "round" + std::to_string(round) + "_val" + std::to_string(k);
      EXPECT_EQ(elem, expected)
          << "Mismatch: round " << round << " key index " << k;
    }
  }
}

// ============================================================================
// Test 4: VLLLock
//   VLL 意向锁测试：预先持有锁，验证事务在意向锁排斥时仍能成功完成。
//   第一组: 持有所有分片的锁 → 投放 MSET，预期事务成功。
//   第二组: 只持有一个分片的锁 → 投放 MSET，预期事务成功。
// ============================================================================
TEST_F(TransactionTest, VLLLock) {
  auto* Namespace = &namespaces->GetDefaultNamespace();
  auto db_index = 0;

  // ── 准备 MSET 参数，确保至少涉及 2 个分片 ──
  const int kKeyCount = 6;
  std::vector<std::string> mset_strings = {"MSET"};
  for (int k = 0; k < kKeyCount; ++k) {
    std::string key = "lock_opt_key_" + std::to_string(k);
    mset_strings.push_back(key);
    mset_strings.push_back("v" + std::to_string(k));
  }

  // ── 模拟 DetermineKeys: 用 MSET 的 first_key_pos/interleaved_step ──
  //     从 args 中提取 key，与 Transaction::InitByArgs 内部行为完全一致
  auto* mset_cid = CIs->Find("MSET");
  ASSERT_NE(mset_cid, nullptr);
  std::vector<std::string_view> mset_views;
  for (auto& s : mset_strings) mset_views.push_back(s);
  CmdArgList mset_args{mset_views};

  int start = mset_cid->first_key_pos();    // MSET: 1
  int step = mset_cid->interleaved_step();  // MSET: 2
  int end = static_cast<int>(mset_args.size());
  ASSERT_GT(start, 0);
  ASSERT_GT(step, 0);

  // ── 以分片为单位划分指纹组（严格按 DetermineKeys 提取的 key） ──
  std::map<ShardId, std::vector<LockFp>> fps_by_shard;
  std::set<ShardId> involved_shards;
  for (int i = start; i < end; i += step) {
    std::string_view key = mset_args[i];
    ShardId sid = Shard(key, shard_set->size());
    involved_shards.insert(sid);
    fps_by_shard[sid].push_back(LockTag(key).Fingerprint());
  }

  ASSERT_GE(involved_shards.size(), 2u) << "Keys must span at least 2 shards";

  // ========================================================================
  // Group 1: 预先持有所有分片的锁 → 投放 MSET
  //   意向锁排斥场景：事务拿到 key_lock_granted=false，仍应成功完成
  // ========================================================================
  {
    // Step 1: 在所有涉及的分片上获取 EXCLUSIVE 锁
    std::atomic<int> finished{0};
    const int nshards = static_cast<int>(involved_shards.size());

    for (const auto& [sid, fps] : fps_by_shard) {
      shard_set->Add(sid, [&, sid = sid, fps = fps]() {
        auto& db_slice = Namespace->GetDbSlice(sid);
        KeyLockArgs lock_args;
        lock_args.db_index = db_index;
        lock_args.fps = fps;
        db_slice.Acquire(IntentLock::EXCLUSIVE, lock_args);
        finished.fetch_add(1, std::memory_order_release);
      });
    }

    // 测试线程等待 finished == nshards，然后继续下一步
    for (int retry = 0; retry < 500 && finished.load() < nshards; ++retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(finished.load(), nshards)
        << "Group 1: timeout waiting for lock acquisition on all shards";

    // Step 2: 创建 MSET 事务，投放到分片 0
    std::atomic<bool> mset_done{false};
    ReplyBuilder mset_rb;
    mset_rb.SetSendCallback([&mset_done](std::string&&) {
      mset_done.store(true, std::memory_order_release);
    });

    auto mset_tx = std::make_shared<Transaction>(mset_cid);
    mset_tx->id = 2001;
    CommandContext mset_cntx(mset_tx, mset_cid, &mset_rb);

    shard_set->Add(*involved_shards.begin(), [&]() {
      mset_tx->InitByArgs(Namespace, db_index, mset_args);
      mset_cid->Invoke(&mset_cntx, mset_args);
    });

    // Step 3: 用 atomic_bool 轮询 MSET 是否完成
    //   完成 → 符合预期（意向锁排斥下事务仍成功）
    //   超时 → 测试失败
    for (int retry = 0; retry < 500 && !mset_done.load(); ++retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(mset_done.load())
        << "Group 1: MSET should complete even when all shard locks are held";
  }

  // ========================================================================
  // Group 2: 只持有一个分片的锁 → 投放 MSET
  //   仅对一个分片加锁，验证意向锁排斥下事务仍能成功
  // ========================================================================
  {
    // Step 1: 只在一个分片上获取 EXCLUSIVE 锁
    ShardId locked_sid = *involved_shards.begin();
    const auto& locked_fps = fps_by_shard[locked_sid];

    std::atomic<int> finished{0};

    shard_set->Add(locked_sid, [&, fps = locked_fps]() {
      auto& db_slice = Namespace->GetDbSlice(locked_sid);
      KeyLockArgs lock_args;
      lock_args.db_index = db_index;
      lock_args.fps = fps;
      db_slice.Acquire(IntentLock::EXCLUSIVE, lock_args);
      finished.store(1, std::memory_order_release);
    });

    // 测试线程等待锁获取完毕
    for (int retry = 0; retry < 500 && finished.load() == 0; ++retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(finished.load(), 1)
        << "Group 2: timeout waiting for partial lock acquisition";

    // Step 2: 创建 MSET 事务，投放到分片 0
    std::atomic<bool> mset_done{false};
    ReplyBuilder mset_rb;
    mset_rb.SetSendCallback([&mset_done](std::string&&) {
      mset_done.store(true, std::memory_order_release);
    });

    auto mset_tx = std::make_shared<Transaction>(mset_cid);
    mset_tx->id = 2002;
    CommandContext mset_cntx(mset_tx, mset_cid, &mset_rb);

    shard_set->Add(*involved_shards.begin(), [&]() {
      mset_tx->InitByArgs(Namespace, db_index, mset_args);
      mset_cid->Invoke(&mset_cntx, mset_args);
    });

    // Step 3: 用 atomic_bool 轮询 MSET 是否完成
    for (int retry = 0; retry < 500 && !mset_done.load(); ++retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(mset_done.load())
        << "Group 2: MSET should complete even when one shard's lock is held";
  }
}

// ============================================================================
// Test 5: 乐观重试
//   VLL 意向锁 + 乐观重试测试：在 VLLLock 基础上设置 committed_txid，
//   触发事务的乐观重试路径。
//   第一组: 持有所有分片的锁 → 投放 MSET，预期事务成功。
//   第二组: 持有所有分片的锁，但仅一个分片设置 committed_txid → 投放 MSET。
// ============================================================================
TEST_F(TransactionTest, VLLLockRetry) {
  auto* Namespace = &namespaces->GetDefaultNamespace();
  auto db_index = 0;

  // ── 准备 MSET 参数，确保至少涉及 2 个分片 ──
  const int kKeyCount = 6;
  std::vector<std::string> mset_strings = {"MSET"};
  for (int k = 0; k < kKeyCount; ++k) {
    std::string key = "retry_key_" + std::to_string(k);
    mset_strings.push_back(key);
    mset_strings.push_back("v" + std::to_string(k));
  }

  auto* mset_cid = CIs->Find("MSET");
  ASSERT_NE(mset_cid, nullptr);
  std::vector<std::string_view> mset_views;
  for (auto& s : mset_strings) mset_views.push_back(s);
  CmdArgList mset_args{mset_views};

  int start = mset_cid->first_key_pos();
  int step = mset_cid->interleaved_step();
  int end = static_cast<int>(mset_args.size());
  ASSERT_GT(start, 0);
  ASSERT_GT(step, 0);

  std::map<ShardId, std::vector<LockFp>> fps_by_shard;
  std::set<ShardId> involved_shards;
  for (int i = start; i < end; i += step) {
    std::string_view key = mset_args[i];
    ShardId sid = Shard(key, shard_set->size());
    involved_shards.insert(sid);
    fps_by_shard[sid].push_back(LockTag(key).Fingerprint());
  }

  ASSERT_GE(involved_shards.size(), 2u) << "Keys must span at least 2 shards";

  // ========================================================================
  // Group 1: 持有所有分片的锁，设置 committed_txid → 投放 MSET
  // ========================================================================
  {
    std::atomic<int> finished{0};
    const int nshards = static_cast<int>(involved_shards.size());

    for (const auto& [sid, fps] : fps_by_shard) {
      shard_set->Add(sid, [&, sid = sid, fps = fps]() {
        auto& db_slice = Namespace->GetDbSlice(sid);
        KeyLockArgs lock_args;
        lock_args.db_index = db_index;
        lock_args.fps = fps;
        db_slice.Acquire(IntentLock::EXCLUSIVE, lock_args);
        EngineShard::tlocal()->committed_txid() = 5;
        finished.fetch_add(1, std::memory_order_release);
      });
    }

    for (int retry = 0; retry < 500 && finished.load() < nshards; ++retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(finished.load(), nshards)
        << "Group 1: timeout waiting for lock acquisition on all shards";

    std::atomic<bool> mset_done{false};
    ReplyBuilder mset_rb;
    mset_rb.SetSendCallback([&mset_done](std::string&&) {
      mset_done.store(true, std::memory_order_release);
    });

    auto mset_tx = std::make_shared<Transaction>(mset_cid);
    mset_tx->id = 3001;
    CommandContext mset_cntx(mset_tx, mset_cid, &mset_rb);

    shard_set->Add(*involved_shards.begin(), [&]() {
      mset_tx->InitByArgs(Namespace, db_index, mset_args);
      mset_cid->Invoke(&mset_cntx, mset_args);
    });

    for (int retry = 0; retry < 500 && !mset_done.load(); ++retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(mset_done.load()) << "Group 1: MSET should complete via retry "
                                     "even when all shard locks are held";
  }

  // ========================================================================
  // Group 2: 持有所有分片的锁，但仅对一个分片设置 committed_txid → 投放 MSET
  // ========================================================================
  {
    ShardId committed_sid = *involved_shards.begin();
    std::atomic<int> finished{0};
    const int nshards = static_cast<int>(involved_shards.size());

    for (const auto& [sid, fps] : fps_by_shard) {
      shard_set->Add(sid, [&, sid = sid, fps = fps]() {
        auto& db_slice = Namespace->GetDbSlice(sid);
        KeyLockArgs lock_args;
        lock_args.db_index = db_index;
        lock_args.fps = fps;
        db_slice.Acquire(IntentLock::EXCLUSIVE, lock_args);
        if (sid == committed_sid) {
          EngineShard::tlocal()->committed_txid() = 5;
        }
        finished.fetch_add(1, std::memory_order_release);
      });
    }

    for (int retry = 0; retry < 500 && finished.load() < nshards; ++retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(finished.load(), nshards)
        << "Group 2: timeout waiting for lock acquisition on all shards";

    std::atomic<bool> mset_done{false};
    ReplyBuilder mset_rb;
    mset_rb.SetSendCallback([&mset_done](std::string&&) {
      mset_done.store(true, std::memory_order_release);
    });

    auto mset_tx = std::make_shared<Transaction>(mset_cid);
    mset_tx->id = 3002;
    CommandContext mset_cntx(mset_tx, mset_cid, &mset_rb);

    shard_set->Add(*involved_shards.begin(), [&]() {
      mset_tx->InitByArgs(Namespace, db_index, mset_args);
      mset_cid->Invoke(&mset_cntx, mset_args);
    });

    for (int retry = 0; retry < 500 && !mset_done.load(); ++retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(mset_done.load())
        << "Group 2: MSET should complete via retry when committed_txid set on "
           "only one shard";
  }
}

// ============================================================================
// Test 6: MultiConcurrentMGET
//   投放大量 MGET 事务，验证所有事务在规定时间内完成。
// ============================================================================
TEST_F(TransactionTest, MultiConcurrentMGET) {
  auto* Namespace = &namespaces->GetDefaultNamespace();
  auto db_index = 0;

  const int base = 10;
  const int Count = base * shardNum;
  const int P = Count / shardNum;
  const int KeyCount = 5;

  // ── 准备 MGET 命令字符串 ──
  std::vector<std::vector<std::string>> all_cmd_strings(Count);
  for (int j = 0; j < Count; ++j) {
    auto& parts = all_cmd_strings[j];
    parts.push_back("MGET");
    for (int k = 0; k < KeyCount; ++k) {
      parts.push_back("key" + std::to_string(k));
    }
  }

  // ── 构建 string_view 层 ──
  std::vector<std::vector<std::string_view>> all_views(Count);
  for (int j = 0; j < Count; ++j) {
    for (auto& s : all_cmd_strings[j]) {
      all_views[j].push_back(s);
    }
  }

  // ── 构建 CmdArgList ──
  std::vector<CmdArgList> args;
  args.reserve(Count);
  for (int j = 0; j < Count; ++j) {
    args.push_back(CmdArgList{all_views[j]});
  }

  auto* cid = CIs->Find("MGET");
  ASSERT_NE(cid, nullptr);

  std::atomic<int> finish_count{0};
  std::vector<ReplyBuilder> rbs(Count);
  std::vector<CommandContext> cmd_cntxs;
  std::vector<std::shared_ptr<Transaction>> txs;
  cmd_cntxs.reserve(Count);

  for (int j = 0; j < Count; ++j) {
    rbs[j].SetSendCallback([&finish_count](std::string&&) {
      finish_count.fetch_add(1, std::memory_order_release);
    });
    auto tx = std::make_shared<Transaction>(cid);
    tx->id = j;
    txs.push_back(tx);
    cmd_cntxs.emplace_back(tx, cid, &rbs[j]);
  }

  // ── 按分片投放 MGET 事务 ──
  for (int s = 0; s < shardNum; ++s) {
    shard_set->Add(s, [&, s]() {
      for (int start = s * P; start < (s + 1) * P; ++start) {
        auto& tx = txs[start];
        tx->InitByArgs(Namespace, db_index, args[start]);
        cid->Invoke(&cmd_cntxs[start], args[start]);
      }
    });
  }

  // ── 轮询等待所有 MGET 事务结束 ──
  {
    int prev = 0;
    for (int retry = 0; retry < 1000 && finish_count.load() < Count; ++retry) {
      int cur = finish_count.load();
      if (cur > prev) {
        prev = cur;
        retry = 0;  // 有进展则重置重试计数
      }
      if (finish_count.load() < Count) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
  }

  ASSERT_EQ(finish_count.load(), Count) << "Not all MGET transactions finished";
}

// ============================================================================
// Test 3: MultiConcurrent — same keys across shards
//
// ============================================================================

TEST_F(TransactionTest, MultiConcurrent) {
  const int loopCount = 5;
  for (int i = 0; i < loopCount; ++i) {
    const int base = 4;
    const int Count = base * shardNum;
    const int KeyCount = 5;
    const int P = Count / shardNum;
    std::vector<std::string> keys;
    for (int k = 0; k < KeyCount; ++k) {
      keys.push_back("key" + std::to_string(k));
    }

    // 持久化存储 MSET 参数字符串
    std::vector<std::vector<std::string>> all_cmd_strings(Count);
    for (int j = 0; j < Count; ++j) {
      int tid = j / P;
      auto& parts = all_cmd_strings[j];
      parts.push_back("MSET");

      std::vector<std::string> expected_vals;
      for (int k = 0; k < KeyCount; ++k) {
        parts.push_back("key" + std::to_string(k));
        std::string val =
            "val" + std::to_string(k) + "_t" + std::to_string(tid);
        parts.push_back(val);
        expected_vals.push_back(val);
      }
    }

    // 构建 string_view 层
    std::vector<std::vector<std::string_view>> all_views(Count);
    for (int j = 0; j < Count; ++j) {
      for (auto& s : all_cmd_strings[j]) {
        all_views[j].push_back(s);
      }
    }

    // 构建 CmdArgList
    std::vector<CmdArgList> args;
    args.reserve(Count);
    for (int j = 0; j < Count; ++j) {
      args.push_back(CmdArgList{all_views[j]});
    }

    std::vector<std::shared_ptr<Transaction>> txs;

    auto* cid = CIs->Find("MSET");
    ASSERT_NE(cid, nullptr);

    std::atomic<int> finish_count{0};
    std::vector<ReplyBuilder> rbs(Count);
    std::vector<CommandContext> cmd_cntxs;
    cmd_cntxs.reserve(Count);

    for (int j = 0; j < Count; ++j) {
      rbs[j].SetSendCallback([&finish_count](std::string&&) {
        finish_count.fetch_add(1, std::memory_order_release);
      });
      auto tx = std::make_shared<Transaction>(cid);
      tx->id = j;
      txs.push_back(tx);
      cmd_cntxs.emplace_back(tx, cid, &rbs[j]);
    }

    auto* Namespace = &namespaces->GetDefaultNamespace();
    auto db_index = 0;

    for (int s = 0; s < shardNum; ++s) {
      shard_set->Add(s, [&, s]() {
        for (int start = s * P; start < (s + 1) * P; ++start) {
          auto& tx = txs[start];
          tx->InitByArgs(Namespace, db_index, args[start]);
          cid->Invoke(&cmd_cntxs[start], args[start]);
        }
      });
    }
    {
      int prev = 0;
      for (int retry = 0; retry < 1000 && finish_count.load() < Count;
           ++retry) {
        int cur = finish_count.load();
        if (cur > prev) {
          prev = cur;
          retry = 0;  // 有进展则重置
        }
        if (finish_count.load() < Count) {
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
      }
    }

    ASSERT_EQ(finish_count.load(), Count)
        << "Not all MSET transactions finished 当前循环数:" << i;

    LOG(INFO) << "All MSET transactions finished";

    {
      auto* mget_cid = CIs->Find("MGET");
      ASSERT_NE(mget_cid, nullptr);

      std::string mget_result;
      ReplyBuilder mget_rb;
      std::atomic<int> mget_done{0};
      mget_rb.SetSendCallback([&mget_result, &mget_done](std::string&& reply) {
        mget_result = std::move(reply);
        mget_done.store(1);
      });

      auto mget_tx = std::make_shared<Transaction>(mget_cid);
      mget_tx->id = -1;

      std::vector<std::string> mget_strings;
      mget_strings.push_back("MGET");
      for (int k = 0; k < KeyCount; ++k) {
        mget_strings.push_back("key" + std::to_string(k));
      }
      std::vector<std::string_view> mget_views;
      for (auto& s : mget_strings) {
        mget_views.push_back(s);
      }
      CmdArgList mget_args{mget_views};

      CommandContext mget_cntx(mget_tx, mget_cid, &mget_rb);

      shard_set->Add(0, [&]() {
        mget_tx->InitByArgs(Namespace, db_index, mget_args);
        mget_cid->Invoke(&mget_cntx, mget_args);
      });

      for (int retry = 0; retry < 500 && mget_done.load() == 0; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      ASSERT_EQ(mget_done.load(), 1) << "MGET did not finish";

      RESP2Parser parser(mget_result);
      int arr_len = parser.ReadArrayLen();
      ASSERT_EQ(arr_len, KeyCount)
          << "MGET should return " << KeyCount << " elements";

      std::vector<std::string> mget_values;
      for (int k = 0; k < KeyCount; ++k) {
        std::string_view elem = parser.NextElement();
        mget_values.push_back(std::string(elem));
      }

      bool found = false;
      for (int j = 0; j < Count; ++j) {
        bool match = true;
        for (int k = 0; k < KeyCount; ++k) {
          std::string expected = all_cmd_strings[j][1 + k * 2 + 1];
          if (mget_values[k] != expected) {
            match = false;
            break;
          }
        }
        if (match) {
          found = true;
          break;
        }
      }
      EXPECT_TRUE(found) << "MGET result does not match any expected value set."
                         << "\nMGET raw: " << mget_result;
    }
  }
}

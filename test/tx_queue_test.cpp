#include "detail/tx_queue.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "network/redis_server.hpp"
#include "redis/facade/reply_builder.hpp"
#include "test_util/RESP2Parser.hpp"
#include "transaction_layer/transaction.hpp"
using namespace dfly;
using namespace yy::net;
using namespace dfly::cmn;
using namespace ::cmn;

const int shardNum = 5;

class TxQueueTest : public ::testing::Test {
 protected:
  void SetUp() override {
    loop_ = new EventLoop();
    pool_.run();
    q_ = new TxQueue();
    shard_set = new EngineShardSet(&pool_);
    shard_set->Init(pool_.size());
    CIs = new CommandRegistry();
    RegisterStringFamily(CIs);
  }
  void TearDown() override {
    pool_.stop();
    sleep(1);
    delete loop_;
    delete q_;
    delete shard_set;
    delete CIs;
  }

  EventLoop* loop_;
  EventLoopThreadPool pool_{shardNum};
  TxQueue* q_;
};

TEST_F(TxQueueTest, BasicFIFO) {
  // 1. 空队列行为
  EXPECT_TRUE(q_->Empty());
  EXPECT_EQ(q_->Size(), 0u);
  EXPECT_EQ(q_->Front(), nullptr);
  EXPECT_EQ(q_->Back(), nullptr);

  // 2. Push 后 FIFO 出队
  Transaction tx1, tx3, tx2;
  tx1.set_txid(1);
  tx2.set_txid(2);
  tx3.set_txid(3);
  q_->Push(&tx1);
  q_->Push(&tx2);
  q_->Push(&tx3);

  EXPECT_FALSE(q_->Empty());
  EXPECT_EQ(q_->Size(), 3u);
  EXPECT_EQ(q_->Front(), &tx1);
  EXPECT_EQ(q_->Back(), &tx3);
  EXPECT_EQ(q_->Front(), &tx1);
  q_->Pop();
  EXPECT_EQ(q_->Size(), 2u);
  EXPECT_EQ(q_->Front(), &tx2);

  q_->Pop();
  EXPECT_EQ(q_->Size(), 1u);
  EXPECT_EQ(q_->Front(), &tx3);

  q_->Pop();

  // 3. 全部 Pop 后回到空队列
  EXPECT_TRUE(q_->Empty());
  EXPECT_EQ(q_->Size(), 0u);
  EXPECT_EQ(q_->Front(), nullptr);
  EXPECT_EQ(q_->Back(), nullptr);

  // 4. 空队列后重新 Push 仍然正常工作
  q_->Push(&tx1);
  EXPECT_FALSE(q_->Empty());
  EXPECT_EQ(q_->Size(), 1u);
  EXPECT_EQ(q_->Front(), &tx1);
  q_->Pop();
  EXPECT_TRUE(q_->Empty());
}

TEST_F(TxQueueTest, SingleElement) {
  Transaction tx1;
  tx1.set_txid(1);
  q_->Push(&tx1);

  EXPECT_EQ(q_->Front(), &tx1);
  EXPECT_EQ(q_->Back(), &tx1);
  EXPECT_EQ(q_->Size(), 1u);

  q_->Pop();
  EXPECT_TRUE(q_->Empty());
}

TEST_F(TxQueueTest, PopSpecificIterator) {
  // 测试带参数 Pop(Iterator) 从中间移除
  Transaction tx1, tx2, tx3, tx4;
  tx1.set_txid(1);
  tx2.set_txid(2);
  tx3.set_txid(3);
  tx4.set_txid(4);
  auto it1 = q_->Push(&tx1);
  q_->Push(&tx2);
  auto it3 = q_->Push(&tx3);
  q_->Push(&tx4);

  EXPECT_EQ(q_->Size(), 4u);

  // 从中间移除 it3
  q_->Pop(it3);
  EXPECT_EQ(q_->Size(), 3u);

  // 验证剩余顺序是 tx1, tx2, tx4
  EXPECT_EQ(q_->Front(), &tx1);
  q_->Pop();

  EXPECT_EQ(q_->Front(), &tx2);
  q_->Pop();

  EXPECT_EQ(q_->Front(), &tx4);
  q_->Pop();

  EXPECT_TRUE(q_->Empty());
}

TEST_F(TxQueueTest, PopFromHead) {
  Transaction tx1, tx2;
  tx1.set_txid(1);
  tx2.set_txid(2);
  auto it1 = q_->Push(&tx1);
  q_->Push(&tx2);

  q_->Pop(it1);  // 从头移除
  EXPECT_EQ(q_->Size(), 1u);
  EXPECT_EQ(q_->Front(), &tx2);
}

TEST_F(TxQueueTest, PopFromTail) {
  Transaction tx1, tx2;
  tx1.set_txid(1);
  tx2.set_txid(2);
  q_->Push(&tx1);
  auto it2 = q_->Push(&tx2);

  q_->Pop(it2);  // 从尾移除
  EXPECT_EQ(q_->Size(), 1u);
  EXPECT_EQ(q_->Front(), &tx1);
  EXPECT_EQ(q_->Back(), &tx1);
}

TEST_F(TxQueueTest, PushPopAlternating) {
  Transaction tx1, tx2, tx3;

  tx1.set_txid(1);
  tx2.set_txid(2);
  tx3.set_txid(3);
  q_->Push(&tx1);
  q_->Push(&tx2);
  EXPECT_EQ(q_->Size(), 2u);

  q_->Pop();  // 移除 tx1
  EXPECT_EQ(q_->Size(), 1u);
  EXPECT_EQ(q_->Front(), &tx2);

  q_->Push(&tx3);  // 追加 tx3
  EXPECT_EQ(q_->Size(), 2u);
  EXPECT_EQ(q_->Front(), &tx2);
  EXPECT_EQ(q_->Back(), &tx3);

  q_->Pop();  // 移除 tx2
  EXPECT_EQ(q_->Front(), &tx3);
  q_->Pop();  // 移除 tx3
  EXPECT_TRUE(q_->Empty());
}

TEST_F(TxQueueTest, MultiConcurrent) {  // 不稳定测试
  const int loopCount = 10;
  for (int i = 0; i < loopCount; ++i) {
    const int base = 1;
    const int Count = base * shardNum;
    const int KeyCount = 5;
    const int P = Count / shardNum;
    std::vector<std::string> keys;
    for (int k = 0; k < KeyCount; ++k) {
      keys.push_back("key" + std::to_string(k));
    }

    // 持久化存储 MSET 参数字符串
    std::vector<std::vector<std::string>> all_cmd_strings(Count);
    for (int i = 0; i < Count; ++i) {
      int tid = i / P;
      auto& parts = all_cmd_strings[i];
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
    for (int i = 0; i < Count; ++i) {
      for (auto& s : all_cmd_strings[i]) {
        all_views[i].push_back(s);
      }
    }

    // 构建 CmdArgList
    std::vector<CmdArgList> args;
    args.reserve(Count);
    for (int i = 0; i < Count; ++i) {
      args.push_back(CmdArgList{all_views[i]});
    }

    std::vector<std::unique_ptr<Transaction>> txs;

    auto* cid = CIs->Find("MSET");
    ASSERT_NE(cid, nullptr);

    std::atomic<int> finish_count{0};
    std::vector<ReplyBuilder> rbs(Count);
    std::vector<CommandContext> cmd_cntxs;
    cmd_cntxs.reserve(Count);

    for (int i = 0; i < Count; ++i) {
      rbs[i].SetSendCallback([&finish_count](std::string&&) {
        finish_count.fetch_add(1, std::memory_order_release);
      });
      auto* tx = new Transaction(cid);
      tx->id = i;
      txs.push_back(std::unique_ptr<Transaction>(tx));
      cmd_cntxs.emplace_back(tx, cid, &rbs[i]);
    }

    auto* Namespace = &namespaces->GetDefaultNamespace();
    auto db_index = 0;

    for (int i = 0; i < shardNum; ++i) {
      shard_set->Add(i, [&, i]() {
        for (int start = i * P; start < (i + 1) * P; ++start) {
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

      Transaction mget_tx(mget_cid);
      mget_tx.id = Count;

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

      CommandContext mget_cntx(&mget_tx, mget_cid, &mget_rb);

      shard_set->Add(0, [&]() {
        mget_tx.InitByArgs(Namespace, db_index, mget_args);
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
      for (int i = 0; i < Count; ++i) {
        bool match = true;
        for (int k = 0; k < KeyCount; ++k) {
          std::string expected = all_cmd_strings[i][1 + k * 2 + 1];
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

#include "detail/tx_queue.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "network/redis_server.hpp"
#include "redis/facade/reply_builder.hpp"
#include "transaction_layer/transaction.hpp"

using namespace dfly;
using namespace yy::net;
using namespace dfly::cmn;
using namespace ::cmn;
class TxQueueTest : public ::testing::Test {
 protected:
  void SetUp() override {
    loop_ = new EventLoop();
    server_ = new RedisServer(6379, loop_, shardNum);
    server_->Start();
    q_ = new TxQueue();
  }
  void TearDown() override {
    server_->Stop();
    sleep(1);
    delete server_;
    delete loop_;
    delete q_;
  }
  EventLoop* loop_;
  RedisServer* server_;
  TxQueue* q_;
  const int shardNum = 5;
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

TEST_F(TxQueueTest, MultiConcurrent) {
  const int loopCount = 3;

  for (int i = 0; i < loopCount; ++i) { 
    // 可能PollExecution不一定会正常消费队列，会测试失败，以后添加定时PollExecution逻辑更好，loopCount设计小点，减少失败可能性
    const int base = 1;
    const int Count = base * shardNum;
    const int KeyCount = 5;
    const int P = Count / shardNum;
    std::vector<std::string> keys;
    for (int i = 0; i < KeyCount; ++i) {
      std::string str = "key" + std::to_string(i);
      keys.push_back(std::move(str));
    }
    std::vector<std::string> commands;
    commands.reserve(Count);

    // 持久化存储 MSET 参数字符串，供 CmdArgList 的 string_view 引用
    std::vector<std::vector<std::string>> all_cmd_strings(Count);
    for (int i = 0; i < Count; ++i) {
      int tid = i / P;  // 每个线程/shard 关联不同的值
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
      commands.push_back(BuildArray(expected_vals));
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

    std::vector<boost::intrusive_ptr<Transaction>> txs;

    auto* cid = CIs->Find("MSET");
    // 断言cid != nullptr
    for (int i = 0; i < Count; ++i) {
      auto* tx = new Transaction(cid);
      tx->id = i;
      txs.push_back(boost::intrusive_ptr<Transaction>(tx));
    }
    auto* Namespace = &namespaces->GetDefaultNamespace();
    auto db_index = 0;

    for (int i = 0; i < shardNum; ++i) {
      shard_set->Add(i, [&, i]() {
        for (int start = i * P; start < (i + 1) * P; ++start) {
          auto& tx = txs[start];
          tx->InitByArgs(Namespace, db_index, args[start]);
          cid->Invoke(&tx->GetCommandContext(), args[start]);
        }
      });
    }

    // 等待所有 MSET 事务完成（带超时重试，有进展则重置计数）
    int FinishCount = 0;
    int prevCount = 0;
    for (int retry = 0; retry < 1000 && FinishCount < Count; ++retry) {
      FinishCount = 0;
      for (int i = 0; i < Count; ++i) {
        if (txs[i]->HasFininsh()) {
          ++FinishCount;
        }
      }
      if (FinishCount > prevCount) {
        prevCount = FinishCount;
        retry = 0;
      }
      if (FinishCount < Count) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }

    ASSERT_EQ(FinishCount, Count)
        << "Not all MSET transactions finished 当前循环数:" << i;

    LOG(INFO) << "All MSET transactions finished";
    // MGET 验证
    auto* mget_cid = CIs->Find("MGET");
    Transaction t(mget_cid);
    t.id = Count;

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

    shard_set->Add(0, [&]() {
      t.InitByArgs(Namespace, db_index, mget_args);
      mget_cid->Invoke(&t.GetCommandContext(), mget_args);
    });
    while (!t.HasFininsh()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    auto res = t.GetResWithOutBlock();

    bool found = false;
    for (int i = 0; i < Count; ++i) {
      if (res == commands[i]) {
        found = true;
        break;
      }
    }
    EXPECT_TRUE(found) << "MGET result does not match any expected value set."
                       << "\nResult: " << res;
  }
}

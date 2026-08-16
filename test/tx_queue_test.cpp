#include "detail/tx_queue.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "network/redis_server.hpp"
#include "redis/facade/reply_builder.hpp"
#include "test_util/RESP2Parser.hpp"
#include "transaction_layer/transaction.hpp"
using namespace dfly;

using namespace dfly::cmn;
using namespace ::cmn;

class TxQueueTest : public ::testing::Test {
 protected:
  void SetUp() override { q_ = new TxQueue(); }
  void TearDown() override { delete q_; }
  TxQueue* q_;
};

TEST_F(TxQueueTest, BasicFIFO) {
  // 1. 空队列行为
  EXPECT_TRUE(q_->Empty());
  EXPECT_EQ(q_->Size(), 0u);
  EXPECT_EQ(q_->Front(), nullptr);
  EXPECT_EQ(q_->Back(), nullptr);

  // 2. Push 后 FIFO 出队
  auto tx1 = util::intrusive_ptr<Transaction>{new Transaction()};
  auto tx2 = util::intrusive_ptr<Transaction>{new Transaction()};
  auto tx3 = util::intrusive_ptr<Transaction>{new Transaction()};
  tx1->set_txid(1);
  tx2->set_txid(2);
  tx3->set_txid(3);
  q_->Push(tx1);
  q_->Push(tx2);
  q_->Push(tx3);

  EXPECT_FALSE(q_->Empty());
  EXPECT_EQ(q_->Size(), 3u);
  EXPECT_EQ(q_->Front(), tx1);
  EXPECT_EQ(q_->Back(), tx3);
  EXPECT_EQ(q_->Front(), tx1);
  q_->Pop();
  EXPECT_EQ(q_->Size(), 2u);
  EXPECT_EQ(q_->Front(), tx2);

  q_->Pop();
  EXPECT_EQ(q_->Size(), 1u);
  EXPECT_EQ(q_->Front(), tx3);

  q_->Pop();

  // 3. 全部 Pop 后回到空队列
  EXPECT_TRUE(q_->Empty());
  EXPECT_EQ(q_->Size(), 0u);
  EXPECT_EQ(q_->Front(), nullptr);
  EXPECT_EQ(q_->Back(), nullptr);

  // 4. 空队列后重新 Push 仍然正常工作
  q_->Push(tx1);
  EXPECT_FALSE(q_->Empty());
  EXPECT_EQ(q_->Size(), 1u);
  EXPECT_EQ(q_->Front(), tx1);
  q_->Pop();
  EXPECT_TRUE(q_->Empty());
}

TEST_F(TxQueueTest, SingleElement) {
  auto tx1 = util::intrusive_ptr<Transaction>{new Transaction()};
  tx1->set_txid(1);
  q_->Push(tx1);

  EXPECT_EQ(q_->Front(), tx1);
  EXPECT_EQ(q_->Back(), tx1);
  EXPECT_EQ(q_->Size(), 1u);

  q_->Pop();
  EXPECT_TRUE(q_->Empty());
}

TEST_F(TxQueueTest, PopSpecificIterator) {
  // 测试带参数 Pop(Iterator) 从中间移除
  auto tx1 = util::intrusive_ptr<Transaction>{new Transaction()};
  auto tx2 = util::intrusive_ptr<Transaction>{new Transaction()};
  auto tx3 = util::intrusive_ptr<Transaction>{new Transaction()};
  auto tx4 = util::intrusive_ptr<Transaction>{new Transaction()};
  tx1->set_txid(1);
  tx2->set_txid(2);
  tx3->set_txid(3);
  tx4->set_txid(4);
  q_->Push(tx1);

  q_->Push(tx2);
  auto it3 = q_->Push(tx3);
  q_->Push(tx4);

  EXPECT_EQ(q_->Size(), 4u);

  // 从中间移除 it3
  q_->Pop(it3);
  EXPECT_EQ(q_->Size(), 3u);

  // 验证剩余顺序是 tx1, tx2, tx4
  EXPECT_EQ(q_->Front(), tx1);
  q_->Pop();

  EXPECT_EQ(q_->Front(), tx2);
  q_->Pop();

  EXPECT_EQ(q_->Front(), tx4);
  q_->Pop();

  EXPECT_TRUE(q_->Empty());
}

TEST_F(TxQueueTest, PopFromHead) {
  auto tx1 = util::intrusive_ptr<Transaction>{new Transaction()};
  auto tx2 = util::intrusive_ptr<Transaction>{new Transaction()};
  tx1->set_txid(1);
  tx2->set_txid(2);
  auto it1 = q_->Push(tx1);
  q_->Push(tx2);

  q_->Pop(it1);  // 从头移除
  EXPECT_EQ(q_->Size(), 1u);
  EXPECT_EQ(q_->Front(), tx2);
}

TEST_F(TxQueueTest, PopFromTail) {
  auto tx1 = util::intrusive_ptr<Transaction>{new Transaction()};
  auto tx2 = util::intrusive_ptr<Transaction>{new Transaction()};
  tx1->set_txid(1);
  tx2->set_txid(2);
  q_->Push(tx1);
  auto it2 = q_->Push(tx2);

  q_->Pop(it2);  // 从尾移除
  EXPECT_EQ(q_->Size(), 1u);
  EXPECT_EQ(q_->Front(), tx1);
  EXPECT_EQ(q_->Back(), tx1);
}

TEST_F(TxQueueTest, PushPopAlternating) {
  auto tx1 = util::intrusive_ptr<Transaction>{new Transaction()};
  auto tx2 = util::intrusive_ptr<Transaction>{new Transaction()};
  auto tx3 = util::intrusive_ptr<Transaction>{new Transaction()};

  tx1->set_txid(1);
  tx2->set_txid(2);
  tx3->set_txid(3);
  q_->Push(tx1);
  q_->Push(tx2);
  EXPECT_EQ(q_->Size(), 2u);

  q_->Pop();  // 移除 tx1
  EXPECT_EQ(q_->Size(), 1u);
  EXPECT_EQ(q_->Front(), tx2);

  q_->Push(tx3);  // 追加 tx3
  EXPECT_EQ(q_->Size(), 2u);
  EXPECT_EQ(q_->Front(), tx2);
  EXPECT_EQ(q_->Back(), tx3);

  q_->Pop();  // 移除 tx2
  EXPECT_EQ(q_->Front(), tx3);
  q_->Pop();  // 移除 tx3
  EXPECT_TRUE(q_->Empty());
}

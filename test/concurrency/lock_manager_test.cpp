/**
 * grading_lock_manager_test.cpp
 */

#include <atomic>
#include <random>

#include "common/exception.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace bustub {

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ(txn->GetSharedLockSet()->size(), shared_size);
  EXPECT_EQ(txn->GetExclusiveLockSet()->size(), exclusive_size);
}

// --- Real tests ---
void WoundWaitBasicTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid0{0, 0};
  RID rid1{0, 1};

  std::mutex id_mutex;
  size_t id_hold = 0;
  size_t id_wait = 10;
  size_t id_kill = 1;

  size_t num_wait = 10;
  size_t num_kill = 1;

  std::vector<std::thread> wait_threads;
  std::vector<std::thread> kill_threads;

  Transaction txn(id_hold);
  txn_mgr.Begin(&txn);
  lock_mgr.LockExclusive(&txn, rid0);
  lock_mgr.LockShared(&txn, rid1);

  auto wait_die_task = [&]() {
    id_mutex.lock();
    Transaction wait_txn(id_wait++);
    id_mutex.unlock();
    bool res;
    txn_mgr.Begin(&wait_txn);
    res = lock_mgr.LockShared(&wait_txn, rid1);
    EXPECT_TRUE(res);
    CheckGrowing(&wait_txn);
    CheckTxnLockSize(&wait_txn, 1, 0);
    try {
      res = lock_mgr.LockExclusive(&wait_txn, rid0);
      EXPECT_FALSE(res) << wait_txn.GetTransactionId() << "ERR";
    } catch (const TransactionAbortException &e) {
    } catch (const Exception &e) {
      EXPECT_TRUE(false) << "Test encountered exception" << e.what();
    }

    CheckAborted(&wait_txn);

    txn_mgr.Abort(&wait_txn);
  };

  // All transaction here should wait.
  for (size_t i = 0; i < num_wait; i++) {
    wait_threads.emplace_back(std::thread{wait_die_task});
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // TODO(peijingx): guarantee all are waiting on LockExclusive
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  auto kill_task = [&]() {
    Transaction kill_txn(id_kill++);

    bool res;
    txn_mgr.Begin(&kill_txn);
    res = lock_mgr.LockShared(&kill_txn, rid1);
    EXPECT_TRUE(res);
    CheckGrowing(&kill_txn);
    CheckTxnLockSize(&kill_txn, 1, 0);

    res = lock_mgr.LockShared(&kill_txn, rid0);
    EXPECT_TRUE(res);
    CheckGrowing(&kill_txn);
    CheckTxnLockSize(&kill_txn, 2, 0);

    txn_mgr.Commit(&kill_txn);
    CheckCommitted(&kill_txn);
    CheckTxnLockSize(&kill_txn, 0, 0);
  };

  for (size_t i = 0; i < num_kill; i++) {
    kill_threads.emplace_back(std::thread{kill_task});
  }

  for (size_t i = 0; i < num_wait; i++) {
    wait_threads[i].join();
  }

  CheckGrowing(&txn);
  txn_mgr.Commit(&txn);
  CheckCommitted(&txn);

  for (size_t i = 0; i < num_kill; i++) {
    kill_threads[i].join();
  }
}

// Two threads, check if one will abort.
void WoundWaitDeadlockTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr(&lock_mgr);
  RID rid0(0, 0);
  RID rid1(0, 1);
  Transaction txn1(1);
  txn_mgr.Begin(&txn1);

  bool res;
  res = lock_mgr.LockExclusive(&txn1, rid0);  // txn id 1 has lock on rid0
  EXPECT_TRUE(res);                           // should be granted
  CheckGrowing(&txn1);
  // This will imediately take the lock

  // Use promise and future to identify
  auto task = [&](std::promise<void> lock_acquired) {
    Transaction txn0(0);  // this transaction is older than txn1
    txn_mgr.Begin(&txn0);
    bool res;
    res = lock_mgr.LockExclusive(&txn0,
                                 rid1);  // we should grant it here and make sure that txn1 is aborted.
    EXPECT_TRUE(res);
    CheckGrowing(&txn0);
    res = lock_mgr.LockExclusive(&txn0, rid0);  // this should abort the main txn

    EXPECT_TRUE(res);
    CheckGrowing(&txn0);
    txn_mgr.Commit(&txn0);

    lock_acquired.set_value();
  };

  std::promise<void> lock_acquired;
  std::future<void> lock_acquired_future = lock_acquired.get_future();

  std::thread t{task, std::move(lock_acquired)};
  auto otask = [&](Transaction *tx) {
    while (tx->GetState() != TransactionState::ABORTED) {
    }
    txn_mgr.Abort(tx);
  };
  std::thread w{otask, &txn1};
  lock_acquired_future.wait();  // waiting for the thread to acquire exclusive lock for rid1
  // this should fail, and txn abort and release all the locks.

  t.join();
  w.join();
}

// Large number of lock and unlock operations.
void WoundWaitStressTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  std::mt19937 generator(time(nullptr));

  size_t num_rids = 100;
  size_t num_threads = 1000;

  std::vector<RID> rids;
  for (uint32_t i = 0; i < num_rids; i++) {
    RID rid{0, i};
    rids.push_back(rid);
  }

  // Task1 is to get shared lock until abort
  auto task1 = [&](int tid) {
    Transaction txn(tid);
    int num_shared = 0;
    int mod = 2;
    for (size_t i = 0; i < rids.size(); i++) {
      if (i % mod == 0) {
        bool res = lock_mgr.LockShared(&txn, rids[i]);
        if (!res) {
          CheckAborted(&txn);
          txn_mgr.Abort(&txn);
          CheckTxnLockSize(&txn, 0, 0);
          return;
        }
        num_shared++;
        CheckTxnLockSize(&txn, num_shared, 0);
      }
    }
    CheckGrowing(&txn);
    for (size_t i = 0; i < rids.size(); i++) {
      if (i % mod == 0) {
        bool res = lock_mgr.Unlock(&txn, rids[i]);
        EXPECT_TRUE(res);  // unlock here should be ok
        CheckShrinking(&txn);
      }
    }
    CheckTxnLockSize(&txn, 0, 0);
  };

  // Task 2 is shared lock from the back
  auto task2 = [&](int tid) {
    Transaction txn(tid);
    int mod = 3;
    int num_shared = 0;
    for (int i = static_cast<int>(rids.size()) - 1; i >= 0; i--) {
      if (i % mod == 0) {
        bool res = lock_mgr.LockShared(&txn, rids[i]);
        if (!res) {
          CheckAborted(&txn);
          txn_mgr.Abort(&txn);
          CheckTxnLockSize(&txn, 0, 0);
          return;
        }
        num_shared++;
        CheckTxnLockSize(&txn, num_shared, 0);
      }
    }
    CheckGrowing(&txn);
    txn_mgr.Commit(&txn);
    CheckTxnLockSize(&txn, 0, 0);
  };

  // Shared lock wants to upgrade
  auto task3 = [&](int tid) {
    Transaction txn(tid);
    int num_exclusive = 0;
    int mod = 6;
    bool res;
    for (size_t i = 0; i < rids.size(); i++) {
      if (i % mod == 0) {
        res = lock_mgr.LockShared(&txn, rids[i]);
        if (!res) {
          CheckTxnLockSize(&txn, 0, num_exclusive);
          CheckAborted(&txn);
          txn_mgr.Abort(&txn);
          CheckTxnLockSize(&txn, 0, 0);
          return;
        }
        CheckTxnLockSize(&txn, 1, num_exclusive);
        res = lock_mgr.LockUpgrade(&txn, rids[i]);
        if (!res) {
          CheckAborted(&txn);
          txn_mgr.Abort(&txn);
          CheckTxnLockSize(&txn, 0, 0);
          return;
        }
        num_exclusive++;
        CheckTxnLockSize(&txn, 0, num_exclusive);
        CheckGrowing(&txn);
      }
    }
    for (size_t i = 0; i < rids.size(); i++) {
      if (i % mod == 0) {
        res = lock_mgr.Unlock(&txn, rids[i]);

        EXPECT_TRUE(res);
        CheckShrinking(&txn);
        // A fresh RID here
        RID rid{tid, static_cast<uint32_t>(tid)};
        res = lock_mgr.LockShared(&txn, rid);
        EXPECT_FALSE(res);

        CheckAborted(&txn);
        txn_mgr.Abort(&txn);
        CheckTxnLockSize(&txn, 0, 0);
        return;
      }
    }
  };

  // Exclusive lock and unlock
  auto task4 = [&](int tid) {
    Transaction txn(tid);
    // randomly pick
    int index = static_cast<int>(generator() % rids.size());
    bool res = lock_mgr.LockExclusive(&txn, rids[index]);
    if (res) {
      bool res = lock_mgr.Unlock(&txn, rids[index]);
      EXPECT_TRUE(res);
    } else {
      txn_mgr.Abort(&txn);
    }
    CheckTxnLockSize(&txn, 0, 0);
  };

  std::vector<std::function<void(int)>> tasks{task1, task2, task4};
  std::vector<std::thread> threads;
  // only one task3 to ensure success upgrade most of the time
  threads.emplace_back(std::thread{task3, num_threads / 2});
  for (size_t i = 0; i < num_threads; i++) {
    if (i != num_threads / 2) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
  }
  for (size_t i = 0; i < num_threads; i++) {
    // Threads might be returned already
    if (threads[i].joinable()) {
      threads[i].join();
    }
  }
}

// Basically a simple multithreaded version of basic upgrade test
void WoundUpgradeTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  Transaction txn(50);
  txn_mgr.Begin(&txn);
  RID rid(0, 0);
  lock_mgr.LockShared(&txn, rid);
  std::atomic<bool> finish_update(false);

  std::atomic<int> id_upgrade = 0;
  std::atomic<int> id_read = 1;

  auto read_task = [&](const std::shared_future<void> &unlock_future) {
    Transaction txn1(id_read++);

    txn_mgr.Begin(&txn1);
    lock_mgr.LockShared(&txn1, rid);
    unlock_future.wait();
    CheckAborted(&txn1);
    txn_mgr.Abort(&txn1);
  };

  auto kill_og = [&](Transaction *tx, const std::shared_future<void> &unlock_future) {
    unlock_future.wait();
    CheckAborted(tx);
    txn_mgr.Abort(tx);
  };

  auto upgrade_task = [&](std::promise<void> un) {
    Transaction txn2(id_upgrade);
    txn_mgr.Begin(&txn2);
    bool res = lock_mgr.LockShared(&txn2, rid);

    EXPECT_TRUE(res);
    CheckTxnLockSize(&txn2, 1, 0);

    res = lock_mgr.LockUpgrade(&txn2, rid);
    EXPECT_TRUE(res);
    CheckTxnLockSize(&txn2, 0, 1);

    un.set_value();
    finish_update = true;
    txn_mgr.Commit(&txn2);
  };

  std::promise<void> unlock;
  std::shared_future<void> unlock_future(unlock.get_future());
  std::vector<std::thread> threads;

  size_t num_threads = 30;
  for (size_t i = 1; i < num_threads; i++) {
    threads.emplace_back(std::thread{read_task, unlock_future});
  }
  threads.emplace_back(std::thread{kill_og, &txn, unlock_future});

  // wait enough time to ensure the read tasks have locked and are waiting
  // on future
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  threads.emplace_back(std::thread{upgrade_task, std::move(unlock)});
  for (auto &thread : threads) {
    thread.join();
  }
}

void FairnessTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};
  Transaction txn_3(3);
  txn_mgr.Begin(&txn_3);
  lock_mgr.LockExclusive(&txn_3, rid);

  std::atomic<size_t> num_ready = 0;
  std::atomic<size_t> read_index = 0;
  std::vector<int> reads_id{5, 4, 6};

  auto read_task = [&]() {
    Transaction txn(reads_id[read_index++]);
    txn_mgr.Begin(&txn);
    bool res = lock_mgr.LockShared(&txn, rid);
    num_ready++;
    EXPECT_TRUE(res);
    // wait for the write thread to abort this txn
    std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    CheckAborted(&txn);
    txn_mgr.Abort(&txn);
  };

  auto write_task = [&](int id, bool expected_res) {
    Transaction txn(id);
    txn_mgr.Begin(&txn);
    bool res = lock_mgr.LockExclusive(&txn, rid);
    EXPECT_EQ(res, expected_res);
  };

  // This might be flaky
  std::vector<std::thread> reads;
  // push first three
  for (size_t i = 0; i < reads_id.size(); i++) {
    reads.emplace_back(std::thread{read_task});
  }

  txn_mgr.Commit(&txn_3);
  CheckCommitted(&txn_3);
  // wait for all read_tasks to grab latch
  while (num_ready != reads_id.size()) {
    std::this_thread::yield();
  }

  std::thread write1{write_task, 1, true};  // should abort all the reads

  write1.join();
  // all reads should join back
  for (size_t i = 0; i < 3; i++) {
    reads[i].join();
  }
}

// Test write fairness
void FairnessTest2() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};
  Transaction txn1(10);
  txn_mgr.Begin(&txn1);

  lock_mgr.LockExclusive(&txn1, rid);
  std::mutex mutex;
  std::vector<int> txn_ids;

  std::mutex index_mutex;
  size_t write_index = 0;

  std::vector<int> write_ids{7, 5, 6, 4, 9};
  std::vector<bool> write_expected{true, true, false, true, true};
  std::vector<bool> abort_expected{true, true, true, false, false};
  std::promise<void> up1;

  auto write_task = [&](const std::shared_future<void> &f) {
    index_mutex.lock();
    size_t index = write_index++;
    index_mutex.unlock();

    int id = write_ids[index];
    bool expected_res = write_expected[index];
    bool should_abort = abort_expected[index];

    Transaction txn(id);
    txn_mgr.Begin(&txn);
    bool res;
    try {
      res = lock_mgr.LockExclusive(&txn, rid);
      EXPECT_EQ(expected_res, res);
    } catch (const TransactionAbortException &e) {
      EXPECT_FALSE(expected_res);
      res = false;
    } catch (const Exception &e) {
      EXPECT_TRUE(false) << "Test encountered exception" << e.what();
    }

    if (res) {
      mutex.lock();
      txn_ids.push_back(id);
      mutex.unlock();
      f.wait();
      if (should_abort) {
        CheckAborted(&txn);
        txn_mgr.Abort(&txn);
        // LOG_DEBUG("true abort %d", (int)id);
      } else {
        txn_mgr.Commit(&txn);
        // LOG_DEBUG("true commit %d", (int)id);
      }
    } else {
      f.wait();
      if (should_abort) {
        CheckAborted(&txn);
        txn_mgr.Abort(&txn);
        // LOG_DEBUG("false abort %d", (int)id);
      }
    }
  };
  std::vector<std::thread> writes;
  std::shared_future<void> uf1(up1.get_future());

  for (size_t i = 0; i < write_ids.size(); i++) {
    writes.emplace_back(std::thread{write_task, uf1});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Make sure writer wounds all readers
  up1.set_value();
  // all abort thread should be joined back
  for (size_t i = 0; i < write_ids.size(); i++) {
    if (!write_expected[i]) {
      writes[i].join();
    }
  }
  // LOG_DEBUG("hhhhhhhhhhh");
  CheckAborted(&txn1);
  // Join back rest of threads
  for (size_t i = 0; i < write_ids.size(); i++) {
    if (write_expected[i]) {
      writes[i].join();
    }
  }

  // we expect get results in order. Just to make sure with mutex
  size_t j = 0;
  mutex.lock();
  for (size_t i = 0; i < write_ids.size(); i++) {
    if (write_expected[i]) {
      EXPECT_EQ(write_ids[i], txn_ids[j++]);
    }
  }
  mutex.unlock();
}

/****************************
 * Prevention Tests (55 pts)
 ****************************/

const size_t NUM_ITERS = 10;

/*
 * Score 10
 * Description: Check basic case if later txn will
 * die when it's waiting for previous txn is also waiting
 */
TEST(LockManagerTest, WoundWaitTest) {
  for (size_t i = 0; i < NUM_ITERS; i++) {
    WoundWaitBasicTest();
  }
}

/*
 * Score 10
 * Description: Large number of concurrent operations.
 * The main point for this test is to ensure no deadlock
 * happen (test won't hang).
 */
TEST(LockManagerTest, WoundWaitDeadlockTest) {
  for (size_t i = 0; i < NUM_ITERS; i++) {
    WoundWaitDeadlockTest();
  }
}

/*
 * Score 10
 * Description: test lock upgrade.
 * 1) Test 1 is for single thread upgrade test
 * 2) Test 2 is for simple two threads upgrade test
 * 3) Test 3 has a queue with several read requests followed by write requests
 *    and one read request wants to upgrade. It will eventually get the lock
 *    after the all waiting write requests.
 *    Test 3 also tests if later txn won't be added into the wait queue
 *    if the queue has transactions with smaller tid.
 */
TEST(LockManagerTest, WoundUpgradeTest) {
  for (size_t i = 0; i < NUM_ITERS; i++) {
    WoundUpgradeTest();
  }
}

/*
 * Score: 10
 * Description:
 * 1) Queue is fair for incoming read requests
 * 2) Queue is fair for incoming read and write requests
 */
TEST(LockManagerTest, WoundWaitFairnessTest) {
  for (size_t i = 0; i < NUM_ITERS; i++) {
    FairnessTest1();
    FairnessTest2();
  }
}

}  // namespace bustub

// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // transaction_test.cpp
// //
// // Identification: test/concurrency/transaction_test.cpp
// //
// // Copyright (c) 2015-2021, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include <atomic>
// #include <cstdio>
// #include <memory>
// #include <random>
// #include <string>
// #include <utility>
// #include <vector>

// #include "buffer/buffer_pool_manager_instance.h"
// #include "catalog/table_generator.h"
// #include "concurrency/transaction.h"
// #include "concurrency/transaction_manager.h"
// #include "execution/execution_engine.h"
// #include "execution/executor_context.h"
// #include "execution/executors/insert_executor.h"
// #include "execution/expressions/aggregate_value_expression.h"
// #include "execution/expressions/column_value_expression.h"
// #include "execution/expressions/comparison_expression.h"
// #include "execution/expressions/constant_value_expression.h"
// #include "execution/plans/limit_plan.h"
// #include "execution/plans/nested_index_join_plan.h"
// #include "execution/plans/seq_scan_plan.h"
// #include "gtest/gtest.h"
// #include "test_util.h"  // NOLINT
// #include "type/value_factory.h"

// #define TEST_TIMEOUT_BEGIN                           \
//   std::promise<bool> promisedFinished;               \
//   auto futureResult = promisedFinished.get_future(); \
//                               std::thread([](std::promise<bool>& finished) {
// #define TEST_TIMEOUT_FAIL_END(X)                                                                  \
//   finished.set_value(true);                                                                       \
//   }, std::ref(promisedFinished)).detach();                                                        \
//   EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
//       << "Test Failed Due to Time Out";

// namespace bustub {

// class TransactionTest : public ::testing::Test {
//  public:
//   // This function is called before every test.
//   void SetUp() override {
//     ::testing::Test::SetUp();
//     // For each test, we create a new DiskManager, BufferPoolManager, TransactionManager, and Catalog.
//     disk_manager_ = std::make_unique<DiskManager>("executor_test.db");
//     bpm_ = std::make_unique<BufferPoolManagerInstance>(2560, disk_manager_.get());
//     page_id_t page_id;
//     bpm_->NewPage(&page_id);
//     lock_manager_ = std::make_unique<LockManager>();
//     txn_mgr_ = std::make_unique<TransactionManager>(lock_manager_.get(), log_manager_.get());
//     catalog_ = std::make_unique<Catalog>(bpm_.get(), lock_manager_.get(), log_manager_.get());
//     // Begin a new transaction, along with its executor context.
//     txn_ = txn_mgr_->Begin();
//     exec_ctx_ =
//         std::make_unique<ExecutorContext>(txn_, catalog_.get(), bpm_.get(), txn_mgr_.get(), lock_manager_.get());
//     // Generate some test tables.
//     TableGenerator gen{exec_ctx_.get()};
//     gen.GenerateTestTables();

//     execution_engine_ = std::make_unique<ExecutionEngine>(bpm_.get(), txn_mgr_.get(), catalog_.get());
//   }

//   // This function is called after every test.
//   void TearDown() override {
//     // Commit our transaction.
//     txn_mgr_->Commit(txn_);
//     // Shut down the disk manager and clean up the transaction.
//     disk_manager_->ShutDown();
//     remove("executor_test.db");
//     delete txn_;
//   };

//   /** @return the executor context in our test class */
//   auto GetExecutorContext() -> ExecutorContext * { return exec_ctx_.get(); }
//   auto GetExecutionEngine() -> ExecutionEngine * { return execution_engine_.get(); }
//   auto GetTxn() -> Transaction * { return txn_; }
//   auto GetTxnManager() -> TransactionManager * { return txn_mgr_.get(); }
//   auto GetCatalog() -> Catalog * { return catalog_.get(); }
//   auto GetBPM() -> BufferPoolManager * { return bpm_.get(); }
//   auto GetLockManager() -> LockManager * { return lock_manager_.get(); }

//   // The below helper functions are useful for testing.

//   auto MakeColumnValueExpression(const Schema &schema, uint32_t tuple_idx, const std::string &col_name)
//       -> const AbstractExpression * {
//     uint32_t col_idx = schema.GetColIdx(col_name);
//     auto col_type = schema.GetColumn(col_idx).GetType();
//     allocated_exprs_.emplace_back(std::make_unique<ColumnValueExpression>(tuple_idx, col_idx, col_type));
//     return allocated_exprs_.back().get();
//   }

//   auto MakeConstantValueExpression(const Value &val) -> const AbstractExpression * {
//     allocated_exprs_.emplace_back(std::make_unique<ConstantValueExpression>(val));
//     return allocated_exprs_.back().get();
//   }

//   auto MakeComparisonExpression(const AbstractExpression *lhs, const AbstractExpression *rhs, ComparisonType
//   comp_type)
//       -> const AbstractExpression * {
//     allocated_exprs_.emplace_back(std::make_unique<ComparisonExpression>(lhs, rhs, comp_type));
//     return allocated_exprs_.back().get();
//   }

//   auto MakeAggregateValueExpression(bool is_group_by_term, uint32_t term_idx) -> const AbstractExpression * {
//     allocated_exprs_.emplace_back(
//         std::make_unique<AggregateValueExpression>(is_group_by_term, term_idx, TypeId::INTEGER));
//     return allocated_exprs_.back().get();
//   }

//   auto MakeOutputSchema(const std::vector<std::pair<std::string, const AbstractExpression *>> &exprs)
//       -> const Schema * {
//     std::vector<Column> cols;
//     cols.reserve(exprs.size());
//     for (const auto &input : exprs) {
//       if (input.second->GetReturnType() != TypeId::VARCHAR) {
//         cols.emplace_back(input.first, input.second->GetReturnType(), input.second);
//       } else {
//         cols.emplace_back(input.first, input.second->GetReturnType(), MAX_VARCHAR_SIZE, input.second);
//       }
//     }
//     allocated_output_schemas_.emplace_back(std::make_unique<Schema>(cols));
//     return allocated_output_schemas_.back().get();
//   }

//  private:
//   std::unique_ptr<TransactionManager> txn_mgr_;
//   Transaction *txn_{nullptr};
//   std::unique_ptr<DiskManager> disk_manager_;
//   std::unique_ptr<LogManager> log_manager_ = nullptr;
//   std::unique_ptr<LockManager> lock_manager_;
//   std::unique_ptr<BufferPoolManager> bpm_;
//   std::unique_ptr<Catalog> catalog_;
//   std::unique_ptr<ExecutorContext> exec_ctx_;
//   std::unique_ptr<ExecutionEngine> execution_engine_;
//   std::vector<std::unique_ptr<AbstractExpression>> allocated_exprs_;
//   std::vector<std::unique_ptr<Schema>> allocated_output_schemas_;
//   static constexpr uint32_t MAX_VARCHAR_SIZE = 128;
// };

// // --- Helper functions ---
// void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

// void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

// void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

// void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

// void CheckTxnLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {
//   EXPECT_EQ(txn->GetSharedLockSet()->size(), shared_size);
//   EXPECT_EQ(txn->GetExclusiveLockSet()->size(), exclusive_size);
// }

// // NOLINTNEXTLINE
// TEST_F(TransactionTest, SimpleInsertRollbackTest) {
//   // txn1: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
//   // txn1: abort
//   // txn2: SELECT * FROM empty_table2;
//   auto txn1 = GetTxnManager()->Begin();
//   auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(),
//   GetLockManager());
//   // Create Values to insert
//   std::vector<Value> val1{ValueFactory::GetIntegerValue(200), ValueFactory::GetIntegerValue(20)};
//   std::vector<Value> val2{ValueFactory::GetIntegerValue(201), ValueFactory::GetIntegerValue(21)};
//   std::vector<Value> val3{ValueFactory::GetIntegerValue(202), ValueFactory::GetIntegerValue(22)};
//   std::vector<std::vector<Value>> raw_vals{val1, val2, val3};
//   // Create insert plan node
//   auto table_info = exec_ctx1->GetCatalog()->GetTable("empty_table2");
//   InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};

//   GetExecutionEngine()->Execute(&insert_plan, nullptr, txn1, exec_ctx1.get());
//   GetTxnManager()->Abort(txn1);
//   delete txn1;

//   // Iterate through table make sure that values were not inserted.
//   auto txn2 = GetTxnManager()->Begin();
//   auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(),
//   GetLockManager()); auto &schema = table_info->schema_; auto col_a = MakeColumnValueExpression(schema, 0, "colA");
//   auto col_b = MakeColumnValueExpression(schema, 0, "colB");
//   auto out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
//   SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

//   std::vector<Tuple> result_set;
//   GetExecutionEngine()->Execute(&scan_plan, &result_set, txn2, exec_ctx2.get());

//   // Size
//   ASSERT_EQ(result_set.size(), 0);
//   std::vector<RID> rids;

//   GetTxnManager()->Commit(txn2);
//   delete txn2;
// }

// // NOLINTNEXTLINE
// TEST_F(TransactionTest, DirtyReadsTest) {
//   // txn1: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
//   // txn2: SELECT * FROM empty_table2;
//   // txn1: abort
//   auto txn1 = GetTxnManager()->Begin(nullptr, IsolationLevel::READ_UNCOMMITTED);
//   auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(),
//   GetLockManager());
//   // Create Values to insert
//   std::vector<Value> val1{ValueFactory::GetIntegerValue(200), ValueFactory::GetIntegerValue(20)};
//   std::vector<Value> val2{ValueFactory::GetIntegerValue(201), ValueFactory::GetIntegerValue(21)};
//   std::vector<Value> val3{ValueFactory::GetIntegerValue(202), ValueFactory::GetIntegerValue(22)};
//   std::vector<std::vector<Value>> raw_vals{val1, val2, val3};
//   // Create insert plan node
//   auto table_info = exec_ctx1->GetCatalog()->GetTable("empty_table2");
//   InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};

//   GetExecutionEngine()->Execute(&insert_plan, nullptr, txn1, exec_ctx1.get());

//   // Iterate through table to read the tuples.
//   auto txn2 = GetTxnManager()->Begin(nullptr, IsolationLevel::READ_UNCOMMITTED);
//   auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(),
//   GetLockManager()); auto &schema = table_info->schema_; auto col_a = MakeColumnValueExpression(schema, 0, "colA");
//   auto col_b = MakeColumnValueExpression(schema, 0, "colB");
//   auto out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
//   SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

//   std::vector<Tuple> result_set;
//   GetExecutionEngine()->Execute(&scan_plan, &result_set, txn2, exec_ctx2.get());

//   GetTxnManager()->Abort(txn1);
//   delete txn1;

//   // First value
//   ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
//   ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 20);

//   // Second value
//   ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
//   ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 21);

//   // Third value
//   ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 202);
//   ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 22);

//   // Size
//   ASSERT_EQ(result_set.size(), 3);

//   GetTxnManager()->Commit(txn2);
//   delete txn2;
// }

}  // namespace bustub

// /**
//  * lock_manager_test.cpp
//  */

// #include <random>
// #include <thread>  // NOLINT

// #include "common/config.h"
// #include "concurrency/lock_manager.h"
// #include "concurrency/transaction_manager.h"
// #include "gtest/gtest.h"

// namespace bustub {

// /*
//  * This test is only a sanity check. Please do not rely on this test
//  * to check the correctness.
//  */

// // --- Helper functions ---
// void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

// void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

// void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

// void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

// void CheckTxnLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {
//   EXPECT_EQ(txn->GetSharedLockSet()->size(), shared_size);
//   EXPECT_EQ(txn->GetExclusiveLockSet()->size(), exclusive_size);
// }

// // Basic shared lock test under REPEATABLE_READ
// void BasicTest1() {
//   LockManager lock_mgr{};
//   TransactionManager txn_mgr{&lock_mgr};

//   std::vector<RID> rids;
//   std::vector<Transaction *> txns;
//   int num_rids = 10;
//   for (int i = 0; i < num_rids; i++) {
//     RID rid{i, static_cast<uint32_t>(i)};
//     rids.push_back(rid);
//     txns.push_back(txn_mgr.Begin());
//     EXPECT_EQ(i, txns[i]->GetTransactionId());
//   }
//   // test

//   auto task = [&](int txn_id) {
//     bool res;
//     for (const RID &rid : rids) {
//       res = lock_mgr.LockShared(txns[txn_id], rid);
//       EXPECT_TRUE(res);
//       CheckGrowing(txns[txn_id]);
//     }
//     for (const RID &rid : rids) {
//       res = lock_mgr.Unlock(txns[txn_id], rid);
//       EXPECT_TRUE(res);
//       CheckShrinking(txns[txn_id]);
//     }
//     txn_mgr.Commit(txns[txn_id]);
//     CheckCommitted(txns[txn_id]);
//   };
//   std::vector<std::thread> threads;
//   threads.reserve(num_rids);

//   for (int i = 0; i < num_rids; i++) {
//     threads.emplace_back(std::thread{task, i});
//   }

//   for (int i = 0; i < num_rids; i++) {
//     threads[i].join();
//   }

//   for (int i = 0; i < num_rids; i++) {
//     delete txns[i];
//   }
// }
// TEST(LockManagerTest, BasicTest) { BasicTest1(); }

// void TwoPLTest() {
//   LockManager lock_mgr{};
//   TransactionManager txn_mgr{&lock_mgr};
//   RID rid0{0, 0};
//   RID rid1{0, 1};

//   auto txn = txn_mgr.Begin();
//   EXPECT_EQ(0, txn->GetTransactionId());

//   bool res;
//   res = lock_mgr.LockShared(txn, rid0);
//   EXPECT_TRUE(res);
//   CheckGrowing(txn);
//   CheckTxnLockSize(txn, 1, 0);

//   res = lock_mgr.LockExclusive(txn, rid1);
//   EXPECT_TRUE(res);
//   CheckGrowing(txn);
//   CheckTxnLockSize(txn, 1, 1);

//   res = lock_mgr.Unlock(txn, rid0);
//   EXPECT_TRUE(res);
//   CheckShrinking(txn);
//   CheckTxnLockSize(txn, 0, 1);

//   try {
//     lock_mgr.LockShared(txn, rid0);
//     CheckAborted(txn);
//     // Size shouldn't change here
//     CheckTxnLockSize(txn, 0, 1);
//   } catch (TransactionAbortException &e) {
//     // std::cout << e.GetInfo() << std::endl;
//     CheckAborted(txn);
//     // Size shouldn't change here
//     CheckTxnLockSize(txn, 0, 1);
//   }

//   // Need to call txn_mgr's abort
//   txn_mgr.Abort(txn);
//   CheckAborted(txn);
//   CheckTxnLockSize(txn, 0, 0);

//   delete txn;
// }
// TEST(LockManagerTest, TwoPLTest) { TwoPLTest(); }

// void UpgradeTest() {
//   LockManager lock_mgr{};
//   TransactionManager txn_mgr{&lock_mgr};
//   RID rid{0, 0};
//   Transaction txn(0);
//   txn_mgr.Begin(&txn);

//   bool res = lock_mgr.LockShared(&txn, rid);
//   EXPECT_TRUE(res);
//   CheckTxnLockSize(&txn, 1, 0);
//   CheckGrowing(&txn);

//   res = lock_mgr.LockUpgrade(&txn, rid);
//   EXPECT_TRUE(res);
//   CheckTxnLockSize(&txn, 0, 1);
//   CheckGrowing(&txn);

//   res = lock_mgr.Unlock(&txn, rid);
//   EXPECT_TRUE(res);
//   CheckTxnLockSize(&txn, 0, 0);
//   CheckShrinking(&txn);

//   txn_mgr.Commit(&txn);
//   CheckCommitted(&txn);
// }
// TEST(LockManagerTest, UpgradeLockTest) { UpgradeTest(); }

// void WoundWaitBasicTest() {
//   LockManager lock_mgr{};
//   TransactionManager txn_mgr{&lock_mgr};
//   RID rid{0, 0};

//   int id_hold = 0;
//   int id_die = 1;

//   std::promise<void> t1done;
//   std::shared_future<void> t1_future(t1done.get_future());

//   auto wait_die_task = [&]() {
//     // younger transaction acquires lock first
//     Transaction txn_die(id_die);
//     txn_mgr.Begin(&txn_die);
//     bool res = lock_mgr.LockExclusive(&txn_die, rid);
//     EXPECT_TRUE(res);

//     CheckGrowing(&txn_die);
//     CheckTxnLockSize(&txn_die, 0, 1);

//     t1done.set_value();

//     // wait for txn 0 to call lock_exclusive(), which should wound us
//     std::this_thread::sleep_for(std::chrono::milliseconds(300));

//     CheckAborted(&txn_die);

//     // unlock
//     txn_mgr.Abort(&txn_die);
//   };

//   Transaction txn_hold(id_hold);
//   txn_mgr.Begin(&txn_hold);

//   // launch the waiter thread
//   std::thread wait_thread{wait_die_task};

//   // wait for txn1 to lock
//   t1_future.wait();

//   bool res = lock_mgr.LockExclusive(&txn_hold, rid);
//   EXPECT_TRUE(res);

//   wait_thread.join();

//   CheckGrowing(&txn_hold);
//   txn_mgr.Commit(&txn_hold);
//   CheckCommitted(&txn_hold);
// }
// TEST(LockManagerTest, WoundWaitBasicTest) { WoundWaitBasicTest(); }

// }  // namespace bustub

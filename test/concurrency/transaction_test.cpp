//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_test.cpp
//
// Identification: test/concurrency/transaction_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <atomic>
#include <cstdio>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "catalog/table_generator.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/insert_executor.h"
#include "execution/expressions/aggregate_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/nested_index_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "gtest/gtest.h"
#include "test_util.h"  // NOLINT
#include "type/value_factory.h"

#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

namespace bustub {

class TransactionTest : public ::testing::Test {
 public:
  // This function is called before every test.
  void SetUp() override {
    ::testing::Test::SetUp();
    // For each test, we create a new DiskManager, BufferPoolManager, TransactionManager, and Catalog.
    disk_manager_ = std::make_unique<DiskManager>("executor_test.db");
    bpm_ = std::make_unique<BufferPoolManagerInstance>(2560, disk_manager_.get());
    page_id_t page_id;
    bpm_->NewPage(&page_id);
    lock_manager_ = std::make_unique<LockManager>();
    txn_mgr_ = std::make_unique<TransactionManager>(lock_manager_.get(), log_manager_.get());
    catalog_ = std::make_unique<Catalog>(bpm_.get(), lock_manager_.get(), log_manager_.get());
    // Begin a new transaction, along with its executor context.
    txn_ = txn_mgr_->Begin();
    exec_ctx_ =
        std::make_unique<ExecutorContext>(txn_, catalog_.get(), bpm_.get(), txn_mgr_.get(), lock_manager_.get());
    // Generate some test tables.
    TableGenerator gen{exec_ctx_.get()};
    gen.GenerateTestTables();

    execution_engine_ = std::make_unique<ExecutionEngine>(bpm_.get(), txn_mgr_.get(), catalog_.get());
  }

  // This function is called after every test.
  void TearDown() override {
    // Commit our transaction.
    txn_mgr_->Commit(txn_);
    // Shut down the disk manager and clean up the transaction.
    disk_manager_->ShutDown();
    remove("executor_test.db");
    delete txn_;
  };

  /** @return the executor context in our test class */
  auto GetExecutorContext() -> ExecutorContext * { return exec_ctx_.get(); }
  auto GetExecutionEngine() -> ExecutionEngine * { return execution_engine_.get(); }
  auto GetTxn() -> Transaction * { return txn_; }
  auto GetTxnManager() -> TransactionManager * { return txn_mgr_.get(); }
  auto GetCatalog() -> Catalog * { return catalog_.get(); }
  auto GetBPM() -> BufferPoolManager * { return bpm_.get(); }
  auto GetLockManager() -> LockManager * { return lock_manager_.get(); }

  // The below helper functions are useful for testing.

  auto MakeColumnValueExpression(const Schema &schema, uint32_t tuple_idx, const std::string &col_name)
      -> const AbstractExpression * {
    uint32_t col_idx = schema.GetColIdx(col_name);
    auto col_type = schema.GetColumn(col_idx).GetType();
    allocated_exprs_.emplace_back(std::make_unique<ColumnValueExpression>(tuple_idx, col_idx, col_type));
    return allocated_exprs_.back().get();
  }

  auto MakeConstantValueExpression(const Value &val) -> const AbstractExpression * {
    allocated_exprs_.emplace_back(std::make_unique<ConstantValueExpression>(val));
    return allocated_exprs_.back().get();
  }

  auto MakeComparisonExpression(const AbstractExpression *lhs, const AbstractExpression *rhs, ComparisonType comp_type)
      -> const AbstractExpression * {
    allocated_exprs_.emplace_back(std::make_unique<ComparisonExpression>(lhs, rhs, comp_type));
    return allocated_exprs_.back().get();
  }

  auto MakeAggregateValueExpression(bool is_group_by_term, uint32_t term_idx) -> const AbstractExpression * {
    allocated_exprs_.emplace_back(
        std::make_unique<AggregateValueExpression>(is_group_by_term, term_idx, TypeId::INTEGER));
    return allocated_exprs_.back().get();
  }

  auto MakeOutputSchema(const std::vector<std::pair<std::string, const AbstractExpression *>> &exprs)
      -> const Schema * {
    std::vector<Column> cols;
    cols.reserve(exprs.size());
    for (const auto &input : exprs) {
      if (input.second->GetReturnType() != TypeId::VARCHAR) {
        cols.emplace_back(input.first, input.second->GetReturnType(), input.second);
      } else {
        cols.emplace_back(input.first, input.second->GetReturnType(), MAX_VARCHAR_SIZE, input.second);
      }
    }
    allocated_output_schemas_.emplace_back(std::make_unique<Schema>(cols));
    return allocated_output_schemas_.back().get();
  }

 private:
  std::unique_ptr<TransactionManager> txn_mgr_;
  Transaction *txn_{nullptr};
  std::unique_ptr<DiskManager> disk_manager_;
  std::unique_ptr<LogManager> log_manager_ = nullptr;
  std::unique_ptr<LockManager> lock_manager_;
  std::unique_ptr<BufferPoolManager> bpm_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<ExecutorContext> exec_ctx_;
  std::unique_ptr<ExecutionEngine> execution_engine_;
  std::vector<std::unique_ptr<AbstractExpression>> allocated_exprs_;
  std::vector<std::unique_ptr<Schema>> allocated_output_schemas_;
  static constexpr uint32_t MAX_VARCHAR_SIZE = 128;
};

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ(txn->GetSharedLockSet()->size(), shared_size);
  EXPECT_EQ(txn->GetExclusiveLockSet()->size(), exclusive_size);
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, SimpleInsertRollbackTest) {
  // txn1: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
  // txn1: abort
  // txn2: SELECT * FROM empty_table2;
  auto txn1 = GetTxnManager()->Begin();
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(200), ValueFactory::GetIntegerValue(20)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(201), ValueFactory::GetIntegerValue(21)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(202), ValueFactory::GetIntegerValue(22)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};
  // Create insert plan node
  auto table_info = exec_ctx1->GetCatalog()->GetTable("empty_table2");
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};

  GetExecutionEngine()->Execute(&insert_plan, nullptr, txn1, exec_ctx1.get());
  GetTxnManager()->Abort(txn1);
  delete txn1;

  // Iterate through table make sure that values were not inserted.
  auto txn2 = GetTxnManager()->Begin();
  auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  auto &schema = table_info->schema_;
  auto col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto col_b = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn2, exec_ctx2.get());

  // Size
  ASSERT_EQ(result_set.size(), 0);
  std::vector<RID> rids;

  GetTxnManager()->Commit(txn2);
  delete txn2;
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, DirtyReadsTest) {
  // txn1: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
  // txn2: SELECT * FROM empty_table2;
  // txn1: abort
  auto txn1 = GetTxnManager()->Begin(nullptr, IsolationLevel::READ_UNCOMMITTED);
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(200), ValueFactory::GetIntegerValue(20)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(201), ValueFactory::GetIntegerValue(21)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(202), ValueFactory::GetIntegerValue(22)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};
  // Create insert plan node
  auto table_info = exec_ctx1->GetCatalog()->GetTable("empty_table2");
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};

  GetExecutionEngine()->Execute(&insert_plan, nullptr, txn1, exec_ctx1.get());

  // Iterate through table to read the tuples.
  auto txn2 = GetTxnManager()->Begin(nullptr, IsolationLevel::READ_UNCOMMITTED);
  auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  auto &schema = table_info->schema_;
  auto col_a = MakeColumnValueExpression(schema, 0, "colA");
  auto col_b = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", col_a}, {"colB", col_b}});
  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn2, exec_ctx2.get());

  GetTxnManager()->Abort(txn1);
  delete txn1;

  // First value
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 20);

  // Second value
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 21);

  // Third value
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 202);
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 22);

  // Size
  ASSERT_EQ(result_set.size(), 3);

  GetTxnManager()->Commit(txn2);
  delete txn2;
}

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

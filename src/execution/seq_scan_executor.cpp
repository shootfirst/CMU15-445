//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "execution/expressions/comparison_expression.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_iterator_(
          exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction())),
      end_iterator_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End()) {}

void SeqScanExecutor::Init() {
  auto transaction = exec_ctx_->GetTransaction();
  table_iterator_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(transaction);
  end_iterator_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Schema schema = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_;
  const Schema *schema_output = plan_->OutputSchema();

  uint32_t out_cnt = schema_output->GetColumnCount();
  auto txn = exec_ctx_->GetTransaction();
  while (table_iterator_ != end_iterator_) {
    Tuple search_tuple = *table_iterator_;

    // fetch sharedlock
    RID d = search_tuple.GetRid();
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!txn->IsSharedLocked(d) && !txn->IsExclusiveLocked(d)) {
        exec_ctx_->GetLockManager()->LockShared(txn, d);
      }
    }

    table_iterator_++;
    auto predicate = plan_->GetPredicate();
    if (predicate == nullptr ||
        dynamic_cast<const ComparisonExpression *>(predicate)->Evaluate(&search_tuple, &schema).GetAs<bool>()) {
      // finish reading , unlock
      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && txn->IsSharedLocked(d)) {
        exec_ctx_->GetLockManager()->Unlock(txn, d);
      }

      std::vector<Value> values;
      values.reserve(out_cnt);
      for (uint32_t i = 0; i < out_cnt; i++) {
        values.push_back(schema_output->GetColumn(i).GetExpr()->Evaluate(&search_tuple, &schema));
      }
      Tuple res = Tuple(values, schema_output);
      *tuple = res;
      *rid = search_tuple.GetRid();
      return true;
    }

    // finish reading , unlock
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && txn->IsSharedLocked(d)) {
      exec_ctx_->GetLockManager()->Unlock(txn, d);
    }
  }
  return false;
}

}  // namespace bustub

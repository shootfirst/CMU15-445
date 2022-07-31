//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());

  Transaction *txn = exec_ctx_->GetTransaction();
  while (child_executor_->Next(tuple, rid)) {
    // fetch exclusivelock
    if (txn->IsSharedLocked(*rid)) {
      exec_ctx_->GetLockManager()->LockUpgrade(txn, *rid);
    }
    if (!txn->IsExclusiveLocked(*rid)) {
      exec_ctx_->GetLockManager()->LockExclusive(txn, *rid);
    }
    // delete in table
    table_info->table_->MarkDelete(*rid, txn);

    // update index
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    size_t size = indexes.size();

    for (size_t i = 0; i < size; i++) {
      auto key_tuple =
          tuple->KeyFromTuple(table_info->schema_, indexes[i]->key_schema_, indexes[i]->index_->GetKeyAttrs());
      indexes[i]->index_->DeleteEntry(key_tuple, *rid, txn);

      IndexWriteRecord rcd = IndexWriteRecord(*rid, plan_->TableOid(), WType::DELETE, {}, key_tuple,
                                              indexes[i]->index_oid_, exec_ctx_->GetCatalog());
      txn->GetIndexWriteSet()->emplace_back(rcd);
    }
  }
  return false;
}

}  // namespace bustub

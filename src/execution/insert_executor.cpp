//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "storage/index/extendible_hash_table_index.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());

  Transaction *txn = exec_ctx_->GetTransaction();

  Tuple insert_tuple;

  if (plan_->IsRawInsert()) {
    const std::vector<std::vector<Value>> raw_values = plan_->RawValues();
    size_t size = raw_values.size();

    for (size_t i = 0; i < size; i++) {
      // insert to table
      *tuple = Tuple(raw_values[i], &table_info->schema_);

      table_info->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());

      // fetch exclusivelock
      exec_ctx_->GetLockManager()->LockExclusive(txn, *rid);

      // update index
      auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
      size_t size = indexes.size();

      for (size_t i = 0; i < size; i++) {
        auto key_tuple =
            tuple->KeyFromTuple(table_info->schema_, indexes[i]->key_schema_, indexes[i]->index_->GetKeyAttrs());
        indexes[i]->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());

        IndexWriteRecord rcd = IndexWriteRecord(*rid, plan_->TableOid(), WType::INSERT, key_tuple, {},
                                                indexes[i]->index_oid_, exec_ctx_->GetCatalog());
        txn->GetIndexWriteSet()->emplace_back(rcd);
      }
    }

  } else {
    while (child_executor_->Next(tuple, rid)) {
      // insert to table
      table_info->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());

      // fetch exclusivelock
      exec_ctx_->GetLockManager()->LockExclusive(txn, *rid);

      // update index
      auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
      size_t size = indexes.size();

      for (size_t i = 0; i < size; i++) {
        auto key_tuple =
            tuple->KeyFromTuple(table_info->schema_, indexes[i]->key_schema_, indexes[i]->index_->GetKeyAttrs());
        indexes[i]->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());

        IndexWriteRecord rcd = IndexWriteRecord(*rid, plan_->TableOid(), WType::INSERT, key_tuple, {},
                                                indexes[i]->index_oid_, exec_ctx_->GetCatalog());
        txn->GetIndexWriteSet()->emplace_back(rcd);
      }
    }
  }

  return false;
}

}  // namespace bustub

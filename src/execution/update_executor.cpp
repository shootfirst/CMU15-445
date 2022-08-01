//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Transaction *txn = exec_ctx_->GetTransaction();

  while (child_executor_->Next(tuple, rid)) {
    // fetch exclusivelock
    if (txn->IsSharedLocked(*rid)) {
      exec_ctx_->GetLockManager()->LockUpgrade(txn, *rid);
    }
    if (!txn->IsExclusiveLocked(*rid)) {
      exec_ctx_->GetLockManager()->LockExclusive(txn, *rid);
    }
    // update table
    auto new_tuple = GenerateUpdatedTuple(*tuple);
    table_info_->table_->UpdateTuple(new_tuple, *rid, txn);

    // update index
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    size_t size = indexes.size();

    for (size_t i = 0; i < size; i++) {
      auto key_tuple =
          tuple->KeyFromTuple(table_info_->schema_, indexes[i]->key_schema_, indexes[i]->index_->GetKeyAttrs());
      indexes[i]->index_->DeleteEntry(key_tuple, *rid, txn);
      auto new_key_tuple =
          new_tuple.KeyFromTuple(table_info_->schema_, indexes[i]->key_schema_, indexes[i]->index_->GetKeyAttrs());
      indexes[i]->index_->InsertEntry(new_key_tuple, *rid, txn);

      IndexWriteRecord rcd = IndexWriteRecord(*rid, plan_->TableOid(), WType::DELETE, new_tuple, *tuple,
                                              indexes[i]->index_oid_, exec_ctx_->GetCatalog());
      txn->GetIndexWriteSet()->emplace_back(rcd);
    }
  }
  return false;
}

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub

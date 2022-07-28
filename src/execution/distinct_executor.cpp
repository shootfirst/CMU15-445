//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  key_dic_.clear();

  Tuple tuple;
  RID rid;

  while (child_executor_->Next(&tuple, &rid)) {
    auto key = GetDistinctKey(&tuple, child_executor_->GetOutputSchema());
    auto value = GetDistinctValue(&tuple, child_executor_->GetOutputSchema());
    key_dic_[key] = value;
  }
  it_ = key_dic_.begin();
}

auto DistinctExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ != key_dic_.end()) {
    *tuple = Tuple(it_->second.sistinct_, GetOutputSchema());
    *rid = tuple->GetRid();
    it_++;
    return true;
  }
  return false;
}

}  // namespace bustub

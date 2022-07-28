//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/comparison_expression.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

// void NestedLoopJoinExecutor::Init() {
//   left_executor_->Init();
//   right_executor_->Init();
//   left_executor_->Next(&left_, &left_rid_);
// }

// auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   const Schema *left_schema = left_executor_->GetOutputSchema();
//   const Schema *right_schema = right_executor_->GetOutputSchema();

//   uint32_t out_cnt = GetOutputSchema()->GetColumnCount();
//   while (true) {
//     if (!right_executor_->Next(&right_, &right_rid_)) {
//       if (!left_executor_->Next(&left_, &left_rid_)) {
//         break;
//       }
//       right_executor_->Init();
//       right_executor_->Next(&right_, &right_rid_);
//     }

//     auto predicate = plan_->Predicate();
//     if (predicate == nullptr || dynamic_cast<const ComparisonExpression *>(predicate)
//                                     ->EvaluateJoin(&left_, left_schema, &right_, right_schema)
//                                     .GetAs<bool>()) {
//       std::vector<Value> values;
//       values.reserve(out_cnt);
//       for (uint32_t i = 0; i < out_cnt; i++) {
//         values.push_back(
//             GetOutputSchema()->GetColumn(i).GetExpr()->EvaluateJoin(&left_, left_schema, &right_, right_schema));
//       }

//       Tuple res = Tuple(values, GetOutputSchema());
//       *tuple = res;
//       return true;
//     }
//   }
//   return false;
// }

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  const Schema *left_schema = left_executor_->GetOutputSchema();
  const Schema *right_schema = right_executor_->GetOutputSchema();
  auto predicate = plan_->Predicate();

  try {
    Tuple ltuple;
    Tuple rtuple;
    RID tmprid;
    while (left_executor_->Next(&ltuple, &tmprid)) {
      right_executor_->Init();
      while (right_executor_->Next(&rtuple, &tmprid)) {
        std::vector<Value> vals;
        if (predicate == nullptr || dynamic_cast<const ComparisonExpression *>(predicate)
                                        ->EvaluateJoin(&ltuple, left_schema, &rtuple, right_schema)
                                        .GetAs<bool>()) {
          for (const auto &col : plan_->OutputSchema()->GetColumns()) {
            vals.emplace_back(col.GetExpr()->EvaluateJoin(&ltuple, left_schema, &rtuple, right_schema));
          }
          ret_.emplace_back(Tuple(vals, GetOutputSchema()));
        }
      }
    }
    it_ = ret_.begin();
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == ret_.end()) {
    return false;
  }
  *tuple = *it_;
  *rid = tuple->GetRid();
  ++it_;
  return true;
}

}  // namespace bustub

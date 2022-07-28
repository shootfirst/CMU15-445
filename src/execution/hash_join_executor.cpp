//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

// void HashJoinExecutor::Init() {
//   left_child_->Init();
//   right_child_->Init();
//   hash_hash_map_.clear();

//   Tuple tuple;
//   RID rid;
//   while (left_child_->Next(&tuple, &rid)) {
//     auto val = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, left_child_->GetOutputSchema());
//     HashJoinKey value{val};
//     hash_hash_map_[value].push_back(tuple);
//   }

//   Tuple right;

//   while (right_child_->Next(&right, &rid)) {
//     auto val = plan_->RightJoinKeyExpression()->Evaluate(&right, right_child_->GetOutputSchema());
//     HashJoinKey value{val};
//     if (hash_hash_map_.find(value) != hash_hash_map_.end()) {
//       tuple_vec_ = hash_hash_map_[value];

//       uint32_t out_cnt = GetOutputSchema()->GetColumnCount();
//       std::vector<Value> values;
//       values.reserve(out_cnt);
//       for (const auto &left : tuple_vec_) {
//         for (uint32_t i = 0; i < out_cnt; i++) {
//           values.push_back(GetOutputSchema()->GetColumn(i).GetExpr()->EvaluateJoin(
//               &left, left_child_->GetOutputSchema(), &right, right_child_->GetOutputSchema()));
//         }
//       }

//       Tuple res = Tuple(values, GetOutputSchema());
//       ret_.push_back(res);
//     }
//   }
//   i_ = 0;
// }

// auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   if (i_ >= ret_.size()) {
//     return false;
//   }

//   *tuple = ret_[i_];
//   *rid = tuple->GetRid();
//   i_++;
//   return true;
// }

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  hash_map_.clear();
  try {
    Tuple tmptuple;
    RID tmprid;
    while (left_child_->Next(&tmptuple, &tmprid)) {
      HashJoinKey key;
      key.val_ = plan_->LeftJoinKeyExpression()->Evaluate(&tmptuple, left_child_->GetOutputSchema());
      if (hash_map_.find(key) != hash_map_.end()) {
        hash_map_[key].emplace_back(tmptuple);
      } else {
        hash_map_[key] = std::vector{tmptuple};
      }
    }
    while (right_child_->Next(&tmptuple, &tmprid)) {
      HashJoinKey key;
      key.val_ = plan_->RightJoinKeyExpression()->Evaluate(&tmptuple, right_child_->GetOutputSchema());
      if (hash_map_.find(key) != hash_map_.end()) {
        for (const auto &ltuple : hash_map_[key]) {
          std::vector<Value> vals;
          for (const auto &col : plan_->OutputSchema()->GetColumns()) {
            vals.emplace_back(col.GetExpr()->EvaluateJoin(&ltuple, left_child_->GetOutputSchema(), &tmptuple,
                                                          right_child_->GetOutputSchema()));
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

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == ret_.end()) {
    return false;
  }
  *tuple = *it_;
  *rid = tuple->GetRid();
  ++it_;
  return true;
}

}  // namespace bustub

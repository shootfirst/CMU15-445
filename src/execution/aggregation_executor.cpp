//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan_->GetAggregates(), plan_->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

// void AggregationExecutor::Init() {
//   RID rid;
//   Tuple tuple;

//   std::vector<Value> keys;
//   std::vector<Value> values;

//   auto group_by = plan_->GetGroupBys();
//   auto aggregates = plan_->GetAggregates();
//   while (child_->Next(&tuple, &rid)) {
//     keys.clear();
//     if (!group_by.empty()) {
//       for (auto &group : group_by) {
//         keys.push_back(group->Evaluate(&tuple, child_->GetOutputSchema()));
//       }
//     }
//     values.clear();
//     for (auto &aggregate : aggregates) {
//       values.push_back(aggregate->Evaluate(&tuple, child_->GetOutputSchema()));
//     }
//     aht_.InsertCombine(AggregateKey{keys}, AggregateValue{values});
//   }
//   aht_iterator_ = aht_.Begin();
// }

// auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   while (aht_iterator_ != aht_.End()) {
//     if (plan_->GetHaving() == nullptr ||
//         plan_->GetHaving()
//             ->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_)
//             .GetAs<bool>()) {
//       uint32_t out_cnt = GetOutputSchema()->GetColumnCount();
//       std::vector<Value> values;
//       values.reserve(out_cnt);
//       for (uint32_t i = 0; i < out_cnt; i++) {
//         values.push_back(GetOutputSchema()->GetColumn(i).GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_,
//                                                                                       aht_iterator_.Val().aggregates_));
//       }

//       *tuple = Tuple(values, GetOutputSchema());
//       *rid = tuple->GetRid();
//       ++aht_iterator_;
//       return true;
//     }
//     ++aht_iterator_;
//   }
//   return false;
// }

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tmptuple;
  RID tmprid;
  try {
    while (child_->Next(&tmptuple, &tmprid)) {
      aht_.InsertCombine(MakeAggregateKey(&tmptuple), MakeAggregateValue(&tmptuple));
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  const AggregateKey &key = aht_iterator_.Key();
  const AggregateValue &val = aht_iterator_.Val();
  ++aht_iterator_;
  if (plan_->GetHaving() == nullptr ||
      plan_->GetHaving()->EvaluateAggregate(key.group_bys_, val.aggregates_).GetAs<bool>()) {
    std::vector<Value> ret;
    for (const auto &col : plan_->OutputSchema()->GetColumns()) {
      ret.emplace_back(col.GetExpr()->EvaluateAggregate(key.group_bys_, val.aggregates_));
    }
    *tuple = Tuple(ret, plan_->OutputSchema());
    *rid = tuple->GetRid();
    return true;
  }
  return Next(tuple, rid);
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub

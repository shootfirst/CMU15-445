//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

struct DistinctKey {
  /** The group-by values */
  std::vector<Value> distin_bys_;

  /**
   * Compares two aggregate keys for equality.
   * @param other the other aggregate key to be compared with
   * @return `true` if both aggregate keys have equivalent group-by expressions, `false` otherwise
   */
  bool operator==(const DistinctKey &other) const {
    for (uint32_t i = 0; i < other.distin_bys_.size(); i++) {
      if (distin_bys_[i].CompareEquals(other.distin_bys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/** AggregateValue represents a value for each of the running aggregates */
struct DistinctValue {
  /** The aggregate values */
  std::vector<Value> sistinct_;
};

}  // namespace bustub

namespace std {
/** Implements std::hash on DistinctKey */
template <>
struct hash<bustub::DistinctKey> {
  std::size_t operator()(const bustub::DistinctKey &agg_key) const {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.distin_bys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the distinct */
  auto GetOutputSchema() -> const Schema * override { return plan_->OutputSchema(); };

 private:
  DistinctKey GetDistinctKey(const Tuple *tuple, const Schema *schema) {
    std::vector<Value> vals;
    std::size_t columns_size = schema->GetColumnCount();
    vals.reserve(columns_size);

    for (uint32_t i = 0; i < columns_size; i++) {
      vals.emplace_back(tuple->GetValue(schema, i));
    }
    return {vals};
  }

  DistinctValue GetDistinctValue(const Tuple *tuple, const Schema *schema) {
    std::vector<Value> vals;
    std::size_t columns_size = schema->GetColumnCount();
    vals.reserve(columns_size);

    for (uint32_t i = 0; i < columns_size; i++) {
      vals.emplace_back(tuple->GetValue(schema, i));
    }
    return {vals};
  }

  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::unordered_map<DistinctKey, DistinctValue> key_dic_{};
  std::unordered_map<DistinctKey, DistinctValue>::iterator it_;
};
}  // namespace bustub

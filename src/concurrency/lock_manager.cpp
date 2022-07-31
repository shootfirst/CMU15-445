//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING || txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end() &&
      txn->GetExclusiveLockSet()->find(rid) != txn->GetExclusiveLockSet()->end()) {
    return true;
  }

  // LOG_DEBUG("%d want to get share %d",(int)txn->GetTransactionId(), (int)rid.GetSlotNum());
  std::unique_lock<std::mutex> u_latch(latch_);
  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }

  lock_table_[rid].request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED));

  auto judge_func = [&]() {
    bool is_kill = false;
    bool ret = true;
    auto it = lock_table_[rid].request_queue_.begin();
    while (it != lock_table_[rid].request_queue_.end()) {
      if (it->txn_id_ == txn->GetTransactionId()) {
        if (is_kill) {
          lock_table_[rid].cv_.notify_all();
        }
        it->granted_ = ret;
        return ret;
      }
      if (it->txn_id_ > txn->GetTransactionId() && it->lock_mode_ == LockMode::EXCLUSIVE) {
        Transaction *tst = TransactionManager::GetTransaction(it->txn_id_);
        tst->SetState(TransactionState::ABORTED);
        tst->GetExclusiveLockSet()->erase(rid);
        tst->GetSharedLockSet()->erase(rid);
        it = lock_table_[rid].request_queue_.erase(it);
        is_kill = true;

      } else if (it->lock_mode_ == LockMode::SHARED) {
        it++;

      } else {
        it++;
        ret = false;
      }
    }
    return true;
  };

  while (!judge_func()) {
    if (txn->GetState() != TransactionState::ABORTED) {
      break;
    }
    lock_table_[rid].cv_.wait(u_latch);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED ||
      txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->GetExclusiveLockSet()->find(rid) != txn->GetExclusiveLockSet()->end()) {
    return true;
  }

  // LOG_DEBUG("%d want to get exclusive %d",(int)txn->GetTransactionId(), (int)rid.GetSlotNum());
  std::unique_lock<std::mutex> u_latch(latch_);
  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }

  lock_table_[rid].request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));

  auto judge_func = [&]() {
    auto it = lock_table_[rid].request_queue_.begin();
    // LOG_DEBUG("%d want to get exclusive %d",(int)txn->GetTransactionId(), (int)rid.GetSlotNum());
    while (it != lock_table_[rid].request_queue_.end()) {
      if (it->txn_id_ == txn->GetTransactionId()) {
        it->granted_ = true;
        return true;
      }
      // kill younger
      if (it->txn_id_ > txn->GetTransactionId()) {
        Transaction *tst = TransactionManager::GetTransaction(it->txn_id_);
        tst->SetState(TransactionState::ABORTED);
        tst->GetExclusiveLockSet()->erase(rid);
        tst->GetSharedLockSet()->erase(rid);
        // LOG_DEBUG("%d kill %d len %d",(int)txn->GetTransactionId(), (int)it->txn_id_, (int)lst.size());
        it = lock_table_[rid].request_queue_.erase(it);
        lock_table_[rid].cv_.notify_all();
      } else {
        return false;
      }
    }
    return true;
  };

  while (!judge_func()) {
    if (txn->GetState() == TransactionState::ABORTED) {
      break;
    }
    lock_table_[rid].cv_.wait(u_latch);
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED ||
      txn->GetSharedLockSet()->find(rid) == txn->GetSharedLockSet()->end() ||
      txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED ||
      txn->GetExclusiveLockSet()->find(rid) != txn->GetExclusiveLockSet()->end()) {
    return false;
  }

  std::unique_lock<std::mutex> u_latch(latch_);
  if (lock_table_.find(rid) == lock_table_.end()) {
    LOG_DEBUG("can not find in queue");
    return false;
  }

  // This should also abort the transaction and return false if another transaction is already waiting to upgrade their
  // lock.
  if (lock_table_[rid].upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  lock_table_[rid].upgrading_ = txn->GetTransactionId();

  auto it = lock_table_[rid].request_queue_.begin();
  for (; it != lock_table_[rid].request_queue_.end(); it++) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }
  lock_table_[rid].request_queue_.erase(it);
  txn->GetSharedLockSet()->erase(rid);

  lock_table_[rid].cv_.notify_all();

  lock_table_[rid].request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));

  auto judge_func = [&]() {
    auto it = lock_table_[rid].request_queue_.begin();
    while (it != lock_table_[rid].request_queue_.end()) {
      if (it->txn_id_ == txn->GetTransactionId()) {
        it->granted_ = true;
        return true;
      }
      // kill younger
      if (it->txn_id_ > txn->GetTransactionId()) {
        Transaction *tst = TransactionManager::GetTransaction(it->txn_id_);
        tst->SetState(TransactionState::ABORTED);
        tst->GetExclusiveLockSet()->erase(rid);
        tst->GetSharedLockSet()->erase(rid);
        // LOG_DEBUG("%d kill %d",(int)txn->GetTransactionId(), (int)it->txn_id_);
        it = lock_table_[rid].request_queue_.erase(it);
        lock_table_[rid].cv_.notify_all();
      } else {
        return false;
      }
    }
    return true;
  };

  while (!judge_func()) {
    if (txn->GetState() == TransactionState::ABORTED) {
      break;
    }
    lock_table_[rid].cv_.wait(u_latch);
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  lock_table_[rid].upgrading_ = INVALID_TXN_ID;

  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> u_latch(latch_);
  if (lock_table_.find(rid) == lock_table_.end()) {
    LOG_DEBUG("can not find in queue");
    return false;
  }

  auto it = lock_table_[rid].request_queue_.begin();
  for (; it != lock_table_[rid].request_queue_.end(); it++) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      break;
    }
  }
  if (it == lock_table_[rid].request_queue_.end()) {
    return false;
  }
  lock_table_[rid].request_queue_.erase(it);
  lock_table_[rid].cv_.notify_all();
  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->IsSharedLocked(rid) && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::SHRINKING);
    } else if (txn->IsExclusiveLocked(rid)) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub

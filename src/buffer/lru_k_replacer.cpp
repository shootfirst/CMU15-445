//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

LRUKReplacer::~LRUKReplacer() {
  for (auto list_it : first_time_) {
    delete list_it;
  }
  for (auto tree_it : k_time_) {
    delete tree_it.second;
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lck(latch_);

  // When multipe frames have +inf backward k-distance, the replacer evicts the frame with the earliest timestamp.
  for (auto list_it = first_time_.begin(); list_it != first_time_.end(); list_it++) {
    LruKInfo *info = *list_it;
    if (info->evictable_) {
      *frame_id = info->frame_id_;
      first_time_.erase(list_it);
      list_map_.erase(list_map_.find(*frame_id));
      //-------------------------------------------------------------
      delete info;
      curr_size_--;
      //-------------------------------------------------------------
      all_cnt_--;
      return true;
    }
  }

  // Then we find in k_time_.
  for (auto tree_it = k_time_.begin(); tree_it != k_time_.end(); tree_it++) {
    LruKInfo *info = tree_it->second;
    if (info->evictable_) {
      *frame_id = info->frame_id_;
      k_time_.erase(tree_it);
      tree_map_.erase(tree_map_.find(*frame_id));
      //-------------------------------------------------------------
      delete info;
      curr_size_--;
      //-------------------------------------------------------------
      all_cnt_--;
      return true;
    }
  }

  // LOG_INFO("Evict:: no evictable page!\n");
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(latch_);

  // first we find in first_time_
  auto list_map_it = list_map_.find(frame_id);
  if (list_map_it != nullptr) {
    auto list_it = list_map_it->second;
    LruKInfo *info = (*list_it);
    info->timestap_list_.push_back(current_timestamp_++);

    if (info->timestap_list_.size() == k_) {
      // delete from first_time_
      first_time_.erase(list_it);
      list_map_.erase(list_map_it);

      // add into k_time_
      size_t last_kth_timestap = info->timestap_list_.front();
      k_time_.insert({last_kth_timestap, info});
      tree_map_.insert({frame_id, k_time_.find(last_kth_timestap)});
      return;
    }
  }

  // Then we find in first_time_
  auto tree_map_it = tree_map_.find(frame_id);
  if (tree_map_it != nullptr) {
    LruKInfo *info = tree_map_it->second->second;
    // delete old last kth timestap
    info->timestap_list_.pop_front();
    // add new visited time_stap
    info->timestap_list_.push_back(current_timestamp_++);

    // we need update last kth timestap, so we firwst delete it, and add it with new last kth timestap
    // (这里顺序不能倒换)
    k_time_.erase(tree_map_it->second);
    tree_map_.erase(tree_map_it);

    size_t new_last_kth_timestap = info->timestap_list_.front();
    k_time_.insert({new_last_kth_timestap, info});
    tree_map_.insert({frame_id, k_time_.find(new_last_kth_timestap)});
    return;
  }

  // if do not exist, we create new info and add it into first_time
  if (replacer_size_ == all_cnt_) {
    LOG_INFO("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n");
    return;
  }
  //-------------------------------------------------------------
  auto new_info = new LruKInfo(frame_id, false);
  //-------------------------------------------------------------
  all_cnt_++;
  new_info->timestap_list_.push_back(current_timestamp_++);

  first_time_.push_back(new_info);
  list_map_[frame_id] = --first_time_.end();

  // auto good = list_map_.find(frame_id)->second;
  // assert(good == --first_time_.end());
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lck(latch_);
  LruKInfo *info;

  // First find in first_time_
  auto lits_map_it = list_map_.find(frame_id);
  if (lits_map_it != nullptr) {
    auto list_it = lits_map_it->second;
    info = *list_it;
    if (info->evictable_ && !set_evictable) {
      //-------------------------------------------------------------
      curr_size_--;
      //-------------------------------------------------------------
      info->evictable_ = set_evictable;
    } else if (!info->evictable_ && set_evictable) {
      //-------------------------------------------------------------
      curr_size_++;
      //-------------------------------------------------------------
      info->evictable_ = set_evictable;
    }
    return;
  }

  // Then find in k_time_
  auto tree_map_it = tree_map_.find(frame_id);
  if (tree_map_it != nullptr) {
    auto tree_it = tree_map_it->second;
    info = tree_it->second;
    if (info->evictable_ && !set_evictable) {
      //-------------------------------------------------------------
      curr_size_--;
      //-------------------------------------------------------------
      info->evictable_ = set_evictable;
    } else if (!info->evictable_ && set_evictable) {
      //-------------------------------------------------------------
      curr_size_++;
      //-------------------------------------------------------------
      info->evictable_ = set_evictable;
    }
    return;
  }

  // LOG_INFO("SetEvictable:: frameid %d does not exist!\n", frame_id);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(latch_);

  LruKInfo *info;

  // First find in first_time_
  auto lits_map_it = list_map_.find(frame_id);
  if (lits_map_it != nullptr) {
    auto list_it = lits_map_it->second;
    info = *list_it;
    //-------------------------------------------------------------
    if (info->evictable_) {
      curr_size_--;
    }
    delete info;
    //-------------------------------------------------------------
    all_cnt_--;
    list_map_.erase(lits_map_it);
    first_time_.erase(list_it);
    return;
  }

  // Then find in k_time_
  auto tree_map_it = tree_map_.find(frame_id);
  if (tree_map_it != nullptr) {
    auto tree_it = tree_map_it->second;
    info = tree_it->second;
    //-------------------------------------------------------------
    if (info->evictable_) {
      curr_size_--;
    }
    delete info;
    //-------------------------------------------------------------
    all_cnt_--;
    tree_map_.erase(tree_map_it);
    k_time_.erase(tree_it);
    return;
  }

  // LOG_INFO("Remove:: frameid %d does not exist!\n", frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lck(latch_);
  return curr_size_;
}

}  // namespace bustub

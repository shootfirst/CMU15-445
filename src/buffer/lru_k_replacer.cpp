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

// LRUKReplacer::~LRUKReplacer() {
//   for (auto list_it : list_) {
//     delete list_it;
//   }
//   for (auto tree_it : tree_) {
//     delete tree_it.second;
//   }
// }

// auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
//   std::lock_guard<std::mutex> lck(latch_);

//   // When multipe frames have +inf backward k-distance, the replacer evicts the frame with the earliest timestamp.
//   for (auto list_it = list_.begin(); list_it != list_.end(); list_it++) {
//     LruKInfo *info = *list_it;
//     if (info->evictable_) {
//       *frame_id = info->frame_id_;
//       list_.erase(list_it);
//       list_map_.erase(list_map_.find(*frame_id));
//       delete info;
//       curr_size_--;
//       all_cnt_--;
//       return true;
//     }
//   }

//   // Then we find in tree_.
//   for (auto tree_it = tree_.begin(); tree_it != tree_.end(); tree_it++) {
//     LruKInfo *info = tree_it->second;
//     if (info->evictable_) {
//       *frame_id = info->frame_id_;
//       tree_.erase(tree_it);
//       tree_map_.erase(tree_map_.find(*frame_id));
//       delete info;
//       curr_size_--;
//       all_cnt_--;
//       return true;
//     }
//   }

//   // LOG_INFO("Evict:: no evictable page!\n");
//   return false;
// }

// void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
//   std::lock_guard<std::mutex> lck(latch_);

//   // first we find in list_
//   auto list_map_it = list_map_.find(frame_id);
//   if (list_map_it != nullptr) {
//     auto list_it = list_map_it->second;
//     LruKInfo *info = (*list_it);
//     info->timestap_list_.push_back(current_timestamp_);
//     current_timestamp_++;

//     if (info->timestap_list_.size() == k_) {
//       // delete from list_
//       list_.erase(list_it);
//       list_map_.erase(list_map_it);

//       // add into tree_
//       size_t last_kth_timestap = info->timestap_list_.front();
//       tree_.insert({last_kth_timestap, info});
//       tree_map_.insert({frame_id, tree_.find(last_kth_timestap)});
//       return;
//     }
//   }

//   // Then we find in list_
//   auto tree_map_it = tree_map_.find(frame_id);
//   if (tree_map_it != nullptr) {
//     LruKInfo *info = tree_map_it->second->second;
//     // delete old last kth timestap
//     info->timestap_list_.pop_front();
//     // add new visited time_stap
//     info->timestap_list_.push_back(current_timestamp_);
//     current_timestamp_++;

//     // we need update last kth timestap, so we firwst delete it, and add it with new last kth timestap
//     // (这里顺序不能倒换)
//     tree_.erase(tree_map_it->second);
//     tree_map_.erase(tree_map_it);

//     size_t new_last_kth_timestap = info->timestap_list_.front();
//     tree_.insert({new_last_kth_timestap, info});
//     tree_map_.insert({frame_id, tree_.find(new_last_kth_timestap)});
//     return;
//   }

//   // if do not exist, we create new info and add it into first_time
//   if (replacer_size_ == all_cnt_) {
//     return;
//   }

//   auto new_info = new LruKInfo(frame_id, false);
//   all_cnt_++;

//   new_info->timestap_list_.push_back(current_timestamp_);
//   current_timestamp_++;

//   list_.push_back(new_info);
//   list_map_[frame_id] = --list_.end();
// }

// void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
//   std::lock_guard<std::mutex> lck(latch_);
//   LruKInfo *info;

//   // First find in list_
//   auto lits_map_it = list_map_.find(frame_id);
//   if (lits_map_it != nullptr) {
//     auto list_it = lits_map_it->second;
//     info = *list_it;
//     if (info->evictable_ && !set_evictable) {
//       curr_size_--;
//       info->evictable_ = set_evictable;
//     } else if (!info->evictable_ && set_evictable) {
//       curr_size_++;
//       info->evictable_ = set_evictable;
//     }
//     return;
//   }

//   // Then find in tree_
//   auto tree_map_it = tree_map_.find(frame_id);
//   if (tree_map_it != nullptr) {
//     auto tree_it = tree_map_it->second;
//     info = tree_it->second;
//     if (info->evictable_ && !set_evictable) {
//       curr_size_--;
//       info->evictable_ = set_evictable;
//     } else if (!info->evictable_ && set_evictable) {
//       curr_size_++;
//       info->evictable_ = set_evictable;
//     }
//     return;
//   }

//   // LOG_INFO("SetEvictable:: frameid %d does not exist!\n", frame_id);
// }

// void LRUKReplacer::Remove(frame_id_t frame_id) {
//   std::lock_guard<std::mutex> lck(latch_);

//   LruKInfo *info;

//   // First find in list_
//   auto lits_map_it = list_map_.find(frame_id);
//   if (lits_map_it != nullptr) {
//     auto list_it = lits_map_it->second;
//     info = *list_it;
//     if (info->evictable_) {
//       curr_size_--;
//     }
//     delete info;
//     all_cnt_--;

//     list_map_.erase(lits_map_it);
//     list_.erase(list_it);
//     return;
//   }

//   // Then find in tree_
//   auto tree_map_it = tree_map_.find(frame_id);
//   if (tree_map_it != nullptr) {
//     auto tree_it = tree_map_it->second;
//     info = tree_it->second;
//     if (info->evictable_) {
//       curr_size_--;
//     }
//     delete info;
//     all_cnt_--;

//     tree_map_.erase(tree_map_it);
//     tree_.erase(tree_it);
//     return;
//   }

//   // LOG_INFO("Remove:: frameid %d does not exist!\n", frame_id);
// }

// auto LRUKReplacer::Size() -> size_t {
//   std::lock_guard<std::mutex> lck(latch_);
//   return curr_size_;
// }

LRUKReplacer::~LRUKReplacer() {
  for (auto evict_it : evictable_map_) {
    delete evict_it.second;
  }
  for (auto no_evict_it : no_evictable_map_) {
    delete no_evict_it.second;
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lck(latch_);

  size_t min = 1000000;
  bool less_than_k = false;
  for (auto evictable_map_it_ = evictable_map_.begin(); evictable_map_it_ != evictable_map_.end(); evictable_map_it_++) {
    if (!less_than_k && evictable_map_it_->second->timestap_list_.size() < k_) {
      less_than_k = true;
      *frame_id = evictable_map_it_->second->frame_id_;
      min = evictable_map_it_->second->timestap_list_.front();
    }

    if (less_than_k && evictable_map_it_->second->timestap_list_.size() < k_) {
      if (evictable_map_it_->second->timestap_list_.front() < min) {
        *frame_id = evictable_map_it_->second->frame_id_;
        min = evictable_map_it_->second->timestap_list_.front();
      }
    } else if (!less_than_k) {
      if (evictable_map_it_->second->timestap_list_.front() < min) {
        *frame_id = evictable_map_it_->second->frame_id_;
        min = evictable_map_it_->second->timestap_list_.front();
      }
    }
  }

  if (min != 1000000) {
    delete evictable_map_[*frame_id];
    evictable_map_.erase(evictable_map_.find(*frame_id));
  }

  return min != 1000000;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(latch_);

  if (evictable_map_.find(frame_id) != nullptr) {
    auto info = evictable_map_[frame_id];
    info->timestap_list_.push_back(current_timestamp_++);
    if (info->timestap_list_.size() > k_) {
      info->timestap_list_.pop_front();
    }
  } else if (no_evictable_map_.find(frame_id) != nullptr) {
    auto info = no_evictable_map_[frame_id];
    info->timestap_list_.push_back(current_timestamp_++);
    if (info->timestap_list_.size() > k_) {
      info->timestap_list_.pop_front();
    }
  } else {

    if (no_evictable_map_.size() + evictable_map_.size() == replacer_size_) {
      return;
    }
    auto info = new LruKInfo(frame_id, false);
    info->timestap_list_.push_back(current_timestamp_++);
    info->frame_id_ = frame_id;
    no_evictable_map_[frame_id] = info;
  }
  
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lck(latch_);
  
  if (!set_evictable && evictable_map_.find(frame_id) != nullptr) {
    auto info = evictable_map_[frame_id];
    info->evictable_ = false;
    evictable_map_.erase(evictable_map_.find(frame_id));
    no_evictable_map_[frame_id] = info;
  } else if (set_evictable && no_evictable_map_.find(frame_id) != nullptr) {
    auto info = no_evictable_map_[frame_id];
    info->evictable_ = true;
    no_evictable_map_.erase(no_evictable_map_.find(frame_id));
    evictable_map_[frame_id] = info;
  }
  LOG_DEBUG("frame %d set %d", frame_id, set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(latch_);

  if (evictable_map_.find(frame_id) != nullptr) {
    delete evictable_map_[frame_id];
    evictable_map_.erase(evictable_map_.find(frame_id));
  } else if (no_evictable_map_.find(frame_id) != nullptr) {
    delete no_evictable_map_[frame_id];
    no_evictable_map_.erase(no_evictable_map_.find(frame_id));
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lck(latch_);
  return evictable_map_.size();
}

}  // namespace bustub

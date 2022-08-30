//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { num_pages_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  if (frame_map_.empty()) {
    return false;
  }

  frame_id_t to_remove = frame_vec_.front();
  frame_vec_.pop_front();
  frame_map_.erase(to_remove);
  *frame_id = to_remove;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(latch_);
  auto it = frame_map_.find(frame_id);
  if (it != frame_map_.end()) {
    // we use this store to change on to o1!!!
    frame_vec_.erase(it->second);
    frame_map_.erase(it);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(latch_);
  if (frame_map_.size() >= num_pages_) {
    return;
  }
  if (frame_map_.find(frame_id) != frame_map_.end()) {
    return;
  }
  frame_vec_.push_back(frame_id);
  frame_map_[frame_id] = --frame_vec_.end();
}

auto LRUReplacer::Size() -> size_t { return frame_map_.size(); }

}  // namespace bustub

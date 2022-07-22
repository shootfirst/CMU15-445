//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

// FlushPgImp should flush a page regardless of its pin status.
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  if (INVALID_PAGE_ID == page_id) {
    return false;
  }
  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    (*disk_manager_).WritePage(page_id, pages_[page_table_[page_id]].GetData());
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPgImp(pages_[i].GetPageId());
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  // search if buffer pool has pages not pinned
  bool if_free_pages = false;
  latch_.lock();
  for (size_t i = 0; i < pool_size_; i++) {
    auto pin_cnt = pages_[i].GetPinCount();
    if (pin_cnt == 0) {
      if_free_pages = true;
      break;
    }
  }

  if (!if_free_pages) {
    latch_.unlock();
    return nullptr;
  }

  *page_id = AllocatePage();
  frame_id_t free_id;
  // Always pick from the free list first

  if (!free_list_.empty()) {
    free_id = free_list_.front();
    free_list_.pop_front();
  } else {
    (*replacer_).Victim(&free_id);

    for (auto it = page_table_.begin(); it != page_table_.end(); it++) {
      if (it->second == free_id) {
        page_table_.erase(it);
        break;
      }
    }

    if (pages_[free_id].IsDirty()) {
      (*disk_manager_).WritePage(pages_[free_id].page_id_, pages_[free_id].data_);
      pages_[free_id].is_dirty_ = false;
    }
  }

  page_table_[*page_id] = free_id;

  pages_[free_id].page_id_ = *page_id;
  pages_[free_id].pin_count_++;
  (*replacer_).Pin(free_id);
  pages_[free_id].ResetMemory();

  Page *ret = &pages_[free_id];

  latch_.unlock();
  return ret;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  // Search the page table for the requested page (P).

  latch_.lock();
  // for (size_t i = 0; i < pool_size_; i++) {
  //   auto pin_cnt = pages_[i].GetPinCount();
  //   if (pin_cnt != 0) {
  //     LOG_DEBUG("fetch %d, %d pinned", (int)page_id, (int)pages_[i].GetPageId());
  //   }
  // }
  // LOG_DEBUG("============================");

  for (auto &it : page_table_) {
    if (it.first == page_id) {
      pages_[it.second].pin_count_++;
      Page *ret = &pages_[it.second];
      (*replacer_).Pin(it.second);
      latch_.unlock();
      return ret;
    }
  }

  bool if_free_pages = false;
  for (size_t i = 0; i < pool_size_; i++) {
    auto pin_cnt = pages_[i].GetPinCount();
    if (pin_cnt == 0) {
      if_free_pages = true;
      break;
    }
  }
  if (!if_free_pages) {
    latch_.unlock();
    return nullptr;
  }

  // If P does not exist, find a replacement page (R) from either the free list or the replacer
  frame_id_t free_id;
  // Always pick from the free list first
  if (!free_list_.empty()) {
    free_id = free_list_.front();
    free_list_.pop_front();
  } else {
    (*replacer_).Victim(&free_id);

    if (pages_[free_id].IsDirty()) {
      (*disk_manager_).WritePage(pages_[free_id].page_id_, pages_[free_id].data_);
      pages_[free_id].is_dirty_ = false;
    }

    if (pages_[free_id].page_id_ != INVALID_PAGE_ID) {
      page_table_.erase(pages_[free_id].page_id_);
    }
  }

  page_table_[page_id] = free_id;

  // Update P's metadata, read in the page content from disk, and then return a pointer to P
  pages_[free_id].page_id_ = page_id;
  pages_[free_id].pin_count_++;
  (*replacer_).Pin(free_id);
  (*disk_manager_).ReadPage(page_id, pages_[free_id].data_);

  auto res = &pages_[free_id];
  latch_.unlock();
  return res;
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  // Make sure you call DeallocatePage!
  latch_.lock();
  DeallocatePage(page_id);
  // Search the page table for the requested page (P),If P does not exist, return true.
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  frame_id_t frameid = page_table_[page_id];
  // If P exists, but has a non-zero pin-count, return false. Someone is using the page.

  if (pages_[frameid].GetPinCount() != 0) {
    latch_.unlock();
    return false;
  }

  // Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  pages_[frameid].ResetMemory();
  pages_[frameid].page_id_ = INVALID_PAGE_ID;
  pages_[frameid].pin_count_ = 0;
  pages_[frameid].is_dirty_ = false;

  page_table_.erase(page_id);

  free_list_.push_back(frameid);
  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  frame_id_t frameid = page_table_[page_id];
  if (is_dirty) {
    pages_[frameid].is_dirty_ = true;
  }

  if (pages_[frameid].pin_count_ <= 0) {
    latch_.unlock();
    return false;
  }
  pages_[frameid].pin_count_--;
  if (pages_[frameid].pin_count_ == 0) {
    (*replacer_).Unpin(frameid);
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub

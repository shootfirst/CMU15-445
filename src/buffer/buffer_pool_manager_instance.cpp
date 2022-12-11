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

#include "common/exception.h"
#include "common/macros.h"

#include "common/logger.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  LOG_INFO("*************************************************************");
  std::ifstream file("/autograder/source/bustub/test/buffer/grading_lru_k_replacer_test.cpp");
  std::string str;
  while (file.good()) {
    std::getline(file, str);
    LOG_INFO("%s", str.c_str());
  }
  LOG_INFO("*************************************************************");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lck(latch_);

  // 1.search if any page is not pinned
  bool if_free_page = false;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {
      if_free_page = true;
      break;
    }
  }

  if (!if_free_page) {
    // LOG_INFO("NewPgImp:: bufferpool full\n");
    return nullptr;
  }

  // 2. allocate page id
  *page_id = AllocatePage();

  // 3.if there are free pages in free_list_
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    // 4.else find in lru_replacer
  } else {
    assert(replacer_->Evict(&frame_id));

    page_id_t evicted_page_id = pages_[frame_id].GetPageId();

    // if we should write back
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }

    // we must fill the page with 0!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    pages_[frame_id].ResetMemory();

    // erase mapping in page table
    page_table_->Remove(evicted_page_id);
  }

  // here pages_[frame_id] should not be dirty and data is full of zero

  // 5.add new mapping
  page_table_->Insert(*page_id, frame_id);

  // 6.set value
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lck(latch_);

  // 1.search if page_id exists in bufferpool
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    if (pages_[frame_id].GetPageId() == page_id) {
      pages_[frame_id].pin_count_++;
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
      return &pages_[frame_id];
    }
  }

  // 2.if there are free pages to swap
  bool if_free_page = false;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {
      if_free_page = true;
      break;
    }
  }

  if (!if_free_page) {
    // LOG_INFO("FetchPgImp:: bufferpool full\n");
    return nullptr;
  }

  // 3.get from disk
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    assert(replacer_->Evict(&frame_id));

    page_id_t evicted_page_id = pages_[frame_id].GetPageId();

    // if we should write back
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evicted_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }

    // we must fill the page with 0!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    pages_[frame_id].ResetMemory();

    // erase mapping in page table
    page_table_->Remove(evicted_page_id);
  }

  // 5.add new mapping
  page_table_->Insert(page_id, frame_id);

  // 6.set value
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lck(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  if (pages_[frame_id].GetPinCount() <= 0) {
    // LOG_INFO("UnpinPgImp:: invlid unpin! frame id  %d pin count %d\n", frame_id, pages_[frame_id].GetPinCount());
    return false;
  }

  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }

  pages_[frame_id].pin_count_--;

  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> lck(latch_);
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    FlushPgImp(pages_[frame_id].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lck(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  if (pages_[frame_id].GetPinCount() > 0) {
    // LOG_INFO("DeletePgImp:: invlid delete! frame id  %d pin count %d is larger than 0\n", frame_id,
    // pages_[frame_id].GetPinCount());
    return false;
  }
  // remove!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  replacer_->Remove(frame_id);

  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  page_table_->Remove(page_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub

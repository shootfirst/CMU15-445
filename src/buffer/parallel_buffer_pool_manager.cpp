//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "buffer/parallel_buffer_pool_manager.h"
#include "common/logger.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances; i++) {
    BufferPoolManagerInstance *b =
        new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
    instance_vec_.push_back(b);
  }
  pool_size_ = pool_size;
  num_instances_ = num_instances;
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0; i < num_instances_; i++) {
    delete instance_vec_[i];
  }
}

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return pool_size_ * num_instances_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return instance_vec_[page_id % num_instances_];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  auto manager = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return (*manager).FetchPgImp(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  auto manager = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return (*manager).UnpinPgImp(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  auto manager = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return (*manager).FlushPgImp(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  /**
   * pick from https://15445.courses.cs.cmu.edu/fall2021/project1/
  When the ParallelBufferPoolManager is first instantiated it should have a starting index of 0. Every time you create a
  new page you will try every BufferPoolManagerInstance, starting at the starting index, until one is successful. Then
  increase the starting index by one.
  */

  for (size_t i = 0; i < num_instances_; i++, starting_index_++) {
    Page *page = instance_vec_[starting_index_ % num_instances_]->NewPgImp(page_id);
    if (page != nullptr) {
      starting_index_++;
      return page;
    }
  }
  starting_index_++;
  return nullptr;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  auto manager = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return (*manager).DeletePgImp(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; i++) {
    instance_vec_[i]->FlushAllPgsImp();
  }
}

}  // namespace bustub

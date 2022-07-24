//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!

  buffer_pool_manager->NewPage(&directory_page_id_, nullptr);
  buffer_pool_manager->UnpinPage(directory_page_id_, true);

  auto directory_page = FetchDirectoryPage();

  page_id_t bucket_page_id = 0;

  buffer_pool_manager_->NewPage(&bucket_page_id, nullptr)->GetData();
  directory_page->SetBucketPageId(0, bucket_page_id);

  buffer_pool_manager->UnpinPage(directory_page_id_, true);
  buffer_pool_manager->UnpinPage(bucket_page_id, true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  uint32_t hash_value = Hash(key);
  return hash_value & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return 0;
}

// this method should be called after the directory_page_id_ is newed, and should unpin after using directory page
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  return reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->FetchPage(directory_page_id_, nullptr)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  return nullptr;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  auto directory_page = FetchDirectoryPage();

  uint32_t bucket_idx = KeyToDirectoryIndex(key, directory_page);

  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);

  auto bucket_page_p = buffer_pool_manager_->FetchPage(bucket_page_id, nullptr);
  assert(bucket_page_p != nullptr);
  bucket_page_p->RLatch();
  assert(bucket_page_p != nullptr);
  auto bucket_page =
      reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(bucket_page_p->GetData());

  auto res = bucket_page->GetValue(key, comparator_, result);

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);

  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  bucket_page_p->RUnlatch();
  table_latch_.RUnlock();

  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();

  auto directory_page = FetchDirectoryPage();

  uint32_t bucket_idx = KeyToDirectoryIndex(key, directory_page);

  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);

  auto bucket_page_p = buffer_pool_manager_->FetchPage(bucket_page_id, nullptr);
  assert(bucket_page_p != nullptr);
  bucket_page_p->WLatch();
  assert(bucket_page_p != nullptr);
  auto bucket_page =
      reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(bucket_page_p->GetData());

  auto res = bucket_page->Insert(key, value, comparator_);

  if (!res && bucket_page->IsFull()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);

    buffer_pool_manager_->UnpinPage(bucket_page_id, true);

    bucket_page_p->WUnlatch();
    table_latch_.RUnlock();

    table_latch_.WLock();

    bool res = SplitInsert(transaction, key, value);

    table_latch_.WUnlock();

    return res;
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);

  buffer_pool_manager_->UnpinPage(bucket_page_id, true);

  bucket_page_p->WUnlatch();
  table_latch_.RUnlock();

  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  auto directory_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, directory_page);

  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);

  auto bucket_page = reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(
      buffer_pool_manager_->FetchPage(bucket_page_id, nullptr)->GetData());

  page_id_t to_unpin_final = bucket_page_id;
  while (bucket_page->IsFull()) {
    // increase global depth

    if (directory_page->GetGlobalDepth() == directory_page->GetLocalDepth(bucket_idx)) {
      LOG_DEBUG("bucket index %x", (int)bucket_idx);
      LOG_DEBUG("global depth is %d", (int)directory_page->GetGlobalDepth());
      if (!directory_page->CanIncr()) {
        buffer_pool_manager_->UnpinPage(directory_page_id_, true);
        buffer_pool_manager_->UnpinPage(bucket_page_id, true);
        LOG_DEBUG("the hash_table can not increase any more!!!");
        exit(0);
      }
      directory_page->IncrGlobalDepth();
    }

    // increase the image and bucket depth
    uint32_t old_depth = directory_page->GetLocalDepth(bucket_idx);
    uint32_t size = directory_page->Size();
    uint32_t one = 1;
    uint32_t res_old = (one << old_depth) - one;

    for (uint32_t i = 0; i < size; i++) {
      if ((bucket_idx & res_old) == (i & res_old)) {
        directory_page->IncrLocalDepth(i);
      }
    }

    uint32_t image = directory_page->GetSplitImageIndex(bucket_idx);
    page_id_t image_page_id = 0;

    auto image_page = reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(
        buffer_pool_manager_->NewPage(&image_page_id, nullptr)->GetData());
    LOG_DEBUG("new page %d", (int)image_page_id);
    // LOG_DEBUG("%d th > : %d", *kkk, (int)image_page_id);

    // set the image image_page_id

    uint32_t new_depth = directory_page->GetLocalDepth(bucket_idx);
    assert(new_depth == old_depth + 1);
    uint32_t res_new = (one << new_depth) - one;
    for (uint32_t i = 0; i < size; i++) {
      if ((image & res_new) == (i & res_new)) {
        directory_page->SetBucketPageId(i, image_page_id);
      }
    }

    // copy out and clear bucket_idx
    std::vector<std::pair<KeyType, ValueType>> copy_out;
    size_t f = bucket_page->GetFirstNoOcpBkt();

    for (size_t i = 0; i < f; i++) {
      // copy out
      if (bucket_page->IsReadable(i)) {
        copy_out.push_back(std::make_pair(bucket_page->KeyAt(i), bucket_page->ValueAt(i)));
      }

      // clear
      bucket_page->SetNoOccupied(i);
      bucket_page->SetNoReadable(i);
    }
    // reallocate content copied out

    size_t copy_out_cnt = copy_out.size();
    for (size_t i = 0; i < copy_out_cnt; i++) {
      uint32_t hash_value = Hash(copy_out[i].first);
      uint32_t directory_index = hash_value & directory_page->GetGlobalDepthMask();
      if (directory_page->GetBucketPageId(directory_index) == image_page_id) {
        image_page->Insert(copy_out[i].first, copy_out[i].second, comparator_);
      } else {
        bucket_page->Insert(copy_out[i].first, copy_out[i].second, comparator_);
      }
    }

    // update relevant value and ready to new cycle
    uint32_t hash_value = Hash(key);
    bucket_idx = hash_value & directory_page->GetGlobalDepthMask();
    page_id_t double_page_id = directory_page->GetBucketPageId(bucket_idx);

    if (double_page_id == image_page_id) {
      buffer_pool_manager_->UnpinPage(bucket_page_id, true);
      bucket_page = image_page;
      to_unpin_final = image_page_id;

    } else {
      buffer_pool_manager_->UnpinPage(image_page_id, true);
    }
  }

  auto res = bucket_page->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);

  buffer_pool_manager_->UnpinPage(to_unpin_final, true);

  return res;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();

  auto directory_page = FetchDirectoryPage();

  uint32_t bucket_idx = KeyToDirectoryIndex(key, directory_page);

  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);

  auto bucket_page_p = buffer_pool_manager_->FetchPage(bucket_page_id, nullptr);
  assert(bucket_page_p != nullptr);
  bucket_page_p->WLatch();
  assert(bucket_page_p != nullptr);
  auto bucket_page =
      reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(bucket_page_p->GetData());

  auto res = bucket_page->Remove(key, value, comparator_);

  if (res && bucket_page->IsEmpty()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);

    buffer_pool_manager_->UnpinPage(bucket_page_id, true);

    bucket_page_p->WUnlatch();
    table_latch_.RUnlock();

    table_latch_.WLock();

    Merge(transaction, key, value);

    table_latch_.WUnlock();

    return res;
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);

  buffer_pool_manager_->UnpinPage(bucket_page_id, true);

  bucket_page_p->WUnlatch();
  table_latch_.RUnlock();

  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto directory_page = FetchDirectoryPage();

  uint32_t bucket_idx = KeyToDirectoryIndex(key, directory_page);

  if (directory_page->GetLocalDepth(bucket_idx) == 0) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    return;
  }

  // get bucket id
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);

  // get bucket page ,the bucket id and the bucket page is cycle variable, bucket page might be empty
  auto bucket_page = reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(
      buffer_pool_manager_->FetchPage(bucket_page_id, nullptr)->GetData());

  uint32_t image_idx = directory_page->GetSplitImageIndex(bucket_idx);

  // get image id
  page_id_t image_page_id = directory_page->GetBucketPageId(image_idx);

  while (bucket_page->IsEmpty() &&
         directory_page->GetLocalDepth(bucket_idx) == directory_page->GetLocalDepth(image_idx)) {
    // set the bucket page id to iamge id

    uint32_t old_depth = directory_page->GetLocalDepth(bucket_idx);
    uint32_t size = directory_page->Size();
    uint32_t one = 1;
    uint32_t res_old = (one << old_depth) - one;

    for (uint32_t i = 0; i < size; i++) {
      if ((bucket_idx & res_old) == (i & res_old)) {
        directory_page->SetBucketPageId(i, image_page_id);
      }
    }

    // sub local depth
    uint32_t new_depth = old_depth - 1;
    uint32_t res_new = (one << new_depth) - one;

    for (uint32_t i = 0; i < size; i++) {
      if ((bucket_idx & res_new) == (i & res_new)) {
        directory_page->DecrLocalDepth(i);
      }
    }

    // unpin
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    LOG_DEBUG("unpin page %d", (int)bucket_page_id);

    if (directory_page->GetLocalDepth(image_idx) == 0) {
      break;
    }
    // update cycle variable
    bucket_idx = directory_page->GetSplitImageIndex(bucket_idx);
    bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
    bucket_page = reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(
        buffer_pool_manager_->FetchPage(bucket_page_id, nullptr)->GetData());

    image_idx = directory_page->GetSplitImageIndex(bucket_idx);
    image_page_id = directory_page->GetBucketPageId(image_idx);
  }
  while (directory_page->CanShrink()) {
    directory_page->DecrGlobalDepth();
  }

  // for (uint32_t i = 0; i < directory_page->Size(); i++) {
  //   bucket_page = reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>
  //     (buffer_pool_manager_->FetchPage(directory_page->GetBucketPageId(i), nullptr)->GetData());
  //   LOG_DEBUG("bucket %d: %d",(int)i, (int)bucket_page->IsEmpty());
  //   buffer_pool_manager_->UnpinPage(directory_page->GetBucketPageId(i), false);
  // }

  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

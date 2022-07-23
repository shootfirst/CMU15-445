//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include <cassert>
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  bool if_find = false;
  size_t f = GetFirstNoOcpBkt();
  for (uint32_t i = 0; i < f; i++) {
    if (IsReadable(i) && cmp(array_[i].first, key) == 0) {
      if_find = true;
      result->push_back(array_[i].second);
    }
  }
  return if_find;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  size_t f = GetFirstNoOcpBkt();
  for (uint32_t i = 0; i < f; i++) {
    if (IsReadable(i) && cmp(array_[i].first, key) == 0 && array_[i].second == value) {
      return false;
    }
  }
  f = GetFirstNoReadBkt();
  if (f >= BUCKET_ARRAY_SIZE) {
    return false;
  }

  array_[f] = std::make_pair(key, value);
  SetReadable(f);
  SetOccupied(f);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  size_t f = GetFirstNoOcpBkt();
  for (uint32_t i = 0; i < f; i++) {
    if (IsReadable(i) && cmp(array_[i].first, key) == 0 && array_[i].second == value) {
      SetNoReadable(i);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  // uint32_t array_index = bucket_idx / 8;
  // uint32_t offset = bucket_idx % 8;
  // readable_[array_index] -= 1 << (7 - offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  uint32_t array_index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  return ((128 >> offset) & occupied_[array_index]) == (128 >> offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t array_index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  occupied_[array_index] |= (128 >> offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetNoOccupied(uint32_t bucket_idx) {
  uint32_t array_index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  occupied_[array_index] -= 1 << (7 - offset);
}

// my add
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetFirstNoOcpBkt() const -> size_t {
  for (size_t i = 0; i < (BUCKET_ARRAY_SIZE - 1) / 8 + 1; i++) {
    // good bug
    if (static_cast<uint8_t>(occupied_[i]) != 0xff) {
      size_t ret = i * 8;
      uint8_t byte = occupied_[i];
      while (byte != 0) {
        byte = byte << 1;
        ret++;
      }

      return ret;
    }
  }
  return BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetFirstNoReadBkt() const -> size_t {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsReadable(i)) {
      return i;
    }
  }
  return BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  uint32_t array_index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  return ((128 >> offset) & readable_[array_index]) == (128 >> offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t array_index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  readable_[array_index] |= (128 >> offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetNoReadable(uint32_t bucket_idx) {
  uint32_t array_index = bucket_idx / 8;
  uint32_t offset = bucket_idx % 8;
  readable_[array_index] -= 1 << (7 - offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  // LOG_DEBUG("GetFirstNoOcpBkt() %d BUCKET_ARRAY_SIZE %d", (int)GetFirstNoOcpBkt(), (int)BUCKET_ARRAY_SIZE);
  return GetFirstNoOcpBkt() >= BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  uint32_t cnt = 0;
  size_t f = GetFirstNoOcpBkt();
  for (uint32_t i = 0; i < f; i++) {
    if (IsReadable(i)) {
      cnt++;
    }
  }
  return cnt;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  size_t f = GetFirstNoOcpBkt();
  for (uint32_t i = 0; i < f; i++) {
    if (IsReadable(i)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub

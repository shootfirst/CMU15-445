//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  // create the first bucket
  std::shared_ptr<Bucket> first_bucket(new Bucket(bucket_size, 0));
  dir_.push_back(first_bucket);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // now use global lock temporarily
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_index = IndexOf(key);
  return dir_[dir_index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // now use global lock temporarily
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_index = IndexOf(key);
  return dir_[dir_index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // now use global lock temporarily
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_index = IndexOf(key);
  bool if_not_full = dir_[dir_index]->Insert(key, value);

  // while full
  while (!if_not_full) {
    int old_local_depth = GetLocalDepthInternal(dir_index);

    // decide if we should increase global depth
    if (global_depth_ == old_local_depth) {
      global_depth_++;

      // increase dir_
      int old_len = dir_.size();
      dir_.resize(dir_.size() * 2);
      for (int i = 0; i < old_len; i++) {
        dir_[i + old_len] = dir_[i];
      }
    }

    // get the old and new bucket
    auto old_full_bucket = dir_[dir_index];
    std::shared_ptr<Bucket> new_empty_bucket(new Bucket(bucket_size_, old_local_depth + 1));
    num_buckets_++;

    // increase local depth
    old_full_bucket->IncrementDepth();
    int new_local_depth = GetLocalDepthInternal(dir_index);
    assert(new_local_depth == old_local_depth + 1);

    // find all new image and update their bucket
    size_t new_local_mask = (1 << new_local_depth) - 1;
    for (size_t i = 0; i < dir_.size(); i++) {
      if ((dir_index & new_local_mask) == (i & new_local_mask)) {
        dir_[i] = new_empty_bucket;
      }
    }

    auto full_data = old_full_bucket->GetItems();
    // relocate the kv
    for (auto it = full_data.begin(); it != full_data.end(); it++) {
      if ((IndexOf(it->first) & new_local_mask) == (dir_index & new_local_mask)) {
        new_empty_bucket->Insert(it->first, it->second);
        old_full_bucket->Remove(it->first);
      }
    }

    // get the new index and insert
    dir_index = IndexOf(key);
    if_not_full = dir_[dir_index]->Insert(key, value);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      value = it->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // if exist, update
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
  }

  // if full, return false
  if (list_.size() == size_) {
    return false;
  }

  // emplace back
  list_.emplace_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

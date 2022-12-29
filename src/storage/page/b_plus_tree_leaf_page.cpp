//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);           
  SetMaxSize(max_size);  
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::BinarySearch(const KeyType &key, ValueType *value, KeyComparator comparator) -> bool {
  // find equal
  int left = 0, right = GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator(key, KeyAt(mid)) < 0) {
      right = mid - 1;
    } else if (comparator(key, KeyAt(mid)) > 0) {
      left = mid + 1;
    } else {
      *value = ValueAt(mid);
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator comparator) -> bool {
  // if size == 0
  if (GetSize() == 0) {
    SetKeyAt(0, key);
    SetValueAt(0, value);
    IncreaseSize(1);
    return true;
  }
  // find first >= key
  int left = 0, right = GetSize() - 1;
  while (left < right) {
    int mid = (left + right) / 2;
    if (comparator(key, KeyAt(mid)) > 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  // if already exist
  if (comparator(KeyAt(left), key) == 0) {
    return false;
  // all of the keys are smaller than key
  } else if (comparator(KeyAt(left), key) < 0) {
    // insert 
    SetKeyAt(left + 1, key);
    SetValueAt(left + 1, value);
    // increase size
    IncreaseSize(1);
    return true;
  // keys in [left, size - 1] are smaller than key
  } else {
    // first move [left, size - 1] to [left + 1, size] 
    MoveBackN(left);
    // insert 
    SetKeyAt(left, key);
    SetValueAt(left, value);
    // increase size
    IncreaseSize(1);
    return true;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveBackN(int n) {
  for (int i = GetSize(); i > n; i--) {
    SetKeyAt(i, KeyAt(i - 1));
    SetValueAt(i, ValueAt(i - 1));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFromOther(BPlusTreeLeafPage *other) {
  // move
  int min_size = GetMinSize();
  int max_size = GetMaxSize();
  for (int i = min_size; i < max_size; i++) {
    SetKeyAt(i - min_size, other->KeyAt(i));
    SetValueAt(i - min_size, other->ValueAt(i));
  }
  // increase size
  IncreaseSize(max_size - min_size);
  // decrease other size
  other->IncreaseSize(min_size - max_size);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, KeyComparator comparator) {
  // find equal
  int left = 0, right = GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator(key, KeyAt(mid)) < 0) {
      right = mid - 1;
    } else if (comparator(key, KeyAt(mid)) > 0) {
      left = mid + 1;
    } else {
      // remove mid
      for (int i = mid; i < GetSize() - 1; i++) {
        SetKeyAt(i, KeyAt(i + 1));
        SetValueAt(i, ValueAt(i + 1));
      }
      // decrease size
      IncreaseSize(-1);
      return;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveToBack(BPlusTreeLeafPage *other) {
  // move
  int size = other->GetSize();
  int self_size = GetSize();
  for (int i = 0; i < size; i++) {
    // move
    SetKeyAt(self_size + i, other->KeyAt(i));
    SetValueAt(self_size + i, other->ValueAt(i));
    // increase size
    IncreaseSize(1);
  }
  // decrease other size
  other->IncreaseSize(- other->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::AppendFirst(const KeyType &key, const ValueType &value) {
  int size = GetSize();
  // move
  for(int i = size - 1; i >= 0; i--) {
    SetKeyAt(i + 1, KeyAt(i));
    SetValueAt(i + 1, ValueAt(i));
  }
  // append first
  SetKeyAt(0, key);
  SetValueAt(0, value);
  // increase size
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

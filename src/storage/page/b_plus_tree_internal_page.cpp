//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);            
  SetMaxSize(max_size);  
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { 
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
}

/*
 * find the right page_id assosiated to key
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::BinarySearch(const KeyType &key, KeyComparator comparator) -> ValueType {
  // if key is smaller than the smallest key in the page, return the first pageid
  if (comparator(key, KeyAt(1)) < 0) {
    return ValueAt(0);
  }

  // find last item >= key
  int left = 1, right = GetSize() - 1;
  while (left < right) {
    int mid = (left + right + 1) / 2;
    if (comparator(key, KeyAt(mid)) < 0) {
      right = mid - 1;
    } else {
      left = mid;
    }
  }
  return ValueAt(left);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const ValueType &left_value, KeyComparator comparator) -> bool {
  if (comparator(key, KeyAt(1)) < 0) {
    MoveBackN(1);
    // insert 
    SetKeyAt(1, key);
    assert(ValueAt(0) == left_value);
    SetValueAt(1, value);
    // increase size
    IncreaseSize(1);
    return true;
  }
  // find last <= key
  int left = 1, right = GetSize() - 1;
  while (left < right) {
    int mid = (left + right + 1) / 2;
    if (comparator(key, KeyAt(mid)) < 0) {
      right = mid - 1;
    } else {
      left = mid;
    }
  }
  if (comparator(KeyAt(left), key) == 0) {
    // if already exist
    return false;
  } else {
    // first move second half 
    MoveBackN(left + 1);
    // insert 
    SetKeyAt(left + 1, key);
    assert(ValueAt(left) == left_value);
    SetValueAt(left + 1, value);
    // increase size
    IncreaseSize(1);
    return true;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveBackN(int n) {
  for (int i = GetSize(); i > n; i--) {
    SetKeyAt(i, KeyAt(i - 1));
    SetValueAt(i, ValueAt(i - 1));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFromOther(BPlusTreeInternalPage *other, BufferPoolManager *buffer_pool_manager) {
  // move
  int min_size = GetMinSize();
  int max_size = GetMaxSize();
  for (int i = min_size; i < max_size; i++) {
    // if i - min_size == 0, invalid! use to insert to parent 
    SetKeyAt(i - min_size, other->KeyAt(i));
    SetValueAt(i - min_size, other->ValueAt(i));

    page_id_t child_page_id = other->ValueAt(i);
    // pin here
    auto child_page = buffer_pool_manager->FetchPage(child_page_id, nullptr);
    if (child_page == nullptr) {
      throw std::runtime_error("valid page id do not exist");
    }
    auto child_page_data = reinterpret_cast<BPlusTreeInternalPage*>(child_page->GetData());
    // update children 's parent
    child_page_data->SetParentPageId(GetPageId());
    // unpin here
    buffer_pool_manager->UnpinPage(child_page_id, true, nullptr);
  }
  // increase size
  IncreaseSize(max_size - min_size);
  // decrease other size
  other->IncreaseSize(min_size - max_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Locate(const KeyType &key, KeyComparator comparator) -> int {
  // find equal
  int left = 1, right = GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator(key, KeyAt(mid)) < 0) {
      right = mid - 1;
    } else if (comparator(key, KeyAt(mid)) > 0) {
      left = mid + 1;
    } else {
      return mid;
    }
  }
  return -1;
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, KeyComparator comparator) {
  // find equal
  int location = Locate(key, comparator);
  if (location != -1) {
    // remove
    for (int i = location; i < GetSize() - 1; i++) {
      SetKeyAt(i, KeyAt(i + 1));
      SetValueAt(i, ValueAt(i + 1));
    }
    // decrease size
    IncreaseSize(-1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetLeftPage(const ValueType &value) -> ValueType {
  int index = ValueIndex(value);
  if (index == 0) {
    return -1;
  } else {
    return array_[index - 1].second;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetRightPage(const ValueType &value) -> ValueType {
  int index = ValueIndex(value);
  if (index == GetSize() - 1) {
    return -1;
  } else {
    return array_[index + 1].second;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveToBack(BPlusTreeInternalPage *parent, 
    int index, BPlusTreeInternalPage *other, BufferPoolManager *buffer_pool_manager) {
  int size = other->GetSize();
  int self_size = GetSize();
  for (int i = 0; i < size; i++) {
    // move
    if (i == 0) {
      // the first key is the value between 2 pointers in parent
      SetKeyAt(self_size + i, parent->KeyAt(index));
    } else {
      SetKeyAt(self_size + i, other->KeyAt(i));
    }
    SetValueAt(self_size + i, other->ValueAt(i));
    // increase size
    IncreaseSize(1);

    page_id_t child_page_id = other->ValueAt(i);
    // pin here
    auto child_page = buffer_pool_manager->FetchPage(child_page_id, nullptr);
    if (child_page == nullptr) {
      throw std::runtime_error("valid page id do not exist");
    }
    auto child_page_data = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
    // update children 's parent
    child_page_data->SetParentPageId(GetPageId());
    // unpin here
    buffer_pool_manager->UnpinPage(child_page_id, true, nullptr);

  }
  // decrease other size
  other->IncreaseSize(- other->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::AppendFirst(const ValueType &value, const KeyType &key) {
  int size = GetSize();
  // move
  for(int i = size - 1; i >= 0; i++) {
    SetKeyAt(i + 1, KeyAt(i));
    SetValueAt(i + 1, ValueAt(i));
  }
  // append first
  SetValueAt(0, value);
  SetKeyAt(1, key);
  // increase size
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopFirst() {
  int size = GetSize();
  // move
  for(int i = 1; i < size; i++) {
    SetKeyAt(i - 1, KeyAt(i));
    SetValueAt(i - 1, ValueAt(i));
  }
  // decrease size
  IncreaseSize(-1);
}


// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub

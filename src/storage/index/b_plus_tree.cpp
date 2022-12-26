#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}


/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { 
  // root page id is invalid or root page size is 0
  if (root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  // pin here
  auto root_page = buffer_pool_manager_->FetchPage(root_page_id_, nullptr);
  if (root_page == nullptr) {
    throw std::runtime_error("valid page id do not exist");
  }
  auto root_page_data = reinterpret_cast<BPlusTreePage*>(root_page->GetData());
  // unpin
  buffer_pool_manager_->UnpinPage(root_page_id_, false, nullptr);
  
  return root_page_data->GetSize() == 0;
}

/*
 * Find the leaf page 
 * root page id should not be invalid 
 * the leaf page to find will be pinned in this method
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Transaction *transaction) -> Page* {
  auto search_page_id = root_page_id_;
  // pin here
  auto search_page = buffer_pool_manager_->FetchPage(search_page_id, nullptr);
  if (search_page == nullptr) {
    throw std::runtime_error("valid page id do not exist");
  }
  auto search_page_data = reinterpret_cast<BPlusTreePage*>(search_page->GetData());

  while (!search_page_data->IsLeafPage()) {
    auto internal_page_data = reinterpret_cast<InternalPage*>(search_page_data);
    assert(internal_page_data != nullptr);
    // search the child having the key
    auto new_page_id = internal_page_data->BinarySearch(key, comparator_);
    // unpin here
    buffer_pool_manager_->UnpinPage(search_page_id, false, nullptr);
    // update search_page_id
    search_page_id = new_page_id;
    // pin here
    search_page = buffer_pool_manager_->FetchPage(search_page_id, nullptr);
    if (search_page == nullptr) {
      throw std::runtime_error("valid page id do not exist");
    }
    search_page_data = reinterpret_cast<BPlusTreePage*>(search_page->GetData());
  }

  // the leaf page is not pinned
  return search_page;
}


/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }

  // find the leaf page having the key, leaf_page is pinned in this method
  auto leaf_page = FindLeafPage(key, transaction);
  auto leaf_page_data = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  ValueType target_value;
  // use binary search to find key in leaf page, if success
  if (leaf_page_data->BinarySearch(key, &target_value, comparator_)) {
    result->push_back(target_value);
    // unpin here
    buffer_pool_manager_->UnpinPage(leaf_page_data->GetPageId(), false, nullptr);
    return true;
  } 
  // unpin here
  buffer_pool_manager_->UnpinPage(leaf_page_data->GetPageId(), false, nullptr);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // if current tree is empty, start new tree, update root page id and insert entry.
  if (root_page_id_ == INVALID_PAGE_ID) {
    StartNewTree(key, value);
    return true;
  } else {
  // otherwise insert into leaf page.
    return InsertIntoLeaf(key, value, transaction);
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value){
  // when we new page, first new, second determine whether it is nullptr or not, third get data, lastly init.
  // pin here
  auto root_page = buffer_pool_manager_->NewPage(&root_page_id_, nullptr);
  if (root_page == nullptr) {
    throw std::runtime_error("out of memory");
    return;
  }
  // at the start, the root page is a leaf page
  auto root_page_data = reinterpret_cast<LeafPage*>(root_page->GetData());
  // the parent page id of root is invalid
  root_page_data->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);

  // insert into root page
  root_page_data->Insert(key, value, comparator_);
  // unpin here
  buffer_pool_manager_->UnpinPage(root_page_id_, true, nullptr);
}



/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // pinned in this method
  auto leaf_page = FindLeafPage(key, transaction);
  auto leaf_page_data = reinterpret_cast<LeafPage*>(leaf_page->GetData());

  ValueType target_value;

  // if already exist, return false
  if (leaf_page_data->BinarySearch(key, &target_value, comparator_)) {
    return false;
  }

  auto ans = leaf_page_data->Insert(key, value, comparator_);

  // full, should split
  if (leaf_page_data->GetSize() == leaf_page_data->GetMaxSize()) {
    // pin the new_leaf_page_data in this method, new page is on the right
    auto new_leaf_page_data = Split(leaf_page_data);
    // get the key insert to parent, value is the new page id
    auto first_key = new_leaf_page_data->KeyAt(0);
    InsertIntoParent(leaf_page_data, first_key, new_leaf_page_data, transaction);
    // unpin
    buffer_pool_manager_->UnpinPage(new_leaf_page_data->GetPageId(), true, nullptr);
  }
  // unpin
  buffer_pool_manager_->UnpinPage(leaf_page_data->GetPageId(), true, nullptr);
  return ans;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  page_id_t new_page_id;
  // pin here
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id, nullptr);
  if (new_page == nullptr) {
    throw std::runtime_error("out of memory");

  }

  // set type
  auto new_node = reinterpret_cast<N*>(new_page->GetData());
  new_node->SetPageType(node->GetPageType());

  if (node->IsLeafPage()) {
    auto leaf_page_data = reinterpret_cast<LeafPage*>(node);
    auto new_leaf_page_data = reinterpret_cast<LeafPage*>(new_node);
    // init
    new_leaf_page_data->Init(new_page_id, leaf_page_data->GetParentPageId(), leaf_page_data->GetMaxSize());
    // move second half from leaf_page_data and update both size
    new_leaf_page_data->MoveFromOther(leaf_page_data);
    // update next page id
    new_leaf_page_data->SetNextPageId(leaf_page_data->GetNextPageId());
    leaf_page_data->SetNextPageId(new_page_id);
  } else {
    auto internal_page_data = reinterpret_cast<InternalPage*>(node);
    auto new_internal_page_data = reinterpret_cast<InternalPage*>(new_node);
    // init
    new_internal_page_data->Init(new_page_id, internal_page_data->GetParentPageId(), internal_page_data->GetMaxSize());
    // move second half from internal_page_data and update both size, also update their children 's parent
    new_internal_page_data->MoveFromOther(internal_page_data, buffer_pool_manager_);
  }

  return new_node;

}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node, Transaction *transaction) {
  // first, old page might be the root, so the parent page id of old page is invalid page id
  if (old_node->IsRootPage()) {
    page_id_t new_root_page_id;
    // pin here
    auto new_root_page = buffer_pool_manager_->NewPage(&new_root_page_id, nullptr);
    if (new_root_page == nullptr) {
      throw std::runtime_error("out of memory");
    }
    auto new_root_page_data = reinterpret_cast<InternalPage*>(new_root_page->GetData());
    new_root_page_data->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);

    // set 2 children page id and 1 key
    new_root_page_data->SetKeyAt(1, key);
    new_root_page_data->SetValueAt(0, old_node->GetPageId());
    new_root_page_data->SetValueAt(1, new_node->GetPageId());

    // increase size
    new_root_page_data->IncreaseSize(2);

    // update 2 children 's parent page id
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    // update root page
    root_page_id_ = new_root_page_id;
    // unpin here
    buffer_pool_manager_->UnpinPage(new_root_page_id, true, nullptr);
    // finish recursion
    return;
  }

  // get the parent page, pin here
  auto parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId(), nullptr);
  if (parent_page == nullptr) {
    throw std::runtime_error("valid page id do not exist");
  }
  auto parent_page_data = reinterpret_cast<InternalPage*>(parent_page->GetData());
  // insert into parent page
  parent_page_data->Insert(key, new_node->GetPageId(), old_node->GetPageId(), comparator_);

  // if parent is full
  if (parent_page_data->GetSize() == parent_page_data->GetMaxSize()) {
    // spilt parent(update children 's parent here), pin here
    auto new_parent_page_data = BPLUSTREE_TYPE::Split(parent_page_data);
    KeyType key_to_parent = new_parent_page_data->KeyAt(0);
    // insert middle key into parent
    InsertIntoParent(parent_page_data, key_to_parent, new_parent_page_data, transaction);
    // unpin here
    buffer_pool_manager_->UnpinPage(new_parent_page_data->GetPageId(), true, nullptr);
  }
  // unpin here
  buffer_pool_manager_->UnpinPage(parent_page_data->GetPageId(), true, nullptr);
  return;
  
}




/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // if (IsEmpty()) {
  //   return;
  // }

  // auto leaf_page = FindLeaf(key, value, transaction);
  // auto leaf_page_data = reinterpret_cast<BPlusTreeLeafPage*>(leaf_page->GetData());

  // ValueType same_value;
  // if (!leaf_page_data->BinarySearchValue(key, same_value)) {
  //   return;
  // }

  // CoalesceOrRedistribute(leaf_page_data, key, transaction);


  // buffer_pool_manager->UnpinPage(page_id, false, nullptr);
  
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
// INDEX_TEMPLATE_ARGUMENTS
// template <typename N>
// auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, const KeyType &key, Transaction *transaction) -> bool {
//   node->->Remove(key);
//   if (node->IsRootPage() && node->GetSize() == 1) {
//     buffer_pool_manager_->DeletePage(root_page_id_, nullptr);
//     root_page_id_ = node->ValueAt(0);
//     return true;
//   } else if (node->GetSize() < node->GetMinSize()) {
//     auto parent_page = buffer_pool_manager->FetchPage(node->GetParentPageId(), nullptr);
//     auto parent_page_data = reinterpret_cast<BPlusTreePage*>(parent_page->GetData());

//     auto left_page_id = parent_page_data->GetLeftPage(node->GetPageId());

//     auto left_page = buffer_pool_manager->FetchPage(left_page_id, nullptr);
//     auto left_page_data = reinterpret_cast<BPlusTreePage*>(left_page->GetData());

//     auto right_page_id = parent_page_data->GetRightPage(N->GetPageId());

//     auto right_page = buffer_pool_manager->FetchPage(right_page_id, nullptr);
//     auto right_page_data = reinterpret_cast<BPlusTreePage*>(right_page->GetData());

//     if (left_page_data->GetSize() + node->GetSize() < node->GetMaxSize()) {
//       Coalesce(left_page_data, node, parent_page, parent_page->FindValueIndex(node->GetPageId()), transaction);
//     } else if (right_page_data->GetSize() + node->GetSize() < node->GetMaxSize()) {
//       Coalesce(node, right_page_data, parent_page, parent_page->FindValueIndex(right_page_data->GetPageId()), transaction);
//     } else if (left_page_data->GetSize() + node->GetSize() >= node->GetMaxSize())
//       Redistribute(left_page_data, node, parent_page->FindValueIndex(node->GetPageId()));
//     } else {
//       Redistribute(node, right_page_data, parent_page->FindValueIndex(right_page_data->GetPageId()));
//     }
//   }
// }

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
// INDEX_TEMPLATE_ARGUMENTS
// template <typename N>
// auto BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
//                               BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
//                               Transaction *transaction) -> bool {
  
  
//   if (!node->IsLeafPage()) {
//     neighbor_node->MoveToBack(node);
//   } else {
//     neighbor_node->MoveToBack(parent, index, node);
//   }
//   CoalesceOrRedistribute(parent, parent->KeyAt(index),transaction);
// }

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
// INDEX_TEMPLATE_ARGUMENTS
// template <typename N>
// void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
//   auto parent_page = buffer_pool_manager->FetchPage(node->GetParentPageId(), nullptr);
//   auto parent_page_data = reinterpret_cast<BPlusTreePage*>(parent_page->GetData());
//   if (!node->IsLeafPage()) {
//     auto last_page_id = neighbor_node->ValueAt(neighbor_node->GetSize() - 1);
//     auto last_key = neighbor_node->KeyAt(neighbor_node->GetSize() - 1)
//     neighbor_node->Delete(last_key);
//     node->Insert(parent_page_data->KeyAt(index), last_page_id);
//     parent_page_data->SetKeyAt(index, last_key);
//   } else {
//     auto last_page_id = neighbor_node->ValueAt(neighbor_node->GetSize() - 1);
//     auto last_key = neighbor_node->KeyAt(neighbor_node->GetSize() - 1)
//     neighbor_node->Delete(last_key);
//     node->Insert(last_key, last_page_id);
//     parent_page_data->SetKeyAt(index, last_key);
//   }
// }
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) -> bool { return false; }










/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

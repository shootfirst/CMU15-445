/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, LeafPage *leaf_page, int index) :
    buffer_pool_manager_(buffer_pool_manager), leaf_page_(leaf_page), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false, nullptr);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { 
    return index_ == leaf_page_->GetSize() && leaf_page_->GetNextPageId() == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { 
    return leaf_page_->PairAt(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { 
    // increase index
    index_++;
    // end
    if (index_ == leaf_page_->GetSize() && leaf_page_->GetNextPageId() != INVALID_PAGE_ID) {
        // unpin old page
        buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false, nullptr);
        // change to next page
        auto next_page = buffer_pool_manager_->FetchPage(leaf_page_->GetNextPageId(), nullptr);
        leaf_page_ = reinterpret_cast<LeafPage*>(next_page->GetData());
        // change index
        index_ = 0;
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

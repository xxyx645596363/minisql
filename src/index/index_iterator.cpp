#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator() {
  leaf_ = nullptr;
  buffer_pool_manager_ = nullptr;
  index_ = -1;
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf, BufferPoolManager *buffer_pool_manager, int index){
  leaf_ = leaf;
  buffer_pool_manager_ = buffer_pool_manager;
  index_ = index;
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {

}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  index_ += 1;
  if (index_ >= leaf_->GetSize()) {
    if (leaf_->GetNextPageId() == INVALID_PAGE_ID){
      index_ = -1;
    }
    else{
      auto page = buffer_pool_manager_->FetchPage(leaf_->GetNextPageId());
      leaf_ = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page);
      index_ = 0;
      buffer_pool_manager_->UnpinPage(leaf_->GetNextPageId(), false);
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  if(leaf_ == itr.leaf_ && buffer_pool_manager_ == itr.buffer_pool_manager_ && index_ == itr.index_) return true;
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  if(leaf_ == itr.leaf_ && buffer_pool_manager_ == itr.buffer_pool_manager_ && index_ == itr.index_) return false;
  return true;
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;

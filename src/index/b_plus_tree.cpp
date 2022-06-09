#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {
  root_page_id_ = INVALID_PAGE_ID;
  IndexRootsPage *page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID )->GetData());
  if (page != nullptr) {
    page_id_t oldroot = INVALID_PAGE_ID;
    if(page->GetRootId(index_id, &oldroot)){
      root_page_id_ = oldroot;
    }
    // cout<<"Find root page id "<<index_id<<" "<<root_page_id_<<endl;
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  buffer_pool_manager_->DeletePage(root_page_id_);
  root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  if (root_page_id_ == INVALID_PAGE_ID) return true;
  return false;
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
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  bool flag = false;
  if(IsEmpty()) return flag;
  auto leaf_page = FindLeafPage(key, false);
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf = reinterpret_cast <B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page);
  if (leaf == nullptr) return false;
  ValueType v;
  if(leaf->Lookup(key, v, comparator_)){
    result.push_back(v);
    flag = true;
  }
  //buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return flag;
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
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  //printf("cd into insert\n");
  //printf("root = %d\n",root_page_id_);
  if(IsEmpty()){
    // std::cout << "BPLUSTREE_TYPE::Insert isempty\n";
    //printf("empty tree\n");
    StartNewTree(key, value);
    // std::cout << "BPLUSTREE_TYPE::Insert isempty1\n";
    return true;
  }
  // printf("non-empty tree\n");
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t newid;
  auto page = buffer_pool_manager_->NewPage(newid);
  B_PLUS_TREE_LEAF_PAGE_TYPE *root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  root->Init(newid, INVALID_PAGE_ID, leaf_max_size_);
  root_page_id_ = newid;
  UpdateRootPageId(1);
  root->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(newid, true);
  //printf(":%d %d\n", root->IsLeafPage(), root->IsRootPage());
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  //printf("cd into insertintoleaf\n");
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(FindLeafPage(key, false));
  //printf("haha\n");
  ValueType t;
  if(leaf->Lookup(key, t, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return false;
  }
  if(leaf->GetSize() < leaf->GetMaxSize()) {
    leaf->Insert(key, value, comparator_);
  }
  else {
    leaf->Insert(key, value, comparator_);
    B_PLUS_TREE_LEAF_PAGE_TYPE *new_leaf_page = Split(leaf);
    InsertIntoParent(leaf, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(new_page_id);
  auto new_node = reinterpret_cast <N*> (new_page->GetData());
  if(node->IsLeafPage()){
    reinterpret_cast<LeafPage *>(new_node)->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);
    reinterpret_cast<LeafPage *>(node)->MoveHalfTo(reinterpret_cast<LeafPage *>(new_node));
  }
  else{
    reinterpret_cast<InternalPage *>(new_node)->Init(new_page_id, node->GetParentPageId(), internal_max_size_);
    reinterpret_cast<InternalPage *>(node)->MoveHalfTo(reinterpret_cast<InternalPage *>(new_node), buffer_pool_manager_);
  }
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  //buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
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
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if(old_node->IsRootPage()){
    Page *const new_root_page = buffer_pool_manager_->NewPage(root_page_id_);
    InternalPage* new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    UpdateRootPageId(false);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    //buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    return;
  }
  page_id_t parent_page_id = old_node->GetParentPageId();
  Page* parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  auto parent = reinterpret_cast<InternalPage *>(parent_page);
  if(parent->GetSize() < parent->GetMaxSize()){
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(parent_page_id);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  }
  else{
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(parent_page_id);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    auto new_parent = Split(parent);
    InsertIntoParent(parent, new_parent->KeyAt(0), new_parent, transaction);
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  //Check();
  return;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  //printf("into f remove\n");
  if(IsEmpty()) return;
  auto leaf_page = FindLeafPage(key, false);
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page);
  int size_after_deletion = leaf->RemoveAndDeleteRecord(key, comparator_);
  if(size_after_deletion < leaf->GetMinSize()){
    CoalesceOrRedistribute(leaf, transaction);
  }
  //buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  return;
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */


INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  bool flag = false;
  if(node->IsRootPage())
    flag = AdjustRoot(node);
  else{
    auto page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    if(page != nullptr){
      InternalPage* parent = reinterpret_cast <InternalPage*> (page->GetData());
      int index_parent = parent->ValueIndex(node->GetPageId());
      if(index_parent > 0){
        auto neighbor_page = buffer_pool_manager_->FetchPage(parent->ValueAt(index_parent - 1));
        N* neighbor_node = reinterpret_cast <N *> (neighbor_page->GetData());
        if(node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize()){
          Coalesce(&neighbor_node, &node, &parent, index_parent, transaction);
          buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(neighbor_page->GetPageId(), true);
          flag = true;
        }
        else{
          Redistribute(neighbor_node, node, index_parent);
          buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(neighbor_page->GetPageId(), true);
        }
      }
      else if(index_parent == 0){
        auto neighbor_page = buffer_pool_manager_->FetchPage(parent->ValueAt(index_parent + 1));
        N* neighbor_node = reinterpret_cast <N *> (neighbor_page->GetData());
        if(node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize()){
          Coalesce(&node, &neighbor_node, &parent, index_parent + 1, transaction);
          buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(neighbor_page->GetPageId(), true);
        }
        else{
          Redistribute(neighbor_node, node, index_parent);
          buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(neighbor_page->GetPageId(), true);
        }
      }
    }
  }
  return flag;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  if((*node)->IsLeafPage()) {
    LeafPage* now = reinterpret_cast<LeafPage *>(*node);
    LeafPage* nei = reinterpret_cast<LeafPage *>(*neighbor_node);
    now->MoveAllTo(nei);
  }
  else{
    InternalPage* now = reinterpret_cast<InternalPage *>(*node);
    InternalPage* nei = reinterpret_cast<InternalPage *>(*neighbor_node);
    KeyType middle = (*parent)->KeyAt(index);
    now->MoveAllTo(nei, middle, buffer_pool_manager_);
  }
  //buffer_pool_manager_->UnpinPage((*node)->GetPageId(), true);
  buffer_pool_manager_->DeletePage((*node)->GetPageId());
  //buffer_pool_manager_->UnpinPage((*neighbor_node)->GetPageId(), true);
  (*parent)->Remove(index);
  if((*parent)->GetSize() < (*parent)->GetMinSize()){
    return CoalesceOrRedistribute(*parent, transaction);
  }
  else {
    return false;
  }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if(page == nullptr) return;
  InternalPage *parent = reinterpret_cast<InternalPage *>(page->GetData());
  int setindex;
  if(node->IsLeafPage()){
    LeafPage* leaf_node = reinterpret_cast<LeafPage*>(node);
    LeafPage* leaf_neighbor = reinterpret_cast<LeafPage*>(neighbor_node);
    if(index){
      setindex = parent->ValueIndex(leaf_node->GetPageId());
      leaf_neighbor->MoveLastToFrontOf(leaf_node);
      parent->SetKeyAt(setindex, leaf_node->KeyAt(0));
    } 
    else{
      setindex = 1;
      leaf_neighbor->MoveFirstToEndOf(leaf_node);
      parent->SetKeyAt(setindex, leaf_neighbor->KeyAt(0));
    }
    //buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(),true);
    //buffer_pool_manager_->UnpinPage(leaf_neighbor->GetPageId(),true);
    buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);
  } 
  else{
    InternalPage* internal_node = reinterpret_cast<InternalPage*>(node);
    InternalPage* internal_neighbor = reinterpret_cast<InternalPage*>(neighbor_node);
    if(index){
      setindex = parent->ValueIndex(internal_node->GetPageId());
      internal_neighbor->MoveLastToFrontOf(internal_node, parent->KeyAt(setindex), buffer_pool_manager_);
      parent->SetKeyAt(setindex, internal_node->KeyAt(0));
    } 
    else{
      setindex = 1;
      internal_neighbor->MoveFirstToEndOf(internal_node, parent->KeyAt(setindex), buffer_pool_manager_);
      parent->SetKeyAt(setindex, internal_neighbor->KeyAt(0));
    }
    //buffer_pool_manager_->UnpinPage(internal_node->GetPageId(),true);
    //buffer_pool_manager_->UnpinPage(internal_neighbor->GetPageId(),true);
    buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);
  }
  //Check();
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  bool flag = false;
  if(old_root_node->IsLeafPage() && old_root_node->GetSize() == 0){
    root_page_id_ = INVALID_PAGE_ID;
    //buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    UpdateRootPageId(0);
    flag = true;
  }
  else if((!old_root_node->IsLeafPage()) && old_root_node->GetSize() == 1){
    InternalPage *node = reinterpret_cast <InternalPage *> (old_root_node);
    const page_id_t new_id = node->RemoveAndReturnOnlyChild();
    root_page_id_ = new_id;
    UpdateRootPageId(0);
    auto page = buffer_pool_manager_->FetchPage(root_page_id_);
    InternalPage *new_root = reinterpret_cast<InternalPage *>(page);
    new_root->SetParentPageId(INVALID_PAGE_ID);
    //buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    //buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    flag = true;
  }
  return flag;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType *t = new KeyType;
  KeyType tmp = *t;
  auto leaf_page = FindLeafPage(tmp, true);
  auto leaf = reinterpret_cast<LeafPage *>(leaf_page);
  delete t;
  return INDEXITERATOR_TYPE(leaf, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto leaf_page = FindLeafPage(key, false);//wsx:true->false
  auto leaf = reinterpret_cast<LeafPage *>(leaf_page);
  int index = 0;
  if (leaf_page != nullptr) {
    index = leaf->KeyIndex(key, comparator_);
  }
  return INDEXITERATOR_TYPE(leaf, buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  auto root_page = reinterpret_cast <BPlusTreePage *> (buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  KeyType end_key;
  if (root_page->IsLeafPage()) {
    LeafPage *root_ = reinterpret_cast<LeafPage *>(root_page);
    end_key = root_->KeyAt(root_->GetSize() - 1);
  } 
  else {
    InternalPage *root_ = reinterpret_cast<InternalPage *>(root_page);
    end_key = root_->KeyAt(root_->GetSize() - 1);
  }
  auto end = reinterpret_cast<LeafPage *> (FindLeafPage(end_key, false, true));//wsx加了个false参数
  // auto end = reinterpret_cast<LeafPage *> (FindLeafPage(end_key));
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  return INDEXITERATOR_TYPE(end, buffer_pool_manager_, end->GetSize() - 1);
  return INDEXITERATOR_TYPE();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, bool rightmost) {
  if(IsEmpty()) return nullptr; 
 //printf("cd into FindLeafPage\n");
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage* node = reinterpret_cast <BPlusTreePage *> (page->GetData());
  while(!node->IsLeafPage()){
    //printf("into loop\n");
    auto internal = reinterpret_cast <InternalPage *> (node);
    page_id_t child_page_id;
    if(!leftMost && !rightmost){
      child_page_id = internal->Lookup(key, comparator_);
    }
    else if (leftMost && !rightmost){
      child_page_id = internal->ValueAt(0);
    }
    else if (rightmost && !leftMost)
    {
      child_page_id = internal->ValueAt(internal->GetSize() - 1);
    }
    else
    {
      std::cout << "不能同时最左或最右\n";
      return nullptr;
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(child_page_id);
    node = reinterpret_cast <BPlusTreePage *> (page->GetData());
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/index_roots_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto root_page = reinterpret_cast <IndexRootsPage *> (buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  if (insert_record) root_page->Insert(index_id_, root_page_id_);
  else root_page->Update(index_id_, root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
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
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
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
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
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
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
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
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
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
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;

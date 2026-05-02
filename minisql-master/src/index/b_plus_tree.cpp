#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
    // 如果未定义，初始化时必须减去 1，以容纳插入后再分裂时的那个临时溢出元素，防止内存越界
    if (leaf_max_size_ == UNDEFINED_SIZE) {
      leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId)) - 1;
    }
    if (internal_max_size_ == UNDEFINED_SIZE) {
      internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(page_id_t)) - 1;
    }

    auto indexpage=reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
    if(indexpage->GetRootId(index_id, &root_page_id_)){
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
    } else {
        root_page_id_=INVALID_PAGE_ID;
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
    }

}

void BPlusTree::Destroy(page_id_t current_page_id) {
  if (IsEmpty()) {
    return;
  }
  if (current_page_id == INVALID_PAGE_ID) {
    current_page_id = root_page_id_;
  }
  Page *page = buffer_pool_manager_->FetchPage(current_page_id);
  if(page == nullptr) {
    return;
  }
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (!node->IsLeafPage()) {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    for (int i = 0; i < internal_node->GetSize(); i++) {
      Destroy(internal_node->ValueAt(i));
    }
  }
  buffer_pool_manager_->UnpinPage(current_page_id, false);
  buffer_pool_manager_->DeletePage(current_page_id);

  if (current_page_id == root_page_id_) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if(root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
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
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  if (IsEmpty()) {
    return false;
  }
  Page *page = FindLeafPage(key,root_page_id_,false);
  if (page == nullptr) {
    return false;
  }
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  RowId value;
  bool found = leaf->Lookup(key, value, processor_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  if (found) {
    result.push_back(value);
  }
  return found;
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
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) { 
  if(IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
 }
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  page_id_t new_page_id;
  Page *page = buffer_pool_manager_->NewPage(new_page_id);
  if(page == nullptr) {
    throw "out of memory";
  }
  auto *leaf = reinterpret_cast<BPlusTreeLeafPage *>(page->GetData());
  leaf->Init(new_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  leaf->Insert(key, value, processor_);
  root_page_id_ = new_page_id;
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  UpdateRootPageId(1);

}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) { 
  if(IsEmpty()) {
    return false;
  }
  RowId copy_value=value;
  Page *page = FindLeafPage(key,root_page_id_,false);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  page->WLatch();
  if(leaf->Lookup(key, copy_value, processor_)) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }
  int size=leaf->Insert(key, value, processor_);
  if(size >= leaf_max_size_) {
    auto new_leaf = Split(leaf, transaction);
    auto new_key = new_leaf->KeyAt(0);
    InsertIntoParent(leaf, new_key, new_leaf, transaction);
    buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) { 
  auto new_page_id=INVALID_PAGE_ID;
  auto new_page=buffer_pool_manager_->NewPage(new_page_id);
  if(new_page == nullptr) {
    throw "out of memory";
  }
  auto *new_node = reinterpret_cast<InternalPage *>(new_page->GetData());
  new_node->Init(new_page_id,node->GetParentPageId(), node->GetKeySize(), internal_max_size_);
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  return new_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) { 
  auto new_page_id=INVALID_PAGE_ID;
  auto new_page=buffer_pool_manager_->NewPage(new_page_id);
  if(new_page == nullptr) {
    throw "out of memory";
  }
  auto *new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_node->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), leaf_max_size_);
  node->MoveHalfTo(new_node);
  new_node->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_page_id);
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
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if(old_node->GetParentPageId() == INVALID_PAGE_ID) {
    auto new_root_page_id=INVALID_PAGE_ID;
    auto new_page=buffer_pool_manager_->NewPage(new_root_page_id);
    if(new_page == nullptr) {
      throw "out of memory";
    }
    auto *new_root = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root->Init(new_root_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    UpdateRootPageId(1);
    root_page_id_ = new_root_page_id;
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
  }
  else {
    Page *page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    auto *parent = reinterpret_cast<InternalPage *>(page->GetData());
    page->WLatch();
    int size = parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    if(size > internal_max_size_) {
      auto new_internal = Split(parent, transaction);
      auto new_key = new_internal->KeyAt(0);
      InsertIntoParent(parent, new_key, new_internal, transaction);
      buffer_pool_manager_->UnpinPage(new_internal->GetPageId(), true);
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }
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
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if(IsEmpty()) {
    return;
  }
  Page * page=FindLeafPage(key);
  if(page == nullptr) {
    return;
  }
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  page->WLatch();
  RowId value;
  if(!leaf->Lookup(key, value, processor_)) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return;
  }
  leaf->RemoveAndDeleteRecord(key, processor_);
  if(leaf->IsRootPage() && leaf->GetSize() == 0) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    buffer_pool_manager_->DeletePage(page->GetPageId());
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return;
  }
  if(leaf->GetSize() < leaf->GetMinSize()) {
    CoalesceOrRedistribute(leaf, transaction);
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  if(IsEmpty()) {
    return false;
  } 
  if(node->IsRootPage()) {
    return AdjustRoot(node);
  }
  Page *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto *parent = reinterpret_cast<InternalPage *>(page->GetData());
  int index = parent->ValueIndex(node->GetPageId());

  if (index > 0) {
    Page *sibling_page = buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1));
    N *sibling = reinterpret_cast<N *>(sibling_page->GetData());
    if (node->GetSize() + sibling->GetSize() <= node->GetMaxSize()) {
      Coalesce(sibling, node, parent, index, transaction);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
      return true;
    }
    Redistribute(sibling, node, index);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return false;
  }

  if (index < parent->GetSize() - 1) {
    Page *sibling_page = buffer_pool_manager_->FetchPage(parent->ValueAt(index + 1));
    N *sibling = reinterpret_cast<N *>(sibling_page->GetData());
    if (node->GetSize() + sibling->GetSize() <= node->GetMaxSize()) {
      Coalesce(node, sibling, parent, index + 1, transaction);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
      return true;
    }
    Redistribute(sibling, node, index);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return false;
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  return false;
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
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
                    if(IsEmpty()) {
                      return false;
                    }
                    node->MoveAllTo(neighbor_node);
                    buffer_pool_manager_->DeletePage(node->GetPageId());
                    parent->Remove(index);//更新父节点数据
  if (parent->IsRootPage() && parent->GetSize() <= 1) {
    return CoalesceOrRedistribute(parent, transaction);
  } else if (!parent->IsRootPage() && parent->GetSize() < parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent, transaction);
  }
  return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  if(IsEmpty()) { 
    return false;
  }
  node->MoveAllTo(neighbor_node,node->KeyAt(index), buffer_pool_manager_);
  parent->Remove(index);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  if(parent->GetSize() >= parent->GetMinSize()){
    return false;
  }else{
    return CoalesceOrRedistribute(parent, transaction);
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
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  if(index == 0) {
    neighbor_node->MoveFirstToEndOf(node);
    auto parent_page=buffer_pool_manager_->FetchPage(neighbor_node->GetParentPageId());
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
    parent->SetKeyAt(parent->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
  else {
    neighbor_node->MoveLastToFrontOf(node);
    auto parent_page=buffer_pool_manager_->FetchPage(neighbor_node->GetParentPageId());
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
    parent->SetKeyAt(parent->ValueIndex(node->GetPageId()), node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  if(index == 0) {
    auto parent_page=buffer_pool_manager_->FetchPage(neighbor_node->GetParentPageId());
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
    // The middle key is the key in the parent that points to the neighbor_node.
    neighbor_node->MoveFirstToEndOf(node, parent->KeyAt(parent->ValueIndex(neighbor_node->GetPageId())), buffer_pool_manager_);
    parent->SetKeyAt(parent->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
  else {
    auto parent_page=buffer_pool_manager_->FetchPage(neighbor_node->GetParentPageId());
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
    neighbor_node->MoveLastToFrontOf(node, parent->KeyAt(parent->ValueIndex(node->GetPageId())), buffer_pool_manager_);
    parent->SetKeyAt(parent->ValueIndex(node->GetPageId()), node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
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
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if(old_root_node->GetSize() == 1)
  {
    if(old_root_node->IsLeafPage())
    {      return false;
    }
    auto *old_root = reinterpret_cast<InternalPage *>(old_root_node);
    auto new_root_page_id = old_root->ValueAt(0);
    auto new_root_page = buffer_pool_manager_->FetchPage(new_root_page_id);
    auto *new_root = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0);
    return true;
  } 
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {

  if (IsEmpty()) {
    return IndexIterator();
  }
  auto page = FindLeafPage(nullptr, root_page_id_, true);
  auto *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());  
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);  
  return IndexIterator(leaf_node->GetPageId(),buffer_pool_manager_,0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  if (IsEmpty()) {
    return IndexIterator();
  }
  auto page = FindLeafPage(key);
  if(page != nullptr) {
    LeafPage *leaf_node = nullptr;
    leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    int index = leaf_node->KeyIndex(key, processor_);
    return IndexIterator(leaf_node->GetPageId(), buffer_pool_manager_, index);
  }
   return IndexIterator();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator(INVALID_PAGE_ID, buffer_pool_manager_, 0);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if(IsEmpty()) {
    return nullptr;
  }
  if (page_id == INVALID_PAGE_ID) {
    page_id = root_page_id_;
  }
  auto page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    return nullptr;
  }
  auto bplus_tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  page->RLatch();
  if(bplus_tree_page->IsLeafPage())
  {
    page->RUnlatch();
    return page;
  }
  page_id_t next_page_id;
  auto node = reinterpret_cast<InternalPage *>(page->GetData());
  if(leftMost) {
    next_page_id = node->ValueAt(0);
  }
  else {
    next_page_id = node->Lookup(key,processor_);
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return FindLeafPage(key, next_page_id, leftMost);
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page isdefined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto head_page=reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  if (insert_record) {
    head_page->Insert(index_id_, root_page_id_);
  } else {
    head_page->Update(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
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
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
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
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
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
      ToGraph(child_page, bpm, out, schema);
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
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
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
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}